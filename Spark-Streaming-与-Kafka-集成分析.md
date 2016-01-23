## 前言

Spark Streaming 诞生于2013年，成为Spark平台上流式处理的解决方案，同时也给大家提供除Storm 以外的另一个选择。这篇内容主要介绍Spark Streaming 数据接收流程模块中与Kafka集成相关的功能。

Spark Streaming 与 Kafka 集成接受数据的方式有两种：

1. Receiver-based Approach
2. Direct Approach (No Receivers)

我们会对这两种方案做详细的解析，同时对比两种方案优劣。选型后，我们针对Direct Approach (No Receivers)模式详细介绍其如何实现Exactly Once Semantics，也就是保证接收到的数据只被处理一次，不丢，不重。
  

## Receiver-based Approach

要描述清楚 Receiver-based Approach ，我们需要了解其接收流程，分析其内存使用，以及相关参数配置对内存的影响。

>  *** 数据接收流程 ***

启动Spark Streaming(后续缩写为SS)后，SS 会选择一台Executor 启动ReceiverSupervisor,并且标记为Active状态。接着按如下步骤处理：

1. ReceiverSupervisor会启动对应的Receiver(这里是KafkaReceiver)

2. KafkaReceiver 会根据配置启动新的线程接受数据，在该线程中调用 `ReceiverSupervisor.store` 方法填充数据，注意，这里是一条一条填充的。

3. `ReceiverSupervisor` 会调用 `BlockGenerator.addData` 进行数据填充。

到目前为止，整个过程不会有太多内存消耗，正常的一个线性调用。所有复杂的数据结构都隐含在 `BlockGenerator` 中。

> *** BlockGenerator 存储结构 ***

BlockGenerator 会复杂些，重要的数据存储结构有四个：

1. 维护了一个缓存 `currentBuffer` ，就是一个无限长度的ArrayBuffer。`currentBuffer`  并不会被复用，而是每次都会新建，然后把老的对象直接封装成Block，BlockGenerator会负责保证`currentBuffer` 只有一个。`currentBuffer` 填充的速度是可以被限制的，以秒为单位，配置参数为 `spark.streaming.receiver.maxRate`。这个是Spark内存控制的第一道防线，填充`currentBuffer` 是阻塞的，消费Kafka的线程直接做填充。

2. 维护了一个 `blocksForPushing` 队列， size 默认为10个(1.5.1版本)，可通过 `spark.streaming.blockQueueSize` 进行配置。该队列主要用来实现生产-消费模式。每个元素其实是一个currentBuffer形成的block。

3. blockIntervalTimer 是一个定时器。其实是一个生产者，负责将`currentBuffer` 的数据放到 `blocksForPushing ` 中。通过参数 `spark.streaming.blockInterval` 设置，默认为200ms。放的方式很简单，直接把currentBuffer做为Block的数据源。这就是为什么currentBuffer不会被复用。

4. `blockPushingThread` 也是一个定时器，负责将Block从`blocksForPushing `取出来,然后交给`BlockManagerBasedBlockHandler.storeBlock` 方法。10毫秒会取一次，不可配置。到这一步，才真的将数据放到了Spark的BlockManager中。

下面我们会详细分析每一个存储对象对内存的使用情况：

  *** currentBuffer ***

首先自然要说下currentBuffer,如果200ms期间你从Kafka接受的数据足够大，则足以把内存承包了。而且currentBuffer使用的并不是spark的storage内存，而是有限的用于运算存储的内存。 默认应该是 heap*0.4。除了把内存搞爆掉了，还有一个是GC。导致receiver所在的Executor 极容易挂掉，处理速度也巨慢。 如果你在SparkUI发现Receiver挂掉了，考虑有没有可能是这个问题。

 ***  blocksForPushing ***

`blocksForPushing` 这个是作为`currentBuffer` 和BlockManager之间的中转站。默认存储的数据最大可以达到  10*`currentBuffer` 大小。一般不打可能，除非你的 `spark.streaming.blockInterval` 设置的比10ms 还小，官方推荐最小也要设置成 50ms，你就不要搞对抗了。所以这块不用太担心。

 *** blockPushingThread ***

`blockPushingThread ` 负责从 `blocksForPushing ` 获取数据，并且写入 `BlockManager` 。`blockPushingThread `只写他自己所在的Executor的 `blockManager`,也就是每个batch周期的数据都会被 一个Executor给扛住了。 这是导致内存被撑爆的最大风险。 建议每个batch周期接受到的数据最好不要超过接受Executor的内存(Storage)的一半。否则在数据量很大的情况下，会导致Receiver所在的Executor直接挂掉。  对应的解决方案是使用多个Receiver来消费同一个topic,使用类似下面的代码

```scala
val kafkaDStreams = (1 to kafkaDStreamsNum).map { _ => KafkaUtils.createStream(
ssc, 
zookeeper, 
groupId, 
Map("your topic" -> 1),  
if (memoryOnly) StorageLevel.MEMORY_ONLY else StorageLevel.MEMORY_AND_DISK_SER_2)}
val unionDStream = ssc.union(kafkaDStreams)
unionDStream
```

> *** 动态控制消费速率以及相关论文 ***

前面我们提到，SS的消费速度可以设置上限，其实SS也可以根据之前的周期处理情况来自动调整下一个周期处理的数据量。你可以通过将 `spark.streaming.backpressure.enabled` 设置为true 打开该功能。算法的论文可参考： Socc 2014: [Adaptive Stream Processing using Dynamic Batch Sizing](http://www.eecs.berkeley.edu/Pubs/TechRpts/2014/EECS-2014-133.html)  ,还是有用的，我现在也都开启着。

另外值得提及的是，Spark里除了这个 `Dynamic`,还有一个就是`Dynamic Allocation`,也就是Executor数量会根据资源使用情况，自动伸缩。我其实蛮喜欢Spark这个特色的。具体的可以查找下相关设计文档。

## Direct Approach (No Receivers)

个人认为，DirectApproach 更符合Spark的思维。我们知道，RDD的概念是一个不变的，分区的数据集合。我们将kafka数据源包裹成了一个KafkaRDD,RDD里的partition 对应的数据源为kafka的partition。唯一的区别是数据在Kafka里而不是事先被放到Spark内存里。其实包括FileInputStream里也是把每个文件映射成一个RDD,比较好奇，为什么一开始会有Receiver-based Approach，额外添加了`Receiver`这么一个概念。


> *** DirectKafkaInputDStream ***

Spark Streaming通过Direct Approach接收数据的入口自然是`KafkaUtils.createDirectStream` 了。在调用该方法时，会先创建

    val kc = new KafkaCluster(kafkaParams)

`KafkaCluster` 这个类是真实负责和Kafka 交互的类，该类会获取Kafka的partition信息,接着会创建 `DirectKafkaInputDStream`,每个`DirectKafkaInputDStream`对应一个Topic。 此时会获取每个Topic的每个Partition的offset。 如果配置成`smallest` 则拿到最早的offset,否则拿最近的offset。

每个`DirectKafkaInputDStream ` 也会持有一个KafkaCluster实例。
到了计算周期后，对应的`DirectKafkaInputDStream .compute`方法会被调用,此时做下面几个操作：

1. 获取对应Kafka Partition的`untilOffset`。这样就确定过了需要获取数据的区间，同时也就知道了需要计算多少数据了

2. 构建一个KafkaRDD实例。这里我们可以看到，每个计算周期里，`DirectKafkaInputDStream` 和 `KafkaRDD` 是一一对应的

3. 将相关的offset信息报给InputInfoTracker

4. 返回该RDD

> *** KafkaRDD 的组成结构 ***

KafkaRDD 包含 N(N=Kafka的partition数目)个 KafkaRDDPartition,每个KafkaRDDPartition 其实只是包含一些信息，譬如topic,offset等，真正如果想要拉数据， 是透过KafkaRDDIterator 来完成，一个`KafkaRDDIterator `对应一个 `KafkaRDDPartition`。

整个过程都是延时过程，也就是数据其实都在Kafka存着呢，直到有实际的Action被触发，才会有去kafka主动拉数据。


> *** 限速 ***

Direct Approach (NoReceivers) 的接收方式也是可以限制接受数据的量的。你可以通过设置
`spark.streaming.kafka.maxRatePerPartition` 来完成对应的配置。需要注意的是，这里是对每个Partition进行限速。所以你需要事先知道Kafka有多少个分区，才好评估系统的实际吞吐量，从而设置该值。

相应的，`spark.streaming.backpressure.enabled` 参数在Direct Approach 中也是继续有效的。

##  Direct Approach VS Receiver-based Approach  

经过上面对两种数据接收方案的介绍，我们发现，
Receiver-based Approach 存在各种内存折腾，对应的Direct Approach (No Receivers)则显得比较纯粹简单些，这也给其带来了较多的优势，主要有如下几点：

1. 因为按需拉数据，所以不存在缓冲区，就不用担心缓冲区把内存撑爆了。这个在Receiver-based Approach 就比较麻烦，你需要通过`spark.streaming.blockInterval `等参数来调整。

2. 数据默认就被分布到了多个Executor上。Receiver-based Approach 你需要做特定的处理，才能让 Receiver分不到多个Executor上。

3. Receiver-based Approach 的方式，一旦你的Batch Processing 被delay了，或者被delay了很多个batch,那估计你的Spark Streaming程序离奔溃也就不远了。 Direct Approach (No Receivers) 则完全不会存在类似问题。就算你delay了很多个batch time,你内存中的数据只有这次处理的。

4. Direct Approach (No Receivers) 直接维护了 Kafka offset,可以保证数据只有被执行成功了，才会被记录下来，透过 `checkpoint`机制。如果采用Receiver-based Approach，消费Kafka和数据处理是被分开的，这样就很不好做容错机制，比如系统当掉了。所以你需要开启WAL,但是开启WAL带来一个问题是，数据量很大，对HDFS是个很大的负担，而且也会对实时程序带来比较大延迟。 

我原先以为Direct Approach 因为只有在计算的时候才拉取数据，可能会比Receiver-based Approach 的方式慢，但是经过我自己的实际测试，总体性能 Direct Approach会更快些，因为Receiver-based Approach可能会有较大的内存隐患，GC也会影响整体处理速度。

##  如何保证数据接受的可靠性
 SS 自身可以做到  at least once 语义,具体方式是通过CheckPoint机制。

>  *** CheckPoint 机制 *** 

 CheckPoint 会涉及到一些类，以及他们之间的关系：

`DStreamGraph`类负责生成任务执行图，而`JobGenerator`则是任务真实的提交者。任务的数据源则来源于`DirectKafkaInputDStream`，checkPoint 一些相关信息则是由类`DirectKafkaInputDStreamCheckpointData` 负责。

好像涉及的类有点多，其实没关系，我们完全可以不用关心他们。先看看checkpoint都干了些啥，checkpoint 其实就序列化了一个类而已：

     org.apache.spark.streaming.Checkpoint

看看类成员都有哪些：
```
val master = ssc.sc.master
val framework = ssc.sc.appName
val jars = ssc.sc.jars
val graph = ssc.graph
val checkpointDir = ssc.checkpointDir
val checkpointDuration = ssc.checkpointDurationval pendingTimes = ssc.scheduler.getPendingTimes().toArray
val delaySeconds = MetadataCleaner.getDelaySeconds(ssc.conf)
val sparkConfPairs = ssc.conf.getAll
```

其他的都比较容易理解，最重要的是 graph，该类全路径名是：

     org.apache.spark.streaming.DStreamGraph

里面有两个核心的数据结构是：

```
private val inputStreams = new ArrayBuffer[InputDStream[_]]()
private val outputStreams = new ArrayBuffer[DStream[_]]()
```

inputStreams 对应的就是 `DirectKafkaInputDStream` 了。

再进一步，`DirectKafkaInputDStream`  有一个重要的对象

```
protected[streaming] override val checkpointData =  new DirectKafkaInputDStreamCheckpointData
```

checkpointData 里则有一个data 对象，里面存储的内容也很简单

```
data.asInstanceOf[mutable.HashMap[Time, Array[OffsetRange.OffsetRangeTuple]]]
```

就是每个batch 的唯一标识 time 对象，以及每个KafkaRDD对应的的Kafka偏移信息。

而 outputStreams 里则是RDD,如果你存储的时候做了foreach操作，那么应该就是 `ForEachRDD`了，他被序列化的时候是不包含数据的。

而downtime由checkpoint 时间决定,pending time之类的也会被序列化。

经过上面的分析，我们发现：

1. checkpoint 是非常高效的。没有涉及到实际数据的存储。一般大小只有几十K，因为只存了Kafka的偏移量等信息。
2. checkpoint 采用的是序列化机制，尤其是DStreamGraph的引入，里面包含了可能如ForeachRDD等，而ForeachRDD里面的函数应该也会被序列化。如果采用了CheckPoint机制，而你的程序包做了做了变更，恢复后可能会有一定的问题。

接着我们看看`JobGenerator`是怎么提交一个真实的batch任务的，分析在什么时间做checkpoint 操作，从而保证数据的高可用：

1. 产生jobs
2. 成功则提交jobs 然后异步执行
3. 失败则会发出一个失败的事件
4. 无论成功或者失败，都会发出一个 `DoCheckpoint` 事件。
5. 当任务运行完成后，还会再调用一次`DoCheckpoint` 事件。

只要任务运行完成后没能顺利执行完`DoCheckpoint `前crash,都会导致这次Batch被重新调度。也就说无论怎样，不存在丢数据的问题，而这种稳定性是靠checkpoint 机制以及Kafka的可回溯性来完成的。

那现在会产生一个问题，假设我们的业务逻辑会对每一条数据都处理，则

1.  我们没有处理一条数据
2.  我们可能只处理了部分数据
3.  我们处理了全部数据

根据我们上面的分析，无论如何，这次失败了，都会被重新调度，那么我们可能会重复处理数据，可能最后失败的那一次数据的一部分，也可能是全部，但不会更多了。

>  *** 业务需要做事务，保证 Exactly Once 语义 *** 

这里业务场景被区分为两个：

1. 幂等操作
2. 业务代码需要自身添加事物操作

所谓幂等操作就是重复执行不会产生问题，如果是这种场景下，你不需要额外做任何工作。但如果你的应用场景是不允许数据被重复执行的，那只能通过业务自身的逻辑代码来解决了。

这个SS 倒是也给出了官方方案：

```
dstream.foreachRDD { (rdd, time) =>
  rdd.foreachPartition { partitionIterator =>
    val partitionId = TaskContext.get.partitionId()
    val uniqueId = generateUniqueId(time.milliseconds, partitionId)
    // use this uniqueId to transactionally commit the data in partitionIterator
  }
}
```

这代码啥含义呢？ 就是说针对每个partition的数据，产生一个uniqueId,只有这个partion的所有数据被完全消费，则算成功，否则算失败，要回滚。下次重复执行这个uniqueId 时，如果已经被执行成功过的，则skip掉。

这样，就能保证数据 Exactly Once 语义啦。

## 总结

根据我的实际经验，目前Direct Approach 稳定性个人感觉比 Receiver-based Approach 更好些，推荐使用 Direct Approach 方式和Kafka进行集成,并且开启响应的checkpoint 功能，保证数据接收的稳定性，Direct Approach 模式本身可以保证数据 at least once语义，如果你需要Exactly Once 语义时，需要保证你的业务是幂等，或者保证了相应的事务。
