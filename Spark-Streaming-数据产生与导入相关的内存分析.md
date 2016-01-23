## 前言

我这篇文章会分几个点来描述Spark Streaming 的Receiver在内存方面的表现。

* 一个大致的数据接受流程
* 一些存储结构的介绍
* 哪些点可能导致内存问题，以及相关的配置参数

另外，有位大牛写了[Spark Streaming 源码解析系列](https://github.com/proflin/CoolplaySpark/tree/master/Spark%20Streaming%20源码解析系列)，我觉得写的不错，这里也推荐下。

我在部门尽力推荐使用Spark Streaming做数据处理，目前已经应用在日志处理，机器学习等领域。这期间也遇到不少问题，尤其是Kafka在接受到的数据量非常大的情况下，会有一些内存相关的问题。

另外特别说明下，我们仅仅讨论的是High Level的Kafka Stream，也就是输入流通过如下方式创建：

     KafkaUtils.createStream

并且不开启WAL的情况下。

## 数据接受流程

启动Spark Streaming(后续缩写为SS)后，SS 会选择一台Executor 启动ReceiverSupervisor,并且标记为Active状态。接着按如下步骤处理：

1. ReceiverSupervisor会启动对应的Receiver(这里是KafkaReceiver)

2. KafkaReceiver 会根据配置启动新的线程接受数据，在该线程中调用 `ReceiverSupervisor.store` 方法填充数据，注意，这里是一条一条填充的。

3. `ReceiverSupervisor` 会调用 `BlockGenerator.addData` 进行数据填充。

到目前为止，整个过程不会有太多内存消耗，正常的一个线性调用。所有复杂的数据结构都隐含在 `BlockGenerator` 中。

## BlockGenerator 存储结构

BlockGenerator 会复杂些，这里有几个点，

1. 维护了一个缓存 `currentBuffer` ，就是一个无限长度的ArrayBuffer。`currentBuffer`  并不会被复用，而是每次都会新建，然后把老的对象直接封装成Block，BlockGenerator会负责保证`currentBuffer` 只有一个。`currentBuffer` 填充的速度是可以被限制的，以秒为单位，配置参数为 `spark.streaming.receiver.maxRate`。这个是Spark内存控制的第一道防线，填充`currentBuffer` 是阻塞的，消费Kafka的线程直接做填充。

2. 维护了一个 `blocksForPushing` 队列， size 默认为10个(1.5.1版本)，可通过 `spark.streaming.blockQueueSize` 进行配置。该队列主要用来实现生产-消费模式。每个元素其实是一个currentBuffer形成的block。

3. blockIntervalTimer 是一个定时器。其实是一个生产者，负责将`currentBuffer` 的数据放到 `blocksForPushing ` 中。通过参数 `spark.streaming.blockInterval` 设置，默认为200ms。放的方式很简单，直接把currentBuffer做为Block的数据源。这就是为什么currentBuffer不会被复用。

4. `blockPushingThread` 也是一个定时器，负责将Block从`blocksForPushing `取出来,然后交给`BlockManagerBasedBlockHandler.storeBlock` 方法。10毫秒会取一次，不可配置。到这一步，才真的将数据放到了Spark的BlockManager中。

步骤描述完了，我们看看有哪些值得注意的地方。

##  currentBuffer

首先自然要说下currentBuffer,如果200ms期间你从Kafka接受的数据足够大，则足以把内存承包了。而且currentBuffer使用的并不是spark的storage内存，而是有限的用于运算存储的内存。 默认应该是 heap*0.4。除了把内存搞爆掉了，还有一个是GC。导致receiver所在的Executor 极容易挂掉，处理速度也巨慢。 如果你在SparkUI发现Receiver挂掉了，考虑有没有可能是这个问题。

## blocksForPushing

`blocksForPushing` 这个是作为`currentBuffer` 和BlockManager之间的中转站。默认存储的数据最大可以达到  10*`currentBuffer` 大小。一般不打可能，除非你的 `spark.streaming.blockInterval` 设置的比10ms 还小，官方推荐最小也要设置成 50ms，你就不要搞对抗了。所以这块不用太担心。

## blockPushingThread

`blockPushingThread ` 负责从 `blocksForPushing ` 获取数据，并且写入 `BlockManager` 。这里很蛋疼的事情是，`blockPushingThread `只写他自己所在的Executor的 `blockManager`,也就是每个batch周期的数据都会被 一个Executor给扛住了。 这是导致内存被撑爆的最大风险。 也就是说，每个batch周期接受到的数据最好不要超过接受Executor的内存(Storage)的一半。否则有你受的。我发现在数据量很大的情况下，最容易挂掉的就是Receiver所在的Executor了。  建议Spark-Streaming团队最好是能将数据写入到多个BlockManager上。


## StorageLevel 的配置问题

另外还有几个值得注意的问题：

*  如果你配置成Memory_Disk ,如果Receiver所在的Executor一旦挂掉，你也歇菜了，整个Spark Streaming作业会失败。失败的原因是一部分block找不到了。

* 如果你配置成Memory_Disk_2，数据会被replication到不同的节点。一般而言不会出现作业失败或者丢数据。但解决不了Receiver也容易挂的问题，当然还是主要还是内存引起的。

* 最好是采用默认设置 MEMORY_AND_DISK_SER_2 比较靠谱些。

* 这里面还有一个风险点就是，如果某个batch processing延迟了，那么对应的BlockManager的数据不会被释放，然后下一个batch的数据还在进，也会加重内存问题。

## 动态控制消费速率以及相关论文

另外，spark的消费速度可以设置上限以外，亦可以根据processing time 来动态调整。通过 `spark.streaming.backpressure.enabled` 设置为true 可以打开。算法的论文可参考： Socc 2014: [Adaptive Stream Processing using Dynamic Batch Sizing](http://www.eecs.berkeley.edu/Pubs/TechRpts/2014/EECS-2014-133.html)  ,还是有用的，我现在也都开启着。

Spark里除了这个 `Dynamic`,还有一个就是`Dynamic Allocation`,也就是Executor数量会根据资源使用情况，自动伸缩。我其实蛮喜欢Spark这个特色的。具体的可以查找下相关设计文档。

## 后话

接下来一篇文章会讲一些解决方案。
