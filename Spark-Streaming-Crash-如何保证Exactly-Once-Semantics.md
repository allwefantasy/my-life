> 这篇文章只是为了阐述Spark Streaming 意外Crash掉后，如何保证Exactly Once Semantics。本来这个是可以直接给出答案的，但是我还是啰嗦的讲了一些东西。

## 前言

其实这次写Spark Streaming相关的内容，主要是解决在其使用过程中大家真正关心的一些问题。我觉得应该有两块：

1. 数据接收。我在用的过程中确实产生了问题。
2. 应用的可靠性。因为SS是7*24小时运行的问题，我想知道如果它Crash了，会不会丢数据。

第一个问题在之前的三篇文章已经有所阐述:

* [Spark Streaming 数据产生与导入相关的内存分析](http://www.jianshu.com/p/9e44d3fd62af)
* [Spark Streaming 数据接收优化](http://www.jianshu.com/p/a1526fbb2be4)
* [Spark Streaming Direct Approach (No Receivers) 分析](http://www.jianshu.com/p/b4af851286e5)

第二个问题则是这篇文章重点会分析的。需要了解的是，基本上Receiver Based Approach 已经被我否决掉了，所以这篇文章会以 Direct Approach 为基准点，详细分析应用Crash后，数据的安全情况。(PS:我这前言好像有点长 O(∩_∩)O~)

 下文中所有涉及到Spark Streaming 的词汇我都直接用 SS了哈。

##  SS 自身可以做到  at least once 语义

SS 是靠CheckPoint 机制 来保证 at least once 语义的。 

如果你并不想了解这个机制，只是想看结论，可跳过这段，直接看 *** 两个结论 ***

#### CheckPoint 机制

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

由上面的分析，我们可以得到如下的结论：

#### 两个结论 

1. checkpoint 是非常高效的。没有涉及到实际数据的存储。一般大小只有几十K，因为只存了Kafka的偏移量等信息。
2. checkpoint 采用的是序列化机制，尤其是DStreamGraph的引入，里面包含了可能如ForeachRDD等，而ForeachRDD里面的函数应该也会被序列化。如果采用了CheckPoint机制，而你的程序包做了做了变更，恢复后可能会有一定的问题。


扯远了，其实上面分析了那么多,就是想让你知道，SS 的checkpoint 到底都存储了哪些东西？我们看看`JobGenerator`是怎么提交一个真实的batch任务的，就清楚了。

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

## 业务需要做事务，保证 Exactly Once 语义

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

其实Direct Approach 的容错性比较容易做，而且稳定。

## 后话

这篇内容本来不想做源码分析的，但是或多或少还是引入了一些。重要的是，为了保证Exactly Once Semantics ，你需要知道SS做了什么，你还需要做什么。
