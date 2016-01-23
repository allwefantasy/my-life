看这篇文章前，请先移步[Spark Streaming 数据产生与导入相关的内存分析](http://www.jianshu.com/p/9e44d3fd62af), 文章重点讲的是从Kafka消费到数据进入BlockManager的这条线路的分析。

这篇内容是个人的一些经验，大家用的时候还是建议好好理解内部的原理，不可照搬

## 让Receiver均匀的分布到你的Executor上

在[Spark Streaming 数据产生与导入相关的内存分析](http://www.jianshu.com/p/9e44d3fd62af)中我说了这么一句话：

> 我发现在数据量很大的情况下，最容易挂掉的就是Receiver所在的Executor了。 建议Spark Streaming团队最好是能将数据写入到多个BlockManager上。

从现在的API来看，是没有提供这种途径的。但是Spark Streaming 提供了同时读多个topic的功能，每个topic是一个InputStream。 我们可以复用这个功能，具体代码如下：

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

kafkaDStreamsNum 是你自己定义的，希望有多少个Executor 启动Receiver 去接收kafka数据。我的经验值是 1/4 个Executors 数目。因为数据还要做replication 一般，所以这样内存最大可以占到  1/2 的storage.

另外，务必给你系统设置 `spark.streaming.receiver.maxRate`。假设你启动了 N个 Receiver，那么你系统实际会接受到的数据不会超过 N*MaxRate，也就是说，maxRate参数是针对每个 Receiver 设置的。

## 减少非Storage 内存的占用

也就是我们尽量让数据都占用Spark 的Storage 内存。方法是把`spark.streaming.blockInterval ` 调小点。当然也会造成一个副作用，就是input-block 会多。每个Receiver 产生的的input-block数为： batchInterval* 1000/blockInterval。 这里假设你的batchInterval 是以秒为单位的。 blockInterval 其实我也不知道会有啥影响。其实说白了，就是为了防止GC的压力。实时计算有一个很大问题是GC。

## 减少单个Executor的内存

一般在Spark Streaming中不建议把 Executor 的内存调的太大。对GC是个压力，大内存一FullGC比较可怕，很可能会拖垮整个计算。 多Executor的容错性也会更好些。
