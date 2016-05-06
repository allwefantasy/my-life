> Spark/Spark Streaming transform 是一个很强的方法，不过使用过程中可能也有一些值得注意的问题。在分析的问题，我们还会顺带讨论下Spark Streaming 生成job的逻辑，从而让大家知道问题的根源。

## 问题描述

今天有朋友贴了一段 [gist](https://github.com/cfmcgrady/SparkStreamingStudy/blob/master/src/main/scala/test/QueueStream.scala),大家可以先看看这段代码有什么问题。

特定情况你会发现UI 的Storage标签上有很多新的Cache RDD，然后你以为是Cache RDD 不被释放，但是通过[Spark Streaming 数据清理机制](http://www.jianshu.com/p/f068afb23c77)分析我们可以排除这个问题。

接着通过给RDD的设置名字,名字带上时间，发现是延时的Batch 也会产生cache RDD。那这是怎么回事呢？

另外还有一个问题，也是相同的原因造成的：我通过KafkaInputStream.transform 方法获取Kafka偏移量，并且保存到HDFS上。然后发现一旦产生job(包括并没有执行的Job),都会生成了Offset,这样如果出现宕机，你看到的最新Offset 其实就是延时的，而不是出现故障时的Offset了。这样做恢复就变得困难了。

## 问题分析

其实是这样，在transform里你可以做很多复杂的工作，但是transform接受到的函数比较特殊，是会在TransformedDStream.compute方法中执行的，你需要确保里面的动作都是transformation(延时的)，而不能是Action(譬如第一个例子里的count动作)，或者不能有立即执行的(比如我提到的例子里的自己通过HDFS API 将Kafka偏移量保存到HDFS)。

```
override def compute(validTime: Time): Option[RDD[U]] = {
    val parentRDDs = parents.map { parent => 
    ....
  //看这一句，你的函数在调用compute方法时，就会被调用
    val transformedRDD = transformFunc(parentRDDs, validTime)
    if (transformedRDD == null) {
      throw new SparkException.....
    }
    Some(transformedRDD)
  }
```

这里有两个疑问：

* 那些.map .transform 都是transformation,不是只有真实被提交后才会被执行么？
* DStream.compute 方法为什么会在generateJob的时候就被调用呢？

## Spark Streaming generateJob 逻辑解析

在JobGenerator中，会定时产生一个GenerateJobs的事件:

```
private val timer = new RecurringTimer(clock, ssc.graph.batchDuration.milliseconds,  longTime => eventLoop.post(GenerateJobs(new Time(longTime))), "JobGenerator")
```
该事件会被DStreamGraph.generateJobs 处理，产生Job的逻辑 也很简单，

```
def generateJobs(time: Time): Seq[Job] = {   
    val jobs = this.synchronized {
      outputStreams.flatMap { outputStream =>
        val jobOption = outputStream.generateJob(time)
        ........    
  }
```

就是调用各个outputStream 的generateJob方法，典型的outputStream如ForEachDStream。 以ForEachDStream为例，产生job的方式如下：

```
override def generateJob(time: Time): Option[Job] = {
    parent.getOrCompute(time) match {
      case Some(rdd) =>
        val jobFunc = () => createRDDWithLocalProperties(time, displayInnerRDDOps) {
          foreachFunc(rdd, time)
        }
        Some(new Job(time, jobFunc))
      case None => None
    }
  }
```

我们看到，在这里会触发所有的DStream链进行compute动作。也就意味着所有transformation产生的DStream的compute方法都会被调用。

正常情况下不会有什么问题，比如.map(func) 产生的MappedDStream里面在compute执行时，func 都是被`记住`而不是被执行。但是TransformedDStream 是比较特殊的，对应的func是会被执行的，在对应的compute方法里，你会看到这行代码：

```
val transformedRDD = transformFunc(parentRDDs, validTime)
```

这里的transformFunc 就是transform(func)里的func了。然而transform 又特别灵活，可以执行各种RDD操作，这个时候Spark Streaming 是拦不住你的，一旦你使用了count之类的Action,产生Job的时候就会被立刻执行，而不是等到Job被提交才执行。
