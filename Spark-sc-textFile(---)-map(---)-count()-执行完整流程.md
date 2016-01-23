> 本文介绍下Spark 到底是如何运行sc.TextFile(...).map(....).count() 这种代码的，从driver端到executor端。

## 引子
 今天正好有人在群里问到相关的问题，不过他的原始问题是：

>我在RDD里面看到很多  new MapPartitionsRDD\[U, T\](this, (context, pid, iter) => iter.map(cleanF)),但是我找不到context是从哪里来的

另外还有pid,iter都是哪来的呢？ 如果你照着源码点进去你会很困惑。为莫名其妙怎么就有了这些iterator呢?

## Transform 和Action的来源

一般刚接触Spark 的同学，都会被告知这两个概念。Transform就是RDD的转换，从一个RDD转化到另一个RDD(也有多个的情况)。 Action则是出发实际的执行动作。

标题中的map就是一个典型的tansform操作,看源码，无非就是从当前的RDD构建了一个新的`MapPartitionsRDD`

```
def map[U: ClassTag](f: T => U): RDD[U] = withScope {
    val cleanF = sc.clean(f)
    new MapPartitionsRDD[U, T](this, (context, pid, iter) => iter.map(cleanF))
  }
```

这个新的RDD 接受了`this`作为参数，也就记住了他的父RDD。同时接受了一个匿名函数:

     (context, pid, iter) => iter.map(cleanF))

至于这个context,pid,iter是怎么来的，你当前是不知道的。你只是知道这个新的RDD,有这么一个函数。至于什么时候这个函数会被调用，我们下面会讲解到。

而一个Action是什么样的呢？我们看看count:

```
def count(): Long = sc.runJob(this, Utils.getIteratorSize _).sum
```

发现不一样了，要真的开始run Job了。sparkContext 的runJob 有很多种形态，这里你看到的是接受当前这个RDD 以及一个函数(Utils.getIteratorSize _)。

当然，这里的Utils.getItteratorSize 是一个已经实现好的函数：

```
  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  } 
```

它符合 sc.runJob 需要接受的签名形态：

     func: Iterator[T] => U



## Driver端的工作

这里你会见到一些熟悉的身影，比如dagScheduler,TaskScheduler,SchedulerBackend等。我们慢慢分解。

我们深入runJob,你马上就可以看到了dagScheduler了。

    dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, resultHandler, localProperties.get)

这里的cleanedFunc 就是前面那个 `func: Iterator[T] => U` 函数。在我们的例子里，就是一个计数的函数。

这样我们就顺利的离开SparkContext 进入DAGScheduler的王国了。

dagScheduler会进一步提交任务。

     val waiter = submitJob(rdd, func, partitions, callSite, resultHandler, properties) 

请记住上面第二个参数，func其实就是前面的 Utils.getItteratorSize 函数,不过签名略有改变，添加了context,变成了这种形态：

    (TaskContext, Iterator[_]) => _

接着会变成一个事件，发到事件队列里,其中 func2 还是上面的func,只是被改了名字而已。

```
eventProcessLoop.post(JobSubmitted(  jobId, rdd, func2, partitions.toArray, callSite, waiter,  SerializationUtils.clone(properties)))
```

dag会通过`handleJobSubmitted` 函数处理这个事件。在这里完成Stage的拆分。这个不是我们这次关注的主题，所以不详细讨论。最后，会把Stage进行提交：

     submitMissingTasks(finalStage)

提交到哪去了呢？会根据Stage的类型，生成实际的任务，然后序列化。序列化后通过广播机制发送到所有节点上去。

```
var taskBinary: Broadcast[Array[Byte]] = null
    try {
      // For ShuffleMapTask, serialize and broadcast (rdd, shuffleDep).
      // For ResultTask, serialize and broadcast (rdd, func).
      val taskBinaryBytes: Array[Byte] = stage match {
        case stage: ShuffleMapStage =>
          closureSerializer.serialize((stage.rdd, stage.shuffleDep): AnyRef).array()
        case stage: ResultStage =>
          closureSerializer.serialize((stage.rdd, stage.resultOfJob.get.func): AnyRef).array()
      }

      taskBinary = sc.broadcast(taskBinaryBytes)
```

然后生成tasks对象，`ShuffleMapTask` 或者`ResultTask`,我们这里的count是ResultTask,通过下面的方式提交：

     taskScheduler.submitTasks(new TaskSet(  tasks.toArray, stage.id, stage.latestInfo.attemptId, stage.firstJobId, properties))

现在我们进入 TaskSchedulerImpl 的地盘了。在submitTasks里我们调用了backend.我们接着就进入到`CoarseGrainedSchedulerBackend.DriverEndpoint`里。这个DriverEndPoint做完应该怎么把Task分配到哪些Executor的计算后，最后会去做真正的launchTask的工作：

```
executorData.executorEndpoint.send(LaunchTask(new SerializableBuffer(serializedTask)))
```

把序列化好的任务发送到Executor 上。到这里，Driver端的工作就完整了。

有一点你可能会比较好奇，为什么要做两次序列化，发送两次的？ 也就是前面的taskBinary，还有serializedTask。 taskBinany 包括一些RDD,函数等信息。而serializedTask 这是整个Task的任务信息，比如对应的那个分区号等。后面我们还会看到taskBinary的身影。


## Executor端

Executor 的入口是`org.apache.spark.executor. Executor`类。你可以看到梦寐以求的launchTask 方法

```
 def launchTask(
      context: ExecutorBackend,
      taskId: Long,
      attemptNumber: Int,
      taskName: String,
      serializedTask: ByteBuffer): Unit = {
    val tr = new TaskRunner(context, taskId = taskId, attemptNumber = attemptNumber, taskName,
      serializedTask)
    runningTasks.put(taskId, tr)
    threadPool.execute(tr)
  }
```

核心你看到了，是`TaskRunner `方法。进去看看，核心代码如下：

```
 val res = task.run(
            taskAttemptId = taskId,
            attemptNumber = attemptNumber,
            metricsSystem = env.metricsSystem)
```

这个task(ResultTask).run里是我们最后的核心，真正的逻辑调用发生在这里：

```
override def runTask(context: TaskContext): U = {
    // Deserialize the RDD and the func using the broadcast variables.
    val deserializeStartTime = System.currentTimeMillis()
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val (rdd, func) = ser.deserialize[(RDD[T], (TaskContext, Iterator[T]) => U)](
      ByteBuffer.wrap(taskBinary.value), Thread.currentThread.getContextClassLoader)
    _executorDeserializeTime = System.currentTimeMillis() - deserializeStartTime

    metrics = Some(context.taskMetrics)
    func(context, rdd.iterator(partition, context))
  }
```

前面通过taskBinary 还原出RDD,func。 而这里的func就是我们那个经过`改良`的Utils.getItteratorSize函数,前面在driver端就被改造成`func(context, rdd.iterator(partition, context))` 这种形态了。但是函数体还是下面的
```
  def getIteratorSize[T](iterator: Iterator[T]): Long = {
    var count = 0L
    while (iterator.hasNext) {
      count += 1L
      iterator.next()
    }
    count
  } 
```
也就是是一个计数函数。参数iterator则是通过rdd.iterator(partition, context)拿到了。

## 总结

到此，我们完成了整个代码的流转过程。之所以很多人看到这些地会比较疑惑，是因为看到的代码都是在driver端的。但是最后这些任务都要被序列化发送到Executor端。所以一般我们看到的流程不是连续的。








