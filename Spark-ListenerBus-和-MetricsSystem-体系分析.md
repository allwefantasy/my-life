> Spark 事件体系的中枢是ListenerBus，由该类接受Event并且分发给各个Listener。MetricsSystem 则是一个为了衡量系统的各种指标的`度量系统`。Listener可以是MetricsSystem的信息来源之一。他们之间总体是一个互相补充的关系。

## 前言

监控是一个大系统完成后最重要的一部分。Spark整个系统运行情况是由ListenerBus以及MetricsSystem 来完成的。这篇文章重点分析他们之间的工作机制以及如何通过这两个系统完成更多的指标收集。

## ListenerBus 是如何工作的

Spark的事件体系是如何工作的呢？我们先简要描述下，让大家有个大概的了解。

首先，大部分类都会引入一个对象叫listenerBus，这个类具体是什么得看实现，但是都一定继承自`org.apache.spark.util.ListenerBus`.

假设我们要提交一个任务集。这个动作可能会很多人关心，我就是使用listenerBus把Event发出去，类似下面的第二行代码。

```
  def submitJobSet(jobSet: JobSet) {
    listenerBus.post(StreamingListenerBatchSubmitted(jobSet.toBatchInfo))    
    jobSet.jobs.foreach(job => jobExecutor.execute(new JobHandler(job)))
    logInfo("Added jobs for time " + jobSet.time)
  }
```

listenerBus里已经注册了很多监听者，我们叫`listener`,通常listenerBus 会启动一个线程异步的调用这些listener去消费这个Event。而所谓的`消费`，其实就是触发事先设计好的回调函数来执行譬如信息存储等动作。 

这就是整个listenerBus的工作方式。这里我们看到，其实类似于`埋点`，这是有侵入性的，每个你需要关注的地方，如果想让人知晓，就都需要发出一个特定的Event。

## ListenerBus 分析

    特定实现 <   AsynchronousListenerBus  < ListenerBus
    特定实现 <   SparkListenerBus  < ListenerBus


这里的`特定实现`有：

       *  StreamingListenerBus extends  AsynchronousListenerBus 
       *  LiveListenerBus extends  AsynchronousListenerBus  with SparkListenerBus
       *  ReplayListenerBus extends SparkListenerBus 

 AsynchronousListenerBus 内部维护了一个queue,事件都会先放到这个queue,然后通过一个线程来让Listener处理Event。

SparkListenerBus 也是一个trait,但是里面有个具体的实现，预先定义了`onPostEvent` 方法对一些特定的事件做了处理。

其他更下面的类则根据需要混入或者继承SparkListenerBus ，AsynchronousListenerBus来完成他们需要的功能。

不同的ListenerBus 需要不同的Event 集 和Listener,比如你看StreamingListenerBus的签名，就知道所有的Event都必须是StreamingListenerEvent，所有的Listener都必须是StreamingListener。

      StreamingListenerBus  
      extends AsynchronousListenerBus[StreamingListener, StreamingListenerEvent]

## Listener(监听器)

通常而言，Listener 是有状态的，一般接受到一个Event后，可能就会更新内部的某个数据结构。以 `org.apache.spark.streaming.ui.StreamingJobProgressListener`为例，他是一个`StreamingListener`,内部就含有一些存储结构，譬如：

      private val waitingBatchUIData = new HashMap[Time, BatchUIData]
      private val runningBatchUIData = new HashMap[Time, BatchUIData]

看申明都是普通的 HashMap ，所以操作是需要做`synchronized `操作。如下：

```
override def onReceiverError(receiverError: StreamingListenerReceiverError) {
    synchronized {
      receiverInfos(receiverError.receiverInfo.streamId) = receiverError.receiverInfo
    }
  }
```


## MetricsSystem介绍

MetricsSystem 比较好理解，一般是为了衡量系统的各种指标的`度量系统`。算是一个key-value形态的东西。举个比较简单的例子，我怎么把当前JVM相关信息展示出去呢？做法自然很多，通过MetricsSystem就可以做的更标准化些，具体方式如下：

1. Source 。数据来源。比如对应的有`org.apache.spark.metrics.source.JvmSource`
2. Sink。  数据发送到哪去。有被动和主动。一般主动的是通过定时器来完成输出，譬如CSVSink，被动的如MetricsServlet等需要被用户主动调用。
3. 桥接Source 和Sink的则是MetricRegistry了。

Spark 并没有实现底层Metrics的功能，而是使用了一个第三方库：http://metrics.codahale.com 。感兴趣大家可以看看，有个更完整的认识。

## 如何配置MetricsSystem

MetricsSystem的配置有两种，第一种是 `metrics.properties` 配置文件的形态。第二种是通过spark conf完成，参数以`spark.metrics.conf.`开头 。

我这里简单介绍下第二种方式。

比如我想查看JVM的信息，包括GC和Memory的使用情况，则我通过类似 

     conf.set("spark.metrics.conf.driver.source.jvm.class","org.apache.spark.metrics.source.JvmSource")

默认情况下，MetricsSystem 配置了一个全局的Sink,MetricsServlet。所以你添加的任何Source 都可以通过一个path `/metrics/json`获取到。
如果你的程序设置做了上面的设置，把你的spark-ui的路径换成`/metrics/json`，就能看到jvm源的一些信息了。

通常，如果你要实现一个自定义的Source,可以遵循如下步骤(这里以JvmSource为例)。

-- 创建一个Source

```
private[spark] class JvmSource extends Source {
  override val sourceName = "jvm"
  override val metricRegistry = new MetricRegistry()

  metricRegistry.registerAll(new GarbageCollectorMetricSet)
  metricRegistry.registerAll(new MemoryUsageGaugeSet)
}
```

其中 sourceName 是为了给配置用的，比如上面我们设置

    spark.metrics.conf.driver.source.jvm.class

里面的`jvm` 就是JvmSource里设置的sourceName

每个Source 一般会自己构建一个MetricRegistry。上面的例子，具体的数据收集工作是由`GarbageCollectorMetricSet `,`MemoryUsageGaugeSet `完成的。

具体就是写一个类继承`com.codahale.metrics.MetricSet`,然后实现`Map<String, Metric> getMetrics()` 方法就好。

接着通过`  metricRegistry.registerAll`将写好的MetricSet注册上就行。

-- 添加配置

```
conf.set("spark.metrics.conf.driver.source.jvm.class","org.apache.spark.metrics.source.JvmSource")
```

-- 调用结果

将Spark UI 的地址换成`/metrics/json`，就能看到输出结果了。当然，这里是因为默认系统默认提供了一个Sink实现:`org.apache.spark.metrics.sink.MetricsServlet`，你可以自己实现一个。

## 如何定制更多的监控指标

通过之前我写的[Spark UI (基于Yarn) 分析与定制](http://www.jianshu.com/p/8e4c38d0c44e)，你应该学会了如何添加新的页面到Spark UI上。

而这通过这一片文章，你应该了解了数据来源有两个：

 * 各个Listener
 * MetricsSystem

你可以组合现有的Listener以及Metrics Source 显示任何你想要的内容。

如果现有的无法满足你，通常你的新的需求应该可以通过下面两种方式来满足：

1. 你需要监控新的事件，那么你需要添加新的ListenerBus,Listener,Event,然后到你需要的地方去`埋点`(post事件)。这肯定需要修改spark-core里的代码了。

2. 你需要呈现现有的listener或者已知对象的变量，则使用MetricsSystem，定义一个新的Source 即可。

这样，把这些对象传递到你的Page中，就可以进行展示。










