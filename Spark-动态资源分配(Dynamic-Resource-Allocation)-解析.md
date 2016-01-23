> Spark 默认采用的是资源预分配的方式。这其实也和按需做资源分配的理念是有冲突的。这篇文章会详细介绍Spark 动态资源分配原理。


## 前言

最近在使用Spark Streaming程序时，发现如下几个问题：

1. 高峰和低峰Spark Streaming每个周期要处理的数据量相差三倍以上,预分配资源会导致低峰的时候资源的大量浪费。
2. Spark Streaming 跑的数量多了后，资源占用相当可观。

所以便有了要开发一套针对Spark Streaming 动态资源调整的想法。我在文章最后一个章节给出了一个可能的设计方案。不过要做这件事情，首先我们需要了解现有的Spark 已经实现的 Dynamic Resource Allocation 机制，以及为什么它无法满足现有的需求。

## 入口

在SparkContext 中可以看到这一行：
```

_executorAllocationManager =
      if (dynamicAllocationEnabled) {
        Some(new ExecutorAllocationManager(this, listenerBus, _conf))
      } else {
        None
      }
```

通过`spark.dynamicAllocation.enabled`参数开启后就会启动ExecutorAllocationManager。

这里有我第一个吐槽的点，这么直接new出来，好歹也做个配置，方便第三方开发个新的组件可以集成进去。但是Spark很多地方都是这么搞的，完全没有原来Java社区的风格。

## 动态调整资源面临的问题

我们先看看，动态资源调整需要解决哪几个问题：

1. Cache问题。如果需要移除的Executor含有RDD cache该如何办？
2. Shuffle问题。 如果需要移除的Executor包含了Shuffle Write先关数据该怎么办？
3. 添加和删除之后都需要告知DAGSchedule进行相关信息更新。

Cache去掉了重算即可。为了防止数据抖动，默认包含有Cache的Executor是不会被删除的，因为默认的Idle时间设置的非常大：

```
private val cachedExecutorIdleTimeoutS = conf.getTimeAsSeconds(  
"spark.dynamicAllocation.cachedExecutorIdleTimeout", 
s"${Integer.MAX_VALUE}s")
```

你可以自己设置从而去掉这个限制。

而对于Shuffle,则需要和Yarn集成，需要配置`yarn.nodemanager.aux-services`。具体配置方式，大家可以Google。这样Spark Executor就不用保存Shuffle状态了。

## 触发条件

添加Worker的触发条件是：

* 有Stage正在运行，并且预估需要的Executors > 现有的

删除Woker的触发条件是：

* 一定时间内(默认60s)没有task运行的Executor

我们看到触发条件还是比较简单的。这种简单就意味着用户需要根据实际场景，调整各个时间参数，比如到底多久没有运行task的Executor才需要删除。

默认检测时间是100ms:

    private val intervalMillis: Long = 100

## 如何实现Container的添加和释放

只有ApplicationMaster才能够向Yarn发布这些动作。而真正的中控是`org.apache.spark.ExecutorAllocationManager`,所以他们之间需要建立一个通讯机制。对应的方式是在ApplicationMaster有一个`private class AMEndpoint(`类，比如删除释放容器的动作在里就有：

```
  case KillExecutors(executorIds) =>
        logInfo(s"Driver requested to kill executor(s) ${executorIds.mkString(", ")}.")
        Option(allocator) match {
          case Some(a) => executorIds.foreach(a.killExecutor)
          case None => logWarning("Container allocator is not ready to kill executors yet.")
        }
        context.reply(true)
```

而`ExecutorAllocationManager`则是引用`YarnSchedulerBackend`实例，该实例持有ApplicationMaster的 RPC引用

```
private var amEndpoint: Option[RpcEndpointRef] 
```

## 如何获取调度信息

要触发上面描述的操作，就需要任务的调度信息。这个是通过`ExecutorAllocationListener extends SparkListener`来完成的。具体是在 ExecutorAllocationMaster的start函数里，会将该Listener实例添加到SparkContext里的listenerBus里，从而实现对DAGSchecude等模块的监听。机制可以参看这篇文章 [Spark ListenerBus 和 MetricsSystem 体系分析](http://www.jianshu.com/p/5506cd264f4d)。

根据上面的分析，我们至少要知道如下三个信息：

1. Executor上是否为空,如果为空，就可以标记为Idle.只要超过一定的时间，就可以删除掉这个Executor.
2. 正在跑的Task有多少
3. 等待调度的Task有多少

这里是以Stage为区分的。分别以三个变量来表示：

```
private val stageIdToNumTasks = new mutable.HashMap[Int, Int]
private val stageIdToTaskIndices = new mutable.HashMap[Int, mutable.HashSet[Int]]
private val executorIdToTaskIds = new mutable.HashMap[String, mutable.HashSet[Long]]
```
名字已经很清楚了。值得说的是stageIdToTaskIndices，其实就是stageId 对应的正在运行的task id 集合。

那么怎么计算出等待调度的task数量呢？计算方法如下： 

 stageIdToNumTasks(stageId) - stageIdToTaskIndices(stageId).size

这些都是动态更新变化的，因为有了监听器，所以任务那边有啥变化，这边都会得到通知。

## 定时扫描器

有了上面的铺垫，我们现在进入核心方法：

```
private def schedule(): Unit = synchronized {
    val now = clock.getTimeMillis

    updateAndSyncNumExecutorsTarget(now)

    removeTimes.retain { case (executorId, expireTime) =>
      val expired = now >= expireTime
      if (expired) {
        initializing = false
        removeExecutor(executorId)
      }
      !expired
    }
  }
```

该方法会每隔100ms被调度一次。你可以理解为一个监控线程。


##  Executor判定为空闲的机制

只要有一个task结束，就会判定有哪些Executor已经没有任务了。然后会被加入待移除列表。在放到removeTimes的时候，会把当前时间now + executorIdleTimeoutS * 1000 作为时间戳存储起来。当调度进程扫描这个到Executor时，会判定时间是不是到了，到了的话就执行实际的remove动作。在这个期间，一旦有task再启动，并且正好运行在这个Executor上，则又会从removeTimes列表中被移除。 那么这个Executor就不会被真实的删除了。

## Executor 需要增加的情况


首先，系统会根据下面的公式计算出实际需要的Executors数目：

```
private def maxNumExecutorsNeeded(): Int = {
    val numRunningOrPendingTasks = listener.totalPendingTasks + listener.totalRunningTasks
    (numRunningOrPendingTasks + tasksPerExecutor - 1) / tasksPerExecutor
  }
```

接着每个计算周期到了之后，会和当前已经有的Executors数：numExecutorsTarget 进行比较。

1. 如果发现  maxNumExecutorsNeeded < numExecutorsTarget 则会发出取消还有没有执行的Container申请。并且重置每次申请的容器数为1,也就是numExecutorsToAdd=1

2. 否则如果发现当前时间now >= addTime(addTime 每次会增加一个sustainedSchedulerBacklogTimeoutS ，避免申请容器过于频繁)，则会进行新容器的申请，如果是第一次，则增加一个(numExecutorsToAdd)，如果是第二次则增加2个以此按倍数类推。直到maxNumExecutorsNeeded <= numExecutorsTarget ,然后就会重置numExecutorsToAdd。

所以我们会发现，我们并不是一次性就申请足够的资源，而是每隔sustainedSchedulerBacklogTimeoutS次时间，按[1,2,4,8]这种节奏去申请资源的。因为在某个sustainedSchedulerBacklogTimeoutS期间，可能已经有很多任务完成了，其实不需要那么多资源了。而按倍数上升的原因是，防止为了申请到足够的资源时间花费过长。这是一种权衡。

## DRA评价

我们发现，DRA(Dynamic Resource Allocation)涉及到的点还是很多的，虽然逻辑比较简单，但是和任务调度密切相关，是一个非常动态的过程。这个设计本身也是面向一个通用的调度方式。

我个人建议如果采用了DRA,可以注意如下几点：

1. 设置一个合理的minExecutors-maxExecutors值
2. 将Executor对应的cpuCore 最好设置为<=3 ，避免Executor数目下降时，等不及新申请到资源，已有的Executor就因为任务过重而导致集群挂掉。
3. 如果程序中有shuffle,例如(reduce*,groupBy*),建议设置一个合理的并行数，避免杀掉过多的Executors。
4. 对于每个Stage持续时间很短的应用，其实不适合这套机制。这样会频繁增加和杀掉Executors，造成系统颠簸。而Yarn对资源的申请处理速度并不快。

## Spark Streaming该使用什么机制动态调整资源

现有的DRA机制其实适合长时的批处理过程中，每个Stage需要的资源量不一样，并且耗时都比较长。Spark Streaming 可以理解为循环的微批处理。而DRA是在每次微批处理起作用，可能还没等DRA反应过来，这个周期就已经过了。

Spark Streaming需要一个从全局一天24小时来考虑。每个调度周期的processing time可能更适合作为增减Executors的标准。同时如果发生delay的话，则可以扩大资源申请的速度。并且，因为是周期性的，释放和新增动作只会发生在一个新的周期的开始，所以他并不会面临现有 DRA的问题，譬如需要通过额外的方式保存Shuffle 状态等。 所以实现起来更加容易。我们可能需要同时监听StreamingContext的一些信息。

具体而言：

 每个周期检查上个周期的处理时间 ，设为 preProcessingTime,周期为duration, 一般而言，我们的Spark Streaming程序都会让preProcessingTime < duration。否则会发生delay。 

如果 preProcessingTime > 0.8 * duration,则一次性将资源申请到maxExecutors。

如果preProcessingTime < duration,则应该删除的Worker为

        removeExecutorNum =  currentExecutors * ((duration -preProcessingTime)/duration - 0.2)

其中0.2 为预留的worker数。如果removeExecutorNum如果<=0 则不进行任何操作。

假设duration =10s, preProcessingTime= 5s, currentExecutors=100，则我们理论上认为 只要保留50%的资源即可。
但是为了防止延时，我们其实额外保留一些20%资源。也就意味着我们删除30个Executor。 我们并不会一次性将资源都释放掉。假设我们增加一个新的参数`spark.streaming.release.num.duration=5`，这个参数意味着我们需要花费5个周期释放掉这30个Executor的资源。也就是当前这个周期，我们要释放掉 6个Executor。

接着到下一个周期，重复上面的计算。 直到计算结果 <=0 为止。
