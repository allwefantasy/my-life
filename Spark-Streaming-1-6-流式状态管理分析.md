> Spark 1.6发布后，官方声称流式状态管理有10倍性能提升。这篇文章会详细介绍Spark Streaming里新的流式状态管理。

## 关于状态管理

在流式计算中，数据是持续不断来的，有时候我们要对一些数据做跨周期(Duration)的统计，这个时候就不得不维护状态了。而状态管理对Spark 的 RDD模型是个挑战，因为在spark里，任何数据集都需要通过RDD来呈现，而RDD 的定义是一个不变的分布式集合。在状态管理中，比如Spark Streaming中的word-count 就涉及到更新原有的记录，比如在batch 1 中  `A` 出现1次，batch 2中出现3次，则总共出现了4次。这里就有两种实现：

1. 获取batch 1 中的 状态RDD  和当前的batch RDD 做co-group 得到一个新的状态RDD。这种方式完美的契合了RDD的不变性，但是对性能却会有比较大的影响,因为需要对所有数据做处理，计算量和数据集大小是成线性相关的。这个我们后续会详细讨论。
2.  第二种是一种变通的实现。因为没法变更RDD/Partition等核心概念，所以Spark Streaming在集合元素上做了文章，定义了`MapWithStateRDD `，将该RDD的元素做了限定，必须是`MapWithStateRDDRecord` 这个东西。该MapWithStateRDDRecord 保持有某个分区的所有key的状态(通过stateMap记录)以及计算结果(mappedData),元素MapWithStateRDDRecord 变成可变的，但是RDD 依然是不变的。

这两个方案分别对应了 updateStateByKey/mapWithState  的实现。

## 前言

在这篇文章中，[Apache Spark 1.6发布](http://geek.csdn.net/news/detail/49664)，提到了spark1.6 三块性能改进：

* Parquet性能
* [自动内存管理模型](http://www.jianshu.com/p/b250797b452a)
* [流式状态管理10倍性能提升](http://www.jianshu.com/p/1463bc1d81b5)

之前就想系统的对这三块仔细阐述下。现在总算有了第二篇。

本文会从三个方面展开：

1. [updateStateByKey的实现](#updateStateByKey的实现);
2. [mapWithState(1.6新引入的流式状态管理)的实现](#mapWithState(1.6新引入的流式状态管理)的实现)
3. [mapWithState额外内容](mapWithState额外内容)



## updateStateByKey的实现

在 [关于状态管理](#关于状态管理)中，我们已经描述了一个大概。该方法可以在`org.apache.spark.streaming.dstream.PairDStreamFunctions`中找到。调用该方法后会构建出一个`org.apache.spark.streaming.dstream.StateDStream`对象。计算的方式也较为简单,核心逻辑是下面两行代码：

```
val cogroupedRDD = parentRDD.cogroup(prevStateRDD, partitioner)
val stateRDD = cogroupedRDD.mapPartitions(finalFunc, preservePartitioning)
Some(stateRDD)
```
首先将`prevStateRDD` 和 `parentRDD`(新batch 的数据) 做一次cogroup,形成了 ` (K, Seq[V], Seq[W]) ` 这样的结果集。你会发现和updateStateByKey 要求的`(Seq[V], Option[S])`签名还是有些类似的。事实上这里的Seq[V] 就是parentRDD的对应K 的新的值。为了适配他两，Spark 内部会对你传进来的`updateFunc` 做两次转换，从而使得你的函数能够接受` (K, Seq[V], Seq[W]) `这样的参数。看到这，想必你也就知道为啥updateStateByKey  接受的函数签名是那样的了。

前文我们提到，这样做很漂亮，代码也少，契合RDD的概念，然而你会发现无论parentRDD里有多少key,哪怕是只有一个，也需要对原有所有的数据做cogroup 并且全部做一遍处理(也就是应用你的update函数)。显然这是很低效的。很多场景下，新的batch 里只有一小部分数据，但是我们却不得不对所有的数据都进行计算。

正因为上面的问题，所以Spark Streaming 提出了一个新的API `mapWithState`,对应的jira为：[Improved state management for Spark Streaming](https://issues.apache.org/jira/browse/SPARK-2629) 。除了我前面提到的性能问题，新的API 还提供两个新的功能：

1. 可以为Key 设置TTL(Timeout)
2. 用户可以对返回值进行控制

## mapWithState(1.6新引入的流式状态管理)的实现

前面我们提到，在新的mapWithState API 中，核心思路是创建一个新的MapWithStateRDD,该RDD的元素是 `MapWithStateRDDRecord`，每个MapWithStateRDDRecord 记录某个Partiton下所有key的State。

依然的，你在`org.apache.spark.streaming.dstream.PairDStreamFunctions` 可以看到mapWithState 签名。

```
@Experimental
  def mapWithState[StateType: ClassTag, MappedType: ClassTag](
      spec: StateSpec[K, V, StateType, MappedType]
    ): MapWithStateDStream[K, V, StateType, MappedType] = {
    new MapWithStateDStreamImpl[K, V, StateType, MappedType](
      self,
      spec.asInstanceOf[StateSpecImpl[K, V, StateType, MappedType]]
    )
  }
```

这一段代码有三点值得注意：

1. 该接口在1.6 中还是 `Experimental` 状态
2. 接受的不是一函数，而是一个StateSpec 的对象。
3. 返回了一个新的DStream

其实StateSpec 只是一个包裹，你在实际操作上依然是定义一个函数，然后通过StateSpec进行包裹一下。以 wordcount 为例：

```
   val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
      val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
      val output = (word, sum)
      state.update(sum)
      output
    }
```

接着StateSpec.function(mappingFunc) 包裹一下就可以传递给mapWithState。
我们看到该函数更加清晰，word 是K,one新值，state 是原始值(本batch之前的状态值)。这里你需要把state 更新为新值，该实现是做了一个内部状态维护的，不像updateStateByKey一样，一切都是现算的。

`MapWithStateDStreamImpl` 的compute逻辑都委托给了`InternalMapWithStateDStream`,最终要得到`MapWithStateRDD`,基本是通过下面的逻辑来计算的：

```
val prevStateRDD = getOrCompute(validTime - slideDuration) match ...
val dataRDD = parent.getOrCompute(validTime).getOrElse {  context.sparkContext.emptyRDD[(K, V)]}
.....
val partitionedDataRDD = dataRDD.partitionBy(partitioner)
Some(new MapWithStateRDD(  prevStateRDD, partitionedDataRDD, mappingFunction, validTime, timeoutThresholdTime))

```
这里有个很重要的操作是对dataRDD进行了partition操作，保证和prevStateRDD 按相同的分区规则进行分区。这个在后面做计算时有用。

获取到prevStateRDD,接着获取当前batch的数据的RDD,最后组装成一个新的MapWithStateRDD。MapWithStateRDD 还接受你定义的函数`mappingFunction`以及key的超时时间。

其中MapWithStateRDD 和别的RDD 不同之处在于RDD里的元素是`MapWithStateRDDRecord` 对象。其实prevStateRDD  也是个MapWithStateRDD 。

整个实际计算逻辑都在` MapWithStateRDDRecord.updateRecordWithData` 方法里。

前面我们提到，MapWithStateRDDRecord 是prevStateRDD 里的元素。有多少个分区，就有多少个MapWithStateRDDRecord 。一个Record 对应一个分区下所有数据的状态。在` MapWithStateRDDRecord.updateRecordWithData` 方法中，第一步是copy 当前record 的状态。这个copy是非常快的。我们会在`mapWithSate额外内容` 那个章节有更详细的分析。

     val newStateMap = prevRecord.map { _.stateMap.copy() }. getOrElse { new EmptyStateMap[K, S]() } //prevRecord=MapWithStateRDDRecord[K, S, E]

接着定义了两个变量，其中mappedData  会作为最后的计算结果返回，wrappedState 类似Hadoop里的 Text,你可以不断给它赋值，然后获得一些新的功能，避免返回创建对象。它主要是给state添加了一些方法，比如update,define状态等。

     val mappedData = new ArrayBuffer[E]
     val wrappedState = new StateImpl[S]()

接着遍历当前batch 所有的数据，并且应用用户定义的函数。这里我们看到，我们只对当前batch的数据进行函数计算，而不是针对历史全集数据进行计算，这是一个很大的性能提升点。接着根据wrappedState的状态对newStateMap做更新，主要是删除或者数据的更新。最后将新的结果返回并且放到mappedData 。

```
dataIterator.foreach { case (key, value) =>
      wrappedState.wrap(newStateMap.get(key))
      val returned = mappingFunction(batchTime, key, Some(value), wrappedState)
      if (wrappedState.isRemoved) {
        newStateMap.remove(key)
      } else if (wrappedState.isUpdated
          || (wrappedState.exists && timeoutThresholdTime.isDefined)) {
        newStateMap.put(key, wrappedState.get(), batchTime.milliseconds)
      }
      mappedData ++= returned
    }
```

上面这段逻辑，你会发现一个问题，如果dataIterator 里有重复的数据比如某个K 出现多次，则mappedData也会有多次。以wordcount 为例：

|输入数据|	mapWithState后的结果	|调用stateSnapshots后的结果|
|---|---|---|
|(hello, 1)	|(hello, 1)|	(hello, 3)|
|(hello, 1)|	(hello, 2)	|(world, 2)
|(world, 1)	|(world, 1)	||
|(world, 1)|	(world, 2)|	|
|(hello, 1)	|(hello, 3)||

hello 出现了三次，所以会加入到mappedData中三次。其实我没发现这么做的意义，并且我认为会对内存占用造成一定的压力。

如果你想要最后的结果，需要调用完mapWithState 之后需要再调用一次stateSnapshots，就可以拿到第三栏的计算结果了。

经过上面的计算，我们对parentRDD里的每个分区进行计算，得到了mappedData以及newStateMap，这两个对象一起构建出MapWithStateRDDRecord，而该Record 则形成一个Partition,最后构成新的MapWithStateRDD。 


## mapWithState额外内容

MapWithStateRDDRecord 透过stateMap 维护了某个分区下所有key的当前状态。 在前面的分析中，我们第一步便是clone old stateMap。如果集合非常大，拷贝也是很费时才对，而且还耗费内存。

所以如何实现好stateMap 变得非常重要：

1.  实现过程采用的是 `增量copy`。也叫deltaMap。 新创建的stateMap 会引用旧的stateMap。新增数据会放到新的stateMap中，而更新，删除，查找等操作则有可能发生在老得stateMap上。
缺点也是有的，如果stateMap 链路太长，则可能会对性能造成一定的影响。我们只要在特定条件下做合并即可。目前是超过DELTA_CHAIN_LENGTH_THRESHOLD=20 时会做合并。

2.  使用 `org.apache.spark.util.collection.OpenHashMap`，该实现比`java.util.HashMap` 快5倍，并且占用更少的内存空间。不过该HashMap 无法进行删除操作。

