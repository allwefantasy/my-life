> 2016年1月4号 Spark 1.6 发布。提出了一个新的内存管理模型： Unified Memory Management。这篇文章会详细分析新的内存管理模型，方便大家做调优。

## 前言

新的内存模型是在这个Jira提出的，[JIRA-10000](https://issues.apache.org/jira/browse/SPARK-10000)，对应的设计文档在这：[[unified-memory-management](https://issues.apache.org/jira/secure/attachment/12765646/unified-memory-management-spark-10000.pdf)](https://issues.apache.org/jira/secure/attachment/12765646/unified-memory-management-spark-10000.pdf)。

贴出这个文档是为了让大家可以更深入的了解这个新的模型的设计动机，社区想到的解决方案，以及经过对比，最终选择了哪个方案。当然我在文章中也会有所提及，但这个不会是本文的重点。

## Memory Manager

在Spark 1.6 版本中，memoryManager 的选择是由 

     spark.memory.useLegacyMode=false

决定的。如果采用1.6之前的模型，这会使用`StaticMemoryManager`来管理，否则使用新的`UnifiedMemoryManager`

我们先看看1.6之前，对于一个Executor,内存都有哪些部分构成：

1. ExecutionMemory。这片内存区域是为了解决 shuffles,joins, sorts and aggregations 过程中为了避免频繁IO需要的buffer。 通过spark.shuffle.memoryFraction(默认 0.2) 配置。

2. StorageMemory。这片内存区域是为了解决 block cache(就是你显示调用dd.cache, rdd.persist等方法), 还有就是broadcasts,以及task results的存储。可以通过参数 spark.storage.memoryFraction(默认0.6)。设置

3. OtherMemory。给系统预留的，因为程序本身运行也是需要内存的。 ​(默认为0.2).​

另外，为了防止OOM,一般而言都会有个safetyFraction，比如ExecutionMemory 真正的可用内存是  `spark.shuffle.memoryFraction * spark.shuffle.safetyFraction` 也就是0.8 * 0.2 ,只有16%的内存可用。
这种内存分配机制，最大的问题是，谁都不能超过自己的上限，规定了是多少就是多少，虽然另外一片内存闲着呢。这在是StorageMemory  和 ExecutionMemory比较严重，他们都是消耗内存的大户。

这个问题引出了Unified Memory Management模型，重点是打破ExecutionMemory 和 StorageMemory 这种分明的界限。


## OtherMemory

Other memory在1.6也做了调整，保证至少有300m可用。你也可以手动设置 `spark.testing.reservedMemory` . 然后把实际可用内存减去这个reservedMemory得到 usableMemory。 ExecutionMemory 和 StorageMemory 会共享`usableMemory * 0.75`的内存。0.75可以通过 新参数 `spark.memory.fraction` 设置。目前`spark.memory.storageFraction` 默认值是0.5,所以ExecutionMemory，StorageMemory默认情况是均分上面提到的可用内存的。

## UnifiedMemoryManager

这个类提供了两个核心的方法：

    acquireExecutionMemory 
    acquireStorageMemory


### acquireExecutionMemory

每次申请`ExecutionMemory` 的时候，都会调用 `maybeGrowExecutionPool`方法，基于该方法我们可以得到几个有意义的结论：


* 如果ExecutionMemory 内存充足，则不会触发向Storage申请内存
* 每个Task能够被使用的内存被限制在 poolSize / (2 * numActiveTasks) ~ maxPoolSize / numActiveTasks   之间。

```
maxPoolSize = maxMemory - math.min(storageMemoryUsed, storageRegionSize)

poolSize = ExecutionMemoryPool.poolSize （当前ExecutionMemoryPool 所持有的内存）
```
* 如果ExecutionMemory 的内存不足，则会触发向StorageMemory索引要内存的操作。
   
如果ExecutionMemory 的内存不足，则会向 StorageMemory要内存，具体怎么样呢? 看下面一句代码就懂了：

```
val memoryReclaimableFromStorage =  math.max(storageMemoryPool.memoryFree, storageMemoryPool.poolSize - storageRegionSize)
```

看StorageMemoryPool的剩余内存和 storageMemoryPool 从ExecutionMemory借来的内存那个大，取最大的那个，作为可以重新归还的最大内存。用公式表达出来就是这一个样子：

ExecutionMemory 能借到的最大内存= StorageMemory 借的内存 + StorageMemory 空闲内存

当然，如果实际需要的小于能够借到的最大值，则以实际需要值为准。下面的代码体现了这个逻辑：

```
val spaceReclaimed = storageMemoryPool.shrinkPoolToFreeSpace(  
      math.min(extraMemoryNeeded,memoryReclaimableFromStorage))

onHeapExecutionMemoryPool.incrementPoolSize(spaceReclaimed)
```

### acquireStorageMemory

流程和acquireExecutionMemory类似，但是区别是，当且仅当ExecutionMemory有空闲内存时，StorageMemory 才能借走该内存。这个逻辑体现在这行代码上：

     val memoryBorrowedFromExecution = Math.min(onHeapExecutionMemoryPool.memoryFree, numBytes)

所以StorageMemory从ExecutionMemory借走的内存，完全取决于当时ExecutionMemory是不是有空闲内存。

### MemoryPool 

前面讲的是StorageMemory和ExecutionMemory的交互。现在内存的具体表示则是由 MemoryPool完成的。

UnifiedMemoryManage 维护了三个对象：

```
@GuardedBy("this")
  protected val storageMemoryPool = new StorageMemoryPool(this)
  @GuardedBy("this")
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, "on-heap execution")
  @GuardedBy("this")
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, "off-heap execution")
```

真实的内存计数其实都是由这几个对象来完成的。比如

* 内存的借出借入
* task目前内存的使用跟踪

值的注意的是，我们以前知道，系统shuffle的时候，是可以使用in-heap /off-heap 内存的。在UnifiedMemoryManage中，用了不同的对象来追踪。如果你开启的是offHeapExecutionMemoryPool，则不存在和StorageMemory的交互，也就没有动态内存的概念了。

## 总结

1. 理论上可以减少Shuffle spill数，极端情况可能中间就没有spill过程了，可以大大减少IO次数
2. 如果你的内存太紧张，可能无法缓解问题
3. 如果你的程序具有偏向性，比如重度ExectionMemory  或者StorageMemory 的某一个，则可能会带来比较好的效果
