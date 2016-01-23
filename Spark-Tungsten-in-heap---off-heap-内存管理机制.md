> 这篇文章具体描述了Spark Tungsten project 引入的新的内存管理机制，并且描述了一些使用细节。

# 前言

发现目前还没有这方面的文章，而自己也对这块比较好奇，所以就有了这篇内容。

分析方式基本是自下而上，也就是我们分析的线路会从最基础内存申请到上层的使用。我们假设你对sun.misc.Unsafe 的API有一些最基本的了解。 

## in-heap 和 off-heap (MemoryAllocator)

首先我们看看 Tungsten 的 MemoryAllocator

    off-heap => org.apache.spark.unsafe.memory.UnsafeMemoryAllocator

    in-heap => org.apache.spark.unsafe.memory.HeapMemoryAllocator

off-heap 获取内存很简单：

    long address = Platform.allocateMemory(size);

这样就拿到内存的地址了。这是一个绝对地址，64bit 应该够大。注意，所有的内存都需要8byte对齐。

in-heap 则是维护了一个long类型数组：

     long[] array = new long[(int) (size / 8)];

然后会拿到 Platform.LONG_ARRAY_OFFSET 的地址，以及array对象的所处的相对地址，这样就能拿到一个绝对地址了，并且进行操作了。in-heap的对象有个特点，如果发生了GC,地址可能会变化，所以我们需要一直持有array的引用。

不管 off-heap,in-heap 最终其实都是地址的管理，所以我们抽象出了一个类来描述这个信息。

     org.apache.spark.unsafe.memory.MemoryBlock

一共有四个属性：

    obj  如果是off-heap,则为null。否则则为一个array数组
    
    offset 如果是off-heap 则为绝对偏移量，否则为  Platform.LONG_ARRAY_OFFSET
    
    pageNumber 
    
    length 申请的内存的长度，这个in/off-heap 是一致的。

## 内存管理器(MemoryManager)

实际的内存管理放在了两个层次：

    org.apache.spark.unsafe.memory.ExecutorMemoryManager
    org.apache.spark.unsafe.memory.TaskMemoryManager

我们先分析下他们的关系，TaskMemeoryManager是针对每个Task而言的，但是这些Task都是运行在一个JVM实例上，对应的是Executor,所以内存应该由ExecutorMemoryManager统一进行管理。但是每个task需要交互，所以就让TaskMemeoryManager来进行这种交互。这是他们的分工，设计的很漂亮。

## ExecutorMemoryManager

我们先分析下`ExecutorMemoryManager`，该类根据你的配置，决定是使用什么样的MemoryAllocator，默认是in-heap。你当然也可以设置啦，通过：

     spark.unsafe.offHeap=true 

来进行开启off-heap 模式。

另外，如果发现你是在使用in-heap模式，则ExecutorMemoryManage 会维护一个MemoryBlock的池子，对象池，大家应该很熟悉了。那为啥只有in-heap模式有池子呢？那是因为in-heap 需要申请long[] 数组，维护一个池子，就不用到heap里反复去做申请动作了。

该类有两个核心方法：

     MemoryBlock allocate(long size)
     void free(MemoryBlock memory)

看名字就知道含义了：申请内存和释放内存。内存的单元是MemoryBlock,逻辑上是Page的概念。

## TaskMemeoryManager

这个会复杂些。然而，其实也没多复杂，好吧我又开始犯话唠了毛病了(O(∩_∩)O)。

为了统一对in-heap,off-heap进行建模，避免上层应用要自己区分自己处理的是in-heap还是off-heap内存块,所以这个时候就提出了一个Page的概念，并且使用逻辑地址来做指针，通过这个逻辑地址可以定位到特定一条记录在MemoryBlock的位置。

那么逻辑地址怎么表示呢？答案是用一个Long类型(64-bit)来表示。任何一条记录的位置都可以用一个Long来记录。

我们先来分析复杂的，in-heap模式：

    [13-bit page num][54-bit offset]

这样就能可以表示8192个page。一个Page对应一个MemoryBlock。然后54-bit 可以表示Pb级别的，也就是说这个MemoryBlock可以是超级大的。

不过如果你还记得前文提到的in-heap模式里使用了一个long[]数组作为数据存储的，那么long的长度最大被限制为 Int的最大值，2^32 * 8，也就是32GB。然后所有的Page加起来，大约35个TB。足够大了 其实。

当然这里是这里的限制，在上层里，比如shuffle，可能又会有其他的限制，导致能表示的内存会更小些。这个后续的文章我会进一步阐述。

申请一个Page的流程为：

1. 申请到空闲的Page number号
2. 进行实际的内存分配，得到一个MemoryBlock
3. 将Page number 赋给MemoryBlock

另外这个类也提供了一个不使用Page管理的方法申请内存，然后通过 `allocatedNonPageMemory` 对象进行追踪。


得到MemoryBlock，就代表我们真的拿到了内存，现在我们还要做一件事情，就是把一个记录用一个long类型表示出来,TaskMemoryManager 提供了`encodePageNumberAndOffset(MemoryBlock page, long offsetInPage)` 方法进行编码，编码的方式就是其那面提到的：

      [13-bit page num][54-bit offset]

内部具体的就是一些位操作了。对应的还有各种decode方法。

你会好奇，只有offset,怎么知道一条记录的长度的？这个长度应该也要存储，才能还原回一条信息吧？

目前基本的做法是从offset开始，前四个字节来表示这条记录的长度，然后后面放具体的字节数组。为了解释这个问题，我从`UnsafeShuffleExternalSorter`类里扣了一段代码出来：

```
获得这条记录的逻辑地址，也就是一个64-bit的编码
final long recordAddress =  taskMemoryManager.encodePageNumberAndOffset(dataPage, dataPagePosition);
//dataPageBaseObject 其实就是数组对象的地址，然后以他为基准， 在dataPagePosition 处写入一个int类型数据，这个就是内容的长度。实际的内容就会放到这个位置之后
Platform.putInt(dataPageBaseObject, dataPagePosition, lengthInBytes);
//最后把数据要拷贝的实际的内存中，就需要多要4个字节了。所以这里要加回来
dataPagePosition += 4;
Platform.copyMemory(  recordBaseObject, recordBaseOffset, dataPageBaseObject, dataPagePosition, lengthInBytes);
```

上面分析的都是in-heap。那off-heap呢？
 整个流程也是一致的。区别在于 off-heap拿到的是绝对地址，不是某个页的偏移量，为了统一处理，在进行编码的时候，我们要通过下面的公式重新算off-heap 在page中的相对位置：

       offsetInPage -= page.getBaseOffset();

这里，page.getBaseOffset()是page对应的内存块的起始位置，也就是MemoryBlock的offset变量。如果你还记得上面off-heap申请MemoryBlock的方式，这个就是一开始拿到的偏移量。

这样就拿到相对于MemoryBlock的相对地址了，处理起来就可以和in-heap一致了。

解析的时候，就是反过来就行了，重新得到实际的绝对地址，然后类似in-heap,往前四个字节写长度，后面写实际的内容。

## 总结

我们看到，Spark Tungsten中，内存管理机制其实还是比较简洁明了的。了解这个本身可能用处不是很大，对于实际上层的应用，权当做好玩吧。
