## 前言

说人话：其实就是讲Spark Streaming 的好处与坑。好处主要从一些大的方面讲，坑则是从实际场景中遇到的一些小细节描述。

## 玫瑰篇

玫瑰篇主要是说Spark Streaming的优势点。

> 玫瑰之代码复用

这主要得益于Spark的设计，以及平台的全面性。你写的流处理的代码可以很方便的适用于Spark平台上的批处理，交互式处理。因为他们本身都是基于RDD模型的，并且Spark Streaming的设计者也做了比较好的封装和兼容。所以我说RDD是个很强大的框，能把各种场景都给框住，这就是高度抽象和思考后的结果。

> 玫瑰之机器学习

如果你使用Spark MLlib 做模型训练。恭喜你，首先是很多算法已经支持Spark Streaming，譬如k-means 就支持流式数据更新模型。 其次，你也可以在Spark Streaming中直接将离线计算好的模型load进来，然后对新进来的数据做实时的Predict操作。

> 玫瑰之SQL支持

Spark Streaming 里天然就可以使用 sql/dataframe/datasets 等。而且时间窗口的使用可以极大扩展这种使用场景，譬如各种系统预警等。类似Storm则需要额外的开发与支持。

> 玫瑰之吞吐和实时的有效控制

Spark Streaming 可以很好的控制实时的程度(小时，分钟，秒)。极端情况可以设置到毫秒。

> 玫瑰之概述

Spark Streaming 可以很好的和Spark其他组件进行交互，获取其支持。同时Spark 生态圈的快速发展，亦能从中受益。

## 刺篇

刺篇就是描述Spark Streaming 的一些问题，做选型前关注这些问题可以有效的降低使用风险。

> checkpoint 之刺

checkpoint 是个很好的恢复机制。但是方案比较粗暴，直接通过序列化的机制写入到文件系统，导致代码变更和配置变更无法生效。实际场景是升级往往比系统崩溃的频率高太多。但是升级需要能够无缝的衔接上一次的偏移量。所以spark streaming在无法容忍数据有丢失的情况下，你需要自己记录偏移量，然后从上一次进行恢复。

我们目前是重写了相关的代码，每次记录偏移量，不过只有在升级的时候才会读取自己记录的偏移量，其他情况都是依然采用checkpoint机制。

> Kafka 之刺

这个和Spark Streaming相关，也不太相关。说相关是因为Spark 对很多异常处理比较简单。很多是和Kafka配置相关的。我举个例子：

如果消息体太大了，超过 `fetch.message.max.bytes=1m`,那么Spark Streaming会直接抛出OffsetOutOfRangeException异常，然后停止服务。

对应的错误会从这行代码抛出：

```
if (!iter.hasNext) {
        assert(requestOffset == part.untilOffset, errRanOutBeforeEnd(part))
        finished = true
        null.asInstanceOf[R]
      }
```

其实就是消费的完成后 实际的消费数据量和预先估计的量不一致。

你在日志中看到的信息其实是这个代码答应出来的：

```
private def errRanOutBeforeEnd(part: KafkaRDDPartition): String =
    s"Ran out of messages before reaching ending offset ${part.untilOffset} " +
    s"for topic ${part.topic} partition ${part.partition} start ${part.fromOffset}." +
    " This should not happen, and indicates that messages may have been lost"
```

解决办法自然是把 `fetch.message.max.bytes` 设置大些。

如果你使用Spark Streaming去追数据，从头开始消费kafka,而Kafka因为某种原因，老数据快速的被清理掉，也会引发OffsetOutOfRangeException错误。并且使得Spark Streaming程序异常的终止。

解决办法是事先记录kafka偏移量和时间的关系(可以隔几秒记录一次)，然后根据时间找到一个较大的偏移量开始消费。

或者你根据目前Kafka新增数据的消费速度，给smallest获取到的偏移量再加一个较大的值，避免出现Spark Streaming 在fetch的时候数据不存在的情况。




> Kafka partition 映射 RDD partition 之刺

Kafka的分区数决定了你的并行度(我们假设你使用Direct Approach的模式集成)。为了获得更大的并行度，则需要进行一次repartition，而repartition 就意味着需要发生Shuffle,在流式计算里，可能会消耗掉我们宝贵的时间。 为了能够避免Shuffle,并且提高Spark Streaming处理的并行度，我们重写了 `DirectKafkaInputDStream`,`KafkaRDD`,`KafkaUtils`等类，实现了一个Kafka partition 可以映射为多个RDD partition的功能。譬如你有M个Kafka partitions,则可映射成  M*N个 RDD partitions。 其中N 为>1 的正整数。

我们期望官方能够实现将一个Kafka的partitions 映射为多个Spark 的partitions,避免发生Shuffle而导致多次的数据移动。

> textFileStream

其实使用textFileStream 的人应该也不少。因为可以很方便的监控HDFS上某个文件夹下的文件，并且进行计算。这里我们遇到的一个问题是，如果底层比如是压缩文件，遇到有顺坏的文件，你是跳不过去的，直接会让Spark Streaming 异常退出。 官方并没有提供合适的方式让你跳过损坏的文件。
以NewHadoopRDD为例，里面有这么几行代码，获取一条新的数据：

```

override def hasNext: Boolean = {
        if (!finished && !havePair) {
         //下面这行是问题的所在点
          finished = !reader.nextKeyValue 
          if (finished) {          
            close()
          }
          havePair = !finished
        }
        !finished
      }

```

通过reader 获取下一条记录的时候，譬如是一个损坏的gzip文件，可能就会抛出异常，而这个异常是用户catch不到的，直接让Spark Streaming程序挂掉了。

而在 HadoopRDD类中，对应的实现如下：

```
override def getNext(): (K, V) = {
        try {
          finished = !reader.next(key, value)
        } catch {
          case eof: EOFException =>
            finished = true
        }
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        (key, value)
      }
```

这里好歹做了个EOFException。然而，如果是一个压缩文件，解压的时候就直接产生错误了，一般而言是 IOException,而不是EOFException了，这个时候也就歇菜了。

个人认为应该添加一些配置，允许用户可以选择如何对待这种有损坏或者无法解压的文件。

因为现阶段我们并没有维护一个Spark的私有版本，所以是通过重写FileInputDStream,NewHadoopRDD 等相关类来修正该问题。


> Shuffle 之刺

Shuffle (尤其是每个周期数据量很大的情况)是Spark Streaming 不可避免的疼痛,尤其是数据量极大的情况，因为Spark Streaming对处理的时间是有限制的。我们有一个场景，是五分钟一个周期，我们仅仅是做了一个repartion，耗时就达到2.1分钟(包括到Kafka取数据)。现阶段Spark 的Shuffle实现都需要落磁盘，并且Shuffle Write 和 Shuffle Read 阶段是完全分开，后者必须等到前者都完成才能开始工作。我认为Spark Streaming有必要单独开发一个更快速，完全基于内存的Shuffle方案。

> 内存之刺

在Spark Streaming中，你也会遇到在Spark中常见的问题，典型如Executor Lost 相关的问题(shuffle fetch 失败，Task失败重试等)。这就意味着发生了内存不足或者数据倾斜的问题。这个目前你需要考虑如下几个点以期获得解决方案：

1. 相同资源下，增加partition数可以减少内存问题。  原因如下：通过增加partition数，每个task要处理的数据少了，同一时间内，所有正在
运行的task要处理的数量少了很多，所有Executor占用的内存也变小了。这可以缓解数据倾斜以及内存不足的压力。

2. 关注shuffle read 阶段的并行数。例如reduce*,group* 之类的函数，其实他们都有第二个参数，并行度(partition数)，只是大家一般都不设置。不过出了问题再设置一下，也不错。

3. 给一个Executor 核数设置的太多，也就意味着同一时刻，在该Executor 的内存压力会更大，GC也会更频繁。我一般会控制在3个左右。然后通过提高Executor数量来保持资源的总量不变。

> 监控之刺

Spark Streaming 的UI 上的Executors Tab缺少一个最大的监控，就是Worker内存GC详情。虽然我们可以将这些信息导入到 第三方监控中，然而终究是不如在 Spark UI上展现更加方便。 为此我们也将该功能列入研发计划。

## 总结

目前Spark Streaming 可以应对的场景不少，但是在很多场景上，还是有这样那样的问题。建议调研后都进一步做测试再做出是否迁移到该平台的决定。
