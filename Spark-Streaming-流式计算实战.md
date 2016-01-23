> 这篇文章由一次平安夜的微信分享整理而来。在Stuq 做的分享，[原文内容](http://www.stuq.org/page/detail/660)。 

## 业务场景

这次分享会比较实战些。具体业务场景描述：

我们每分钟会有几百万条的日志进入系统，我们希望根据日志提取出时间以及用户名称，然后根据这两个信息形成  

* userName/year/month/day/hh/normal 
* userName/year/month/day/hh/delay 

路径,存储到HDFS中。如果我们发现日志产生的时间和到达的时间相差超过的一定的阈值，那么会放到 delay 目录，否则放在正常的 normal 目录。

##  Spark Streaming 与 Storm 适用场景分析

为什么这里不使用 Storm呢? 我们初期确实想过使用 Storm 去实现，然而使用 Storm 写数据到HDFS比较麻烦：

   ＊ Storm 需要持有大量的 HDFS 文件句柄。需要落到同一个文件里的记录是不确定什么时候会来的，你不能写一条就关掉，所以需要一直持有。
   ＊ 需要使用HDFS 的写文件的 append 模式，不断追加记录。

大量持有文件句柄以及在什么时候释放这些文件句柄都是一件很困难的事情。另外使用 HDFS 的追加内容模式也会有些问题。

 后续我们就调研 Spark Streaming 。 Spark Streaming 有个好处，我可以攒个一分钟处理一次即可。这就意味着，我们可以隔一分钟(你当然也可以设置成五分钟，十分钟)批量写一次集群，HDFS 对这种形态的文件存储还是非常友好的。这样就很轻易的解决了 Storm 遇到的两个问题。

实时性也能得到保证，譬如我的 batch interval 设置为 一分钟 那么我们就能保证一分钟左右的延迟 ，事实上我们的业务场景是可以容忍半小时左右的。

当然，Spark 处理完数据后，如何落到集群是比较麻烦的一件事情，不同的记录是要写到不同的文件里面去的，没办法简单的 saveAsTextFile 就搞定。这个我们通过自定义 Partitioner 来解决，第三个环节会告诉大家具体怎么做。 

上面大家其实可以看到 Spark Streaming 和 Storm 都作为流式处理的一个解决方案，但是在不同的场景下，其实有各自适合的时候。 

##  Spark Streaming 与 Kafka 集成方案选型

我们的数据来源是Kafka ,我们之前也有应用来源于 HDFS文件系统监控的,不过建议都尽量对接 Kafka 。

Spark Streaming 对接Kafka 做数据接受的方案有两种：
    ＊Receiver-based Approach
    ＊Direct Approach (No Receivers)

两个方案具体优劣我专门写了文章分析，大家晚点可以看看这个链接和 Spark Streaming 相关的文章。 

[我的技术博文](http://www.jianshu.com/users/59d5607f1400)

我这里简单描述下：
    ＊Receiver-based Approach 内存问题比较严重，因为她接受数据和处理数据是分开的。如果处理慢了，它还是不断的接受数据。容易把负责接受的节点给搞挂了。
    ＊ Direct Approach 是直接把 Kafka 的 partition 映射成 RDD 里的 partition 。 所以数据还是在 kafka 。只有在算的时候才会从 Kafka 里拿，不存在内存问题，速度也快。

所以建议都是用 Direct Approach 。具体调用方式是这样：
![](http://upload-images.jianshu.io/upload_images/1063603-96783a5c9c05d6a1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

还是很简单的，之后就可以像正常的 RDD 一样做处理了。

## 自定义 Partitioner 实现日志文件快速存储到 HDFS

![](http://upload-images.jianshu.io/upload_images/1063603-630011f25481d4b4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

经过处理完成后 ，我们拿到了logs 对象 。

到这一步位置，日志的每条记录其实是一个 tuple(path,line)  也就是每一条记录都会被标记上一个路径。那么现在要根据路径，把每条记录都写到对应的目录去该怎么做呢？

一开始想到的做法是这样：

![](http://upload-images.jianshu.io/upload_images/1063603-498fa4db0f36b79d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

首先收集到所有的路径。接着 for 循环 paths ,然后过滤再进行存储，类似这样：

![](http://upload-images.jianshu.io/upload_images/1063603-288b7843554ab8c4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

这里我们一般会把 rdd 给 cache 住，这样每次都直接到内存中过滤就行了。但如果 path 成百上千个呢？ 而且数据量一分钟至少几百万，save 到磁盘也是需要时间的。所以这种方案肯定是不可行的。

我当时还把 paths 循环给并行化了，然而当前情况是 CPU 处理慢了，所以有改善，但是仍然达不到要求。

这个时候你可能会想，要是我能把每个路径的数据都事先收集起来，得到几个大的集合，然后把这些集合并行的写入到 HDFS 上就好了。事实上，后面我实施的方案也确实是这样的。所谓集合的概念，其实就是 Partition 的概念。而且这在Spark 中也是易于实现的，而实现的方式就是利用自定义 Partioner 。具体的方式如下：

![](http://upload-images.jianshu.io/upload_images/1063603-a480884bf4fb1cad.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

通过上面的代码，我们就得到了路径和 partiton id 的对应关系。接着遍历 partition 就行了。对应的 a 是分区号，b 则是分区的数据迭代器。接着做对应的写入操作就行。这些分区写入都是在各个 Executor 上执行的，并不是在 Driver 端，所以足够快。

我简单解释下代码 ，首先我把收集到的路径 zipWithIndex 这样就把路径和数字一一对应了 ；接着我新建了一个匿名类 实现了 Partitioner 。numPartitions 显然就是我们的路径集合的大小，遇到一个 key (其实就是个路径)时，则调用路径和数字的映射关系 ，然后就把所有的数据根据路径 hash 到不同的 partition 了 。接着遍历 partition 就行了，对应的 a 是分区号，b 则是分区的数据迭代器。接着做对应的写入操作就行。这些分区写入都是在各个 Executor 上执行的，并不是在 Driver 端，所以足够快。我们在测试集群上五分钟大概 1000-2000w 数据，90颗核，180G 内存，平均处理时间大概是2分钟左右。内存可以再降降  我估计 100G 足够了  。

## 在演示场景中，Spark Streaming 如何保证数据的完整性，不丢，不重

虽然 Spark Streaming 是作为一个24 * 7 不间断运行的程序来设计的，但是程序都会 crash ,那如果 crash 了，会不会导致数据丢失？会不会启动后重复消费？

关于这个，我也有专门的文章阐述([http://www.jianshu.com/p/885505daab29](http://www.jianshu.com/p/885505daab29) ),

我这里直接给出结论：

＊ 使用 Direct Approach 模式
＊ 启用 checkPoint 机制

做到上面两步，就可以保证数据至少被消费一次。

那如何保证不重复消费呢？

这个需要业务自己来保证。简单来说，业务有两种：

    ＊ 幂等的
    ＊ 自己保证事务

所谓幂等操作就是重复执行不会产生问题，如果是这种场景下，你不需要额外做任何工作。但如果你的应用场景是不允许数据被重复执行的，那只能通过业务自身的逻辑代码来解决了。

以当前场景为例，就是典型的幂等 ，因为可以做写覆盖 ，

![](http://upload-images.jianshu.io/upload_images/1063603-207ee36a99220e32.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

具体代码如上 ，那如何保证写覆盖呢？ 

文件名我采用了 job batch time 和 partition 的 id 作为名称。这样，如果假设系统从上一次失败的 job 重新跑的时候，相同的内容会被覆盖写，所以就不会存在重复的问题。

## 回顾 

我们每分钟会有几百万条的日志进入系统，我们希望根据日志提取出时间以及用户名称，然后根据这两个信息形成  

* userName/year/month/day/hh/normal 
* userName/year/month/day/hh/delay 

路径,存储到HDFS中。如果我们发现日志产生的时间和到达的时间相差超过的一定的阈值，那么会放到 delay 目录，否则放在正常的 normal 目录。
 
我们作了四个方面的分析： 

  1. Spark Streaming 与 Storm 适用场景分析 ；
  2. Spark Streaming 与 Kafka 集成方案选型，我们推荐Direct Approach 方案 ；
  3. 自定义 Partitioner 实现日志文件快速存储到HDFS ；
  4. Spark Streaming 如何保证数据的完整性，不丢，不重 。

好的  感谢大家 圣诞快乐 ^_^

## Q&A

> Q1. spark streaming 可以直接在后面连上 elasticsearch 么？

A1. 可以的。透露下，我马上也要做类似的实践。
      
> Q2. 公司选用 storm 是由于它可以针对每条日志只做一次处理，spark streaming 可以做到么？

A2.  spark streaming 是按时间周期的， 需要攒一段时间，再一次性对获得的所有数据做处理
                
> Q3. 什么是文件句柄？

A3. HDFS 写入 你需要持有对应的文件的 client 。不可能来一条数据，就重新常见一个链接，然后用完就关掉。

> Q4. Spark 分析流数据，分析好的数据怎么存到 mysql 比较好？

A4. 我没有这个实践过存储到 MySQL。一般数据量比较大，所以对接的会是 Reids/HBase/HDFS。
            
> Q5. 有没有尝试过将数据写入 hive？

A5. 没有。但没有问题的。而且 Spark Streaming 里也可以使用 Spark SQL 。我不知道这会不会有帮助。
         
> Q6. 幂等是什么？

A6. 就是反复操作不会有副作用。
                
> Q7. 可不可以分享一下 spark 完整的应用场景？

A7. 这个有点太大。 目前 spark 覆盖了离线计算，数据分析，机器学习，图计算，流式计算等多个领域，目标也是一个通用的数据平台，所以一般你想到的都能用 spark 解决。
             
> Q8. 如何理解日志产生时间和到达时间相差超过一定的阈值？

A8. 每条日志都会带上自己产生的时间。同时，如果这条日志到我们的系统太晚了，我们就认为这属于延时日志。
                       
> Q9. 目前这套体系稳定性如何？会不会有经常d节点的情况？

A9. 稳定性确实有待商榷 建议小范围尝试。
          
> Q10. Spark Streaming 内部是如何设计并解决 storm 存在的两个问题的？老师能分析一下细节吗？

A10. 这和 Spark Streaming 的设计是相关的。微批处理模式使得我们可以一个周期打开所有文件句柄，然后直接写入几千万条数据，然后关闭。第二个是使用 partition 并行加快写入速度。
            
> Q11. 如何应对网络抖动导致阻塞？

A11. Spark 本身有重试机制,还有各种超时机制。
           
> Q12. 怎样保证消息的及时性？

A12. 依赖于数据源，kafka,Spark Streaming 是否处理能力充足，没有 delay . 所有环节都会影响消息的及时性。
        
> Q13. 实际运用中，分析完的数据，本身有很大的结构关系，有时又需要对数据二次补充，处理完的数据量不大，该选哪种存储方式？

A13. 能用分布式存储的就用分布式存储。可以不做更新的，尽量不做更新。我一般推荐对接到 HBase 。
           
> Q14. Streaming 字面是流的意思，倒是课程中提到对日志有延迟的考虑，是 Spark  Streaming 是自定一个周期，处理周期到达的数据集合，通俗讲感觉像批处理，不是每条记录不一定要有时间戳？

A14. 你理解对了。每条记录没有时间戳。如果有，也是日志自己带的。Spark Streaming 并不会给每条记录带上时间。
              

> Q16. storm 避免重复是依赖 zookeeper，Spark Streaming 靠什么记录处理到哪行呢？

A16. 通过 checkpoint 机制，自己维护了 zookeeper 的偏移量。
            
> Q17. 请问一下 Spark Streaming 处理日志数据的压测结果如何呢？

Q17. 刚刚说了，在我们的测试集群里， 1000-2000w 条记录，平均处理时间大约2分钟，90颗核，180G 内存。没有任何调优参数。理论内存可以继续降低,，因为不 cache 数据 。
          
> Q18. AMQ 与他们之间区别和联系？

A18. AMQ 也是消息队列？ Spark Streaming 支持相当多的消息队列。
           
> Q19. 国内 spark 集群部署在哪些云上？

A19. 没有用过云。
                              
> Q21. zookeeper 目前 hbase 都不想依赖它了，因为会导致系统的不稳定，请问老师怎么看？

A21. 还好吧，产生问题主要是 client 太多。比如 hbase 依赖 zookeeper，所有使用 hbase 的，都需要先和 zookeeper 建立连接，这对 zookeeper 产生较大的压力。其他的系统也类似。如果共享 zookeeper 集群，那么它的连接数会成为一个瓶颈。
