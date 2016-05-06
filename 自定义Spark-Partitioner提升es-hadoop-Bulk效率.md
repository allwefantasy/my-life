## 前言

之前写过一篇文章，[如何提高ElasticSearch 索引速度](http://www.jianshu.com/p/5eeeeb4375d4)。除了对ES本身的优化以外，我现在大体思路是尽量将逻辑外移到Spark上,Spark的分布式计算能力强，cpu密集型的很适合。这篇文章涉及的调整也是对[SparkES 多维分析引擎设计](http://www.jianshu.com/p/556ac1bd29ea) 中提及的一个重要概念“shard to partition ,partition to shard ” 的实现。不过目前只涉及到构建索引那块。

## 问题描述

当你bulk数据到集群，按照[ElasticSearch Bulk 源码解析](http://www.jianshu.com/p/62febe581fcb)所描述的:

>   接着通过executeBulk方法进入原来的流程。在该方法中，对bulkRequest.requests 进行了两次for循环。

>第一次判定如果是IndexRequest就调用IndexRequest.process方法，主要是为了解析出timestamp,routing,id,parent 等字段。

> 第二次是为了对数据进行分拣。大致是为了形成这么一种结构：

第二次就是对提交的数据进行分拣，然后根据route/_id 等值找到每个数据所属的Shard，最后将数据发送到对应Shard所在的Node节点上。

然而这导致了两个问题：

1. ES Node之间会形成N*N个连接，消耗掉过多的bulk线程
2. 出现了很多并不需要的网络IO

所以我们希望能够避免这种情况。

## Spark Partition to ES Shard

我们希望能够将分拣的逻辑放到Spark端，保证Spark 的Partition 和ES的Shard 一一对应，并且实现特定的Partitoner 保证数据到达ES都会被对应的Shard所在的节点直接消费，而不会再被转发到其他节点。
经过我的实际测试，做了该调整后，写入QPS有两倍以上的提升


## 理论基础

这里的理论基础自然是es-hadoop项目。

类的调用路径关系为：

```
EsSpark -> 
     EsRDDWriter -> 
           RestService -> 
                  RestRepository -> 
                            RestClient ->
                                NetworkClient -> 
                                        CommonsHttpTransport
```

简单介绍下他们的作用：

* EsSpark, 读取ES和存储ES的入口。通过隐式转换，会显得更Spark.
* EsRDDWriter ,调用RestService创建PartitionWriter,对ES进行数据写入
* RestService，负责创建 RestRepository，PartitionWriter
* RestRepository，bulk高层抽象，底层利用NetworkClient做真实的http请求，另外也维护Buffer相关的，典型比如积攒了多少条，多少M之后进行flush等。
* NetworkClient 对 CommonsHttpTransport的封装，主要添加了一些节点校验功能。
* CommonsHttpTransport 你可以认为是对HttpClient的一个封装

原来我以为需要对es-hadoop项目的源码进行修改才能实现前面提到的逻辑。事实上基于es-hadoop很容易实现上面提到的需求。

我们现在解释下为什么不需要修改源码。

在RestService类里，构建RestRepository的时候，会判定是多索引还是单索引。对应代码如下：

```
RestRepository repository = (iformat.hasPattern() ?
 initMultiIndices(settings, currentSplit, resource, log) : 
initSingleIndex(settings, currentSplit, resource, log));
```

这里我们只解析单索引部分代码，在对应的initSingleIndex方法里有如下代码：

```
int bucket = currentInstance % targetShards.size();
Shard chosenShard = orderedShards.get(bucket);
Node targetNode = targetShards.get(chosenShard);
```
先简要说明下几个参数变量。

* targetShards 是索引所有的主分片到对应Node节点的映射。
* orderedShards 则是根据shardId 顺序排序Shard集合
* currentInstance 是partitionId

因为我们已经通过partitioner 将partitionId 转化为shardId，
,也就是partitionId X 里的数据，都是属于shardId 为X。 也就是说currentInstance == partitionId == shardId。
下面是我们推导出来的关系：

* currentInstance < targetShards.size() 
* bucket == currentInstance == partitionId == shardId
* targetNode 持有ShardId=currentInstance 的Primary Shard

所以这段代码实际完成了partitionId 到 targetNode的映射关系。

## ESShardPartitioner 实现

涉及到这块的主要有 es-hadoop 的mr以及 spark模块。在mr模块里包含了ES的分片规则实现。 spark 模块则包含ESShardPartitioner类。

代码如下：

```
package org.elasticsearch.spark
import ....
class ESShardPartitioner(settings:String) extends Partitioner {
      protected val log = LogFactory.getLog(this.getClass())
      
      protected var _numPartitions = -1 
      
      override def numPartitions: Int = {   
        val newSettings = new PropertiesSettings().load(settings)
        val repository = new RestRepository(newSettings)
        val targetShards = repository.getWriteTargetPrimaryShards(newSettings.getNodesClientOnly())
        repository.close()
        _numPartitions = targetShards.size()
        _numPartitions
      }

      override def getPartition(key: Any): Int = {
        val shardId = ShardAlg.shard(key.toString(), _numPartitions)
        shardId
      }
}

public class ShardAlg {
    public static int shard(String id, int shardNum) {
        int hash = Murmur3HashFunction.hash(id);
        return mod(hash, shardNum);
    }

    public static int mod(int v, int m) {
        int r = v % m;
        if (r < 0) {
            r += m;
        }
        return r;
    }
}
```

使用方式如下：

```

......partitionBy(new ESShardPartitioner(settings)).foreachPartition { iter =>
      try {
        val newSettings = new PropertiesSettings().load(settings)
        //创建EsRDDWriter
        val writer = EsRDDCreator.createWriter(newSettings.save())
        writer.write(TaskContext.get(), iter.map(f => f._2))        
      }
```

不过这种方式也是有一点问题，经过partition 后，Spark Partition Num==ES Primary Shard Num，这样会使得Spark写入并发性会受到影响。

这个和Spark Streaming 里KafkaRDD 的partition数受限于Kafka Partition Num 非常类似。我之前也对这个做了扩展，是的多个Spark Partition 可以映射到同一个Kafka Partition.

所以这里有第二套方案：

1. 修改ESShardPartitioner，可以让多个分区对应一个Shard,并且通过一个Map维护这个关系
2. 每个分区通过EsRDDWriter指定shardId进行写入。

第二点可能需要修改es-hadoop源码了，不过修改也很简单，通过settings传递shardId,然后在RestService.initSingleIndex添加如下代码：

```
if(settings.getProperty(ConfigurationOptions.ES_BULK_SHARDID) != null){       
        	targetNode = targetShards.get(orderedShards.get(Integer.parseInt(settings.getProperty(ConfigurationOptions.ES_BULK_SHARDID))));
        }
```

在创建EsRDDWriter时拷贝settings的副本并且加入对应的ConfigurationOptions.ES_BULK_SHARDID.

使用时类似下面这个例子：

```
//val settings = new SparkSettings(conf).save()
.partitionBy(new ESShardPartitioner(settings)).mapPartitionsWithIndex { (partitionIndex, iter) =>
      try {
      val writer = EsSpark.createEsRDDWriter[Map[String,String]](settings, resource)
       //shardToPartitions个 Spark partition 对应一个ES Shard
        val shardId = ESShardPartitioner.shardIdFromPartitionId(partionId, shardToPartitions)
       //强制该分片写入到特定的Shard里
        val stats = writer.writeToSpecificPrimaryShard(TaskContext.get(), shardId, iter.map(f => f._2))
        List(NewStats(stats.bulkTotalTime, stats.docsSent)).iterator
      } catch {
```

这样可以把一份数据切成多分，并发写入ES的某个Shard.

## 总结

将ES的计算外移到Spark在这个场景中还是比较容易的。下次我还会专门写篇文章，剖析es-hadoop的实现，以及一些关键参数，尤其是一些类的使用。方便我们对es-hadoop实现定制化修改。
