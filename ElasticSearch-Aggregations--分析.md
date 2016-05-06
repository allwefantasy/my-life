> 承接上篇文章 [ElasticSearch Rest/RPC 接口解析](http://www.jianshu.com/p/3257b31c46f0),这篇文章我们重点分析让ES步入数据分析领域的Aggregation相关的功能和设计。

## 前言

我记得有一次到一家公司做内部分享，然后有研发问我，即席分析这块，他们用ES遇到一些问题。我当时直接就否了，我说ES还是个全文检索引擎，如果要做分析，还是应该用Impala,Phenix等这种主打分析的产品。随着ES的发展，我现在对它的看法，也有了比较大的变化。而且我认为ES+Spark SQL组合可以很好的增强即席分析能够处理的数据规模，并且能够实现复杂的逻辑，获得较好的易用性。

需要说明的是，我对这块现阶段的理解也还是比较浅。问题肯定有不少，欢迎指正。


## Aggregations的基础

Lucene 有三个比较核心的概念：

1. 倒排索引
2. fieldData/docValue
3. Collector

倒排索引不用我讲了，就是term -> doclist的映射。 

fieldData/docValue 你可以简单理解为列式存储,索引文件的所有文档的某个字段会被单独存储起来。 对于这块，Lucene 经历了两阶段的发展。第一阶段是fieldData ，查询时从倒排索引反向构成doc-term。这里面有两个问题：

* 数据需要全部加载到内存
* 第一次构建会很慢

这两个问题其实会衍生出很多问题：最严重的自然是内存问题。所以lucene后面搞了DocValue，在构建索引的时候就生成这个文件。DocValue可以充分利用操作系统的缓存功能，如果操作系统cache住了，则速度和内存访问是一样的。

另外就是Collector的概念，ES的各个Aggregator 实现都是基于Collector做的。我觉得你可以简单的理解为一个迭代器就好，所有的候选集都会调用`Collector.collect(doc)`方法，这里collect == iterate 可能会更容易理解些。

ES 能把聚合做快，得益于这两个数据结构，一个迭代器。我们大部分聚合功能，其实都是在fieldData/docValue 上工作的。

##  Aggregations 分类

Aggregations种类分为:

1. Metrics
2. Bucket

Metrics 是简单的对过滤出来的数据集进行avg,max等操作，是一个单一的数值。

Bucket 你则可以理解为将过滤出来的数据集按条件分成多个小数据集，然后Metrics会分别作用在这些小数据集上。

对于最后聚合出来的结果，其实我们还希望能进一步做处理，所以有了Pipline Aggregations,其实就是组合一堆的Aggregations 对已经聚合出来的结果再做处理。

## Aggregations 类设计

下面是一个聚合的例子：

```
{
	"aggregations": {
		"user": {
			"terms": {
				"field": "user",
				"size": 10,
				"order": {
					"_count": "desc"
				}
			}
		}
	}
}
```

其语义类似这个sql 语句： `select count(*) as user_count group by user order by user_count  desc`。

对于Aggregations 的解析，基本是顺着下面的路径分析：

```
TermsParser ->  
        TermsAggregatorFactory -> 
                  GlobalOrdinalsStringTermsAggregator
```

在实际的一次query里，要做如下几个阶段：

1. Query Phase 此时 会调用GlobalOrdinalsStringTermsAggregator的Collector 根据user 的不同进行计数。

2. RescorePhase

3. SuggestPhase

4.  AggregationPhase  在该阶段会会执行实际的aggregation build, `aggregator.buildAggregation(0)`，也就是一个特定Shard(分片)的聚合结果

5.  MergePhase。这一步是由接受到请求的ES来完成，具体负责执行Merge(Reduce)操作`SearchPhaseController.merge`。这一步因为会从不同的分片拿到数据再做Reduce,也是一个内存消耗点。所以很多人会专门搞出几台ES来做这个工作，其实就是ES的client模式，不存数据，只做接口响应。

在这里我们我们可以抽取出几个比较核心的概念：

1. AggregatorFactory (生成对应的Aggregator)
2. Aggregation (聚合的结果输出)
3. Aggregator (聚合逻辑实现)

另外值得注意的，PipeLine Aggregator 我前面提到了，其实是对已经生成的Aggregations重新做加工，这个工作是只能单机完成的，会放在请求的接收端执行。

## Aggregation Bucket的实现

前面的例子提到，在Query 阶段，其实就会调用Aggregator 的collect 方法，对所有符合查询条件的文档集都会计算一遍,这里我们涉及到几个对象：

1. doc id
2. field (docValue)
3. IntArray 对象

collect 过程中会得到 doc id,然后拿着docId 到 docValue里去拿到field的值(一般而言字符串也会被编码成Int类型的)，然后放到IntArray 进行计数。如果多个doc id 在某filed里的字段是相同的，则会递增计数。这样就实现了group by 的功能了。

## Spark-SQL 和 ES 的组合

我之前一直在想这个问题，后面看了下es-hadoop的文档，发现自己有些思路和现在es-hadoop的实现不谋而合。主要有几点：

1. Spark-SQL 的 where 语句全部(或者部分)下沉到 ES里进行执行，依赖于倒排索引，DocValues，以及分片,并行化执行，ES能够获得比Spark-SQL更优秀的响应时间
2. 其他部分包括分片数据Merge(Reduce操作，Spark 可以获得更好的性能和分布式能力)，更复杂的业务逻辑都交给Spark-SQL (此时数据规模已经小非常多了)，并且可以做各种自定义扩展，通过udf等函数
3. ES 无需实现Merge操作,可以减轻内存负担，提升并行Merge的效率(并且现阶段似乎ES的Reduce是只能在单个实例里完成)








