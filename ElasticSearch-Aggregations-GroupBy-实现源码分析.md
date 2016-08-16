> 在前文 [ElasticSearch Aggregations 分析](http://www.jianshu.com/p/56ad2b7e27b7) 中，我们提及了 【Aggregation Bucket的实现】，然而只是用文字简要描述了原理。今天我们会举个实际groupBy的例子进行剖析，让大家对ElasticSearch Aggregations 的工作原理有更深入的理解

## 准备工作

* 为了方便调试，我对索引做了如下配置

```
{
  "mappings": {
    "my_type": {
      "properties": {
        "newtype": { 
          "type":       "string",
          "index":      "not_analyzed"
        },
        "num": { 
          "type":       "integer"
        }
      }
    }
  },
   "settings" : {
        "index" : {
            "number_of_shards" : 1, 
            "number_of_replicas" : 0 
        }
    }
}
```

这样只有一个分片，方便IDE的跟踪，也算是个看源码的技巧

*  数据

```
{
    "user" : "kimchy",
    "post_date" : "2009-11-15T14:12:12",
    "newtype": "abc",
    "message" : "trying out Elasticsearch",
    "num" : 10
}
```
## 查询语句

假定的查询如下：

```
{
	"from": 0,
	"size": 0,
	"_source": {
		"includes": [
			"AVG"
		],
		"excludes": []
	},
	"aggregations": {
		"newtype": {
			"terms": {
				"field": "newtype",
				"size": 200
			},
			"aggregations": {
				"AVG(num)": {
					"avg": {
						"field": "num"
					}
				}
			}
		}
	}
}
```

其语义类似这个sql 语句： 

```
SELECT avg(num) FROM twitter group by newtype
```

也就是按newtype 字段进行group by,然后对num求平均值。在我们实际的业务系统中，这种统计需求也是最多的。


## Phase概念

在查询过程中，ES是将整个查询分成几个阶段的，大体如下：

* QueryPhase
* rescorePhase
* suggestPhase
* aggregationPhase
* FetchPhase

对于全文检索，可能还有DFSPhase。

顺带提一点，Spark SQL + ES 的组合，最影响响应时间的地方其实是Fetch original source 。

而对于这些Phase,并不是一个链路的模式，而是在某个Phase调用另外一个Phase。这个在源码中也很明显，我们看如下一段代码：

```
      //创建聚合需要的AggregationContext,
     //里面包含了各个Aggregator
      aggregationPhase.preProcess(searchContext);

       //实际query,还有聚合操作其实是在这部完成的
        boolean rescore = execute(searchContext, searchContext.searcher());

        //如果是全文检索，并且需要打分
        if (rescore) { // only if we do a regular search
            rescorePhase.execute(searchContext);
        }
        suggestPhase.execute(searchContext);
        //获取聚合结果
        aggregationPhase.execute(searchContext);       
        }
```

## Aggregation的相关概念

要了解具体是如何实现聚合功能的，则需要了解ES 的aggregator相关的概念。大体有五个：

* AggregatorFactory （典型的工厂模式）负责创建Aggregator实例
* Aggregator (负责提供collector,并且提供具体聚合逻辑的类)
* Aggregations (聚合结果)
* PipelineAggregator (对聚合结果进一步处理)
* Aggregator 的嵌套，比如 示例中的AvgAggregator 就是根据GlobalOrdinalsStringTermsAggregator 的以bucket为维度，对相关数据进行操作.这种嵌套结构也是
* Bucket 其实就是被groupBy 字段的数字表示形式。用数字表示，可以节省对应字段列式存储的空间，并且提高性能。

## Aggregations 实现的机制

我们知道，无论检索亦或是聚合查询，本质上都需要转化到Lucene里的Collector，以上面的案例为例,其实由两个Collector 完成最后的计算：

* TotalHitCountCollecotr
* GlobalOrdinalsStringTermsAggregator(里面还有个Aggregator)

因为我们没有定义过滤条件，所以最后的Query 是个MatchAllQuery，之后基于这个基础上，这两个collector 完成对应的计算。通常，这两个Collector 会被wrap成一个新的MultiCollector ，最终传入IndexSearcher的Collector 就是MultiCollector。

根据上面的分析，我们知道示例中的聚合计算完全由GlobalOrdinalsStringTermsAggregator负责。

## 基于DocValues实现groupBy概览

对于每一个segment,我们都会为每个列单独存储成一个文件，为了压缩，我们可能会将里面具体的值转换成数字，然后再形成一个字典和数字对应关系的文件。我们进行所谓的groupBy操作，以最后进行Avg为例子，其实就是维护了两个大数组，

```
LongArray counts;//Long数组
DoubleArray sums; //Double 数组
```


counts是newtype(我们例子中被groupby的字段)次数统计，对应的数组下标是newtype(我们已经将newtype转化为数字表示了)。我们遍历文档的时候(MatchAllQuery)，可以获取doc,然后根据doc到列存文件获取对应的newtype,然后给counts 对应的newtype +1。 这样我们就知道每个newtype 出现的次数了。

这里我们也可以看到，消耗内存的地方取决于newtype的数量(distinct后)，我们称之为基数。基数过高的话，是比较消耗内存的。

sums 也是一样的，下标是newtype的值，而对应的值则是不断累加num(我们例子中需要被avg的字段)。

之后就可以遍历两个数组得到结果了，代码大体如下：

```
//这里的owningBucketOrd 就是newtype 的数字化表示
public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= sums.size()) {
            return Double.NaN;
        }
        return sums.get(owningBucketOrd) / counts.get(owningBucketOrd);
    }
```

## GlobalOrdinalsStringTermsAggregator/AvgAggregator组合实现

GlobalOrdinalsStringTermsAggregator 首先要提供一个Collector 给主流程，所以其提供了一个newCollector方法：

```
protected LeafBucketCollector newCollector(
//DocValue 列式存储的一个API表现
final RandomAccessOrds ords,
//AvgAggregator提供的Collector
final LeafBucketCollector sub)
```

接着判定是不是只有一个列文件(DocValues):

```
final SortedDocValues singleValues = DocValues.unwrapSingleton(words);
//如果singleValues!=null 则是一个，否则有多个列文件

```

如果是一个的话：

```
public void collect(int doc, long bucket) throws IOException {
                    assert bucket == 0;
                    final int ord = singleValues.getOrd(doc);
                    if (ord >= 0) {
                        collectExistingBucket(sub, doc, ord);
                    }
                }
//collectExistingBucket
 public final void collectExistingBucket(LeafBucketCollector subCollector, int doc, long bucketOrd) throws IOException {
        docCounts.increment(bucketOrd, 1);
        subCollector.collect(doc, bucketOrd);
    }

```

通过doc 拿到ord(newtype),然后交给Avg的collector 接着处理,进入AvgAggregator 里的Collector的collect逻辑：

```
public void collect(int doc, long bucket) throws IOException {
                counts = bigArrays.grow(counts, bucket + 1);
                sums = bigArrays.grow(sums, bucket + 1);

                values.setDocument(doc);
                final int valueCount = values.count();
                counts.increment(bucket, valueCount);
                double sum = 0;
                for (int i = 0; i < valueCount; i++) {
                    sum += values.valueAt(i);
                }
                sums.increment(bucket, sum);
            }
```

这个和我上面的概述中描述是一致的。

如果是多个DocValues(此时索引还没有对那些Segment做合并)，这个时候会走下面的流程：

```
public void collect(int doc, long bucket) throws IOException {
                    assert bucket == 0;
                    ords.setDocument(doc);
                    final int numOrds = ords.cardinality();
                    for (int i = 0; i < numOrds; i++) {
                        final long globalOrd = ords.ordAt(i);
                        collectExistingBucket(sub, doc, globalOrd);
                    }
                }
```
这里的ords 包括了多个DocValues文件,然后做了全局映射，因为要把文件的下标做映射。为啥要有下标映射呢？因为多个列文件(DocValues)的全集才具有完整的newtype，但是每个列文件都是从0开始递增的。现在要扩张到一个global的空间上。 ords.cardinality()
拿到了列文件(DocValues)的数目，然后对每个文件都处理一遍，通过ords.ordAt(i) 拿到newtype的全局下标，这个时候就可以继续交给Avg完成了。

到这个阶段，我们其实已经算好了每个newtype 出现的次数，以及num的累计值，也就是我们前面提到的两个数组。

## BuildAggregation

最终我们是要把这个数据输出输出的，不论是输出给别的ES节点，还是直接输出给调用方。所以有个BuildAggregation的过程，可以根据名字进行直观的了解。

考虑到内存问题，ES允许你设置一些Threshhold,然后通过BucketPriorityQueue(优先队列)来完成实际的数据收集以及排序(默认按文档出现次数排序)。 里面的元素是OrdBucket，OrdBucket包含了几个值：

```
globalOrd： 全局下标
bucketOrd： 在所属文件里的下标
docCount : 文档出现的次数
```

接着取出 topN 的对象，放到InternalTerms.Bucket[] 数组中。然后遍历该数组，调用子Aggregator的buildAggregation方法，这里的子Aggregator是AvgAggregator ,每个Bucket(newtype)就获取到一个avg aggregations了，该aggregations通过InternalAggregations 包裹，InternalAggregations 包含了一个reduce 方法，该方法会调用具体InternalAggregation的doReduce 方法，比如AvgAggregator就有自己的reduce方法。说这个主要给下一小结做铺垫。

最后会被包装成StringTerms ,然后就可以序列化成JSON格式，基本就是你在接口上看到的样子了。

## 多分片聚合结果合并

前面我们讨论的，都是基于一个分片，但是最终是要把结果数据进行Merge的。 这个功能是由SearchPhaseController 对象来完成，大体如下：

```
sortedShardList = searchPhaseController.sortDocs(useScroll, firstResults);

final InternalSearchResponse internalResponse = searchPhaseController.merge(sortedShardList, firstResults,
                    firstResults, request);
```
其中merge 动作是按分类进行merge的，比如：

* counter(计数,譬如total_hits)
* hits
* aggregations
* suggest
* profile （性能相关的数据）

这里我们只关注aggregations的merge

```
 // merge addAggregation
        InternalAggregations aggregations = null;
        if (!queryResults.isEmpty()) {
            if (firstResult.aggregations() != null && firstResult.aggregations().asList() != null) {
                List<InternalAggregations> aggregationsList = new ArrayList<>(queryResults.size());
                for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
                    aggregationsList.add((InternalAggregations) entry.value.queryResult().aggregations());
                }
                aggregations = InternalAggregations.reduce(aggregationsList, new ReduceContext(bigArrays, scriptService, headersContext));
            }
        }

```

代码有点长，核心是

```
InternalAggregations.reduce(.....)
```

里面实际的逻辑也是比较简单直观的。会调用InternalTerms的reduce方法做merge,但是不同的类型的Aggregator产生Aggregations 合并逻辑是不一样的，所以会委托给对应实现。比如GlobalOrdinalsStringTermsAggregator则会委托给InternalTerms的doReduce方法，而如AvgAggregator
会委托给InternalAvg的doReduce。 这里就不展开。未来会单独出一片文章讲解。


## 附录

这里我们再额外讲讲ValueSource (ES 对FieldData/DocValues的抽象)。

前文我们提到，大部分Aggregator 都是依赖于FieldData/DocValues 来实现的，而ValueSource 则是他们在ES里的表示。所以了解他们是很有必要的。ValuesSource 全类名是：

     org.elasticsearch.search.aggregations.support.ValuesSource

该类就是ES 为了管理 DocValues 而封装的。它是一个抽象类，内部还有很多实现类，Bytes,WithOrdinals,FieldData,Numeric,LongValues 等等。这些都是对特定类型DocValues类型的 ES 表示。

按上面我们的查询示例来看，`newtype` 字段对应的是

     org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals.FieldData

对象。这个对象是ES对Lucene String 类型的DocValues的一个表示。 
你会发现在ValueSource类里，有不同的FieldData。不同的FieldData 可能继承自不同基类从而表示不同类型的数据。在现在这个FieldData 里面有一个对象：

```
protected final IndexOrdinalsFieldData indexFieldData;
```

该对象在newtype(我们示例中的字段)是String类型的时候，对应的是实现类是

```
org.elasticsearch.index.fielddata.plain.SortedSetDVOrdinalsIndexFieldData
```

该对象的大体作用是，构建出DocValue的ES的Wraper。

具体代码如下：

```
@Overridepublic AtomicOrdinalsFieldData load(LeafReaderContext context) {    
return new SortedSetDVBytesAtomicFieldData(
   context.reader(),
   fieldNames.indexName());
}
//或者通过loadGlobal方法得到
//org.elasticsearch.index.fielddata.ordinals.InternalGlobalOrdinalsIndexFieldData  
```

以第一种情况为例，上面的代码new 了一个新的`org.elasticsearch.index.fielddata.AtomicOrdinalsFieldData`对象,该对象的一个实现类是`SortedSetDVBytesAtomicFieldData `。 这个对象和Lucene的DocValues 完成最后的对接：

```
 @Override
    public RandomAccessOrds getOrdinalsValues() {
        try {
            return FieldData.maybeSlowRandomAccessOrds(DocValues.getSortedSet(reader, field));
        } catch (IOException e) {
            throw new IllegalStateException("cannot load docvalues", e);
        }
    }
```
我们看到，通过Reader获取到最后的列就是在该类里的getOrdinalsValues 方法里实现的。

该方法最后返回的RandomAccessOrds 就是Lucene的DocValues实现了。

分析了这么多，所有的逻辑就浓缩在`getLeafCollector `的第一行代码上。globalOrds  的类型是RandomAccessOrds，并且是直接和Lucene对应上了。 

```
globalOrds = valuesSource.globalOrdinalsValues(cox);
```

getLeafCollector 最后newCollector的规则如下：

```
 protected LeafBucketCollector newCollector(final RandomAccessOrds ords, final LeafBucketCollector sub) {
        grow(ords.getValueCount());
        final SortedDocValues singleValues = DocValues.unwrapSingleton(ords);
        if (singleValues != null) {
            return new LeafBucketCollectorBase(sub, ords) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    assert bucket == 0;
                    final int ord = singleValues.getOrd(doc);
                    if (ord >= 0) {
                        collectExistingBucket(sub, doc, ord);
                    }
                }
            };
        }
```

我们知道，在Lucene里，大部分文件都是不可更新的。一个段一旦生成后就是不可变的，新的数据或者删除数据都需要生成新的段。DocValues的存储文件也是类似的。所以DocValues.unwrapSingleton其实就是做这个判定的，是不是有多个文件 。无论是否则不是都直接创建了一个匿名的Collector。

当个文件的很好理解，包含了索引中newtype字段所有的值，其下标获取也很自然。

```
//singleValues其实就是前面的RandomAccessOrds。
final int ord = singleValues.getOrd(doc);
```

根据文档号获取值对应的位置，如果ord >=0 则代表有值，否则代表没有值。

如果有多个文件，则会返回如下的Collecor:

```
else {
            return new LeafBucketCollectorBase(sub, ords) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    assert bucket == 0;
                    ords.setDocument(doc);
                    final int numOrds = ords.cardinality();
                    for (int i = 0; i < numOrds; i++) {
                        final long globalOrd = ords.ordAt(i);
                        collectExistingBucket(sub, doc, globalOrd);
                    }
                }
            };
```

上面的代码可以保证多个文件最终合起来保持一个文件的序号。什么意思呢？比如A文件有一个文档，B文件有一个，那么最终获取的globalOrd 就是0,1 而不会都是0。此时的 ords 实现类 不是SingletonSortedSetDocValues 而是
```
org.elasticsearch.index.fielddata.ordinals.GlobalOrdinalMapping
```

对象了。

计数的方式两个都大体类似。
```
docCounts.increment(bucketOrd, 1);
```

这里的bucketOrd 其实就是前面的ord/globalOrd。所以整个计算就是填充docCounts

## 总结

ES的 Aggregation机制还是挺复杂的。本文试图通过一个简单的group by 的例子来完成对其机制的解释。其中ValueSource 那层我目前也没没完全吃透，如有表述不合适的地方，欢迎大家指出。
