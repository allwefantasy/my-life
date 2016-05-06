> 在前文 [ElasticSearch Aggregations 分析](http://www.jianshu.com/p/56ad2b7e27b7) 中，我们提及了 【Aggregation Bucket的实现】，然而只是用文字简要描述了原理。今天这篇文章会以简单的类似grouyBy 的操作，让大家Aggregator的工作原理有进一步的理解


## 查询语句

今天我们假定的查询如下：

```
{	
	"aggs":{
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

其语义类似这个sql 语句： select count(*) as user_count group by user order by user_count  desc。 也就是按user 字段进行group by,接着进行降序排列。

## 调用链关系

首先

```
org.elasticsearch.search.aggregations.bucket.terms.TermsParser

```

会解析上面的JSON 查询串，然后构建出

```
org.elasticsearch.search.aggregations.bucket.terms.TermsAggregatorFactory
```

构建该Factory,大体需要下面一些信息：

1. 聚合名称
2. 需要统计的字段(ValueSourceConfig对象)
3. 排序(Order对象)
4. 其他一些terms聚合特有的配置

之后会构建出

```
org.elasticsearch.search.aggregations.bucket.terms.GlobalOrdinalsStringTermsAggregator
```

对象。这个是实现聚合的核心对象，当然就这个类而言仅仅是针对当前的例子适用的。 该类有两个核心方法：

1. getLeafCollector
2. buildAggregation

getLeafCollector 是获取Collector,还记得前文提及Collector 其实是一个迭代器，迭代所有文档，在这个步骤中，我们会获取一个Collector,然后依托于DocValues进行计数。

第二个方法，buildAggrgation方法则会将收集好的结果进行排序，获取topN。


## getLeafCollector

整个方法的代码我贴在了下面。在解析源码的过程中，我们会顺带解释ES对DocValues的封装。

```
@Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
                                                final LeafBucketCollector sub) throws IOException {

       //valuesSource类型为：ValuesSource.Bytes.WithOrdinals.FieldData
        globalOrds = valuesSource.globalOrdinalsValues(ctx);

       // 下面的两个if 做过滤，不是我们核心的点
        if (acceptedGlobalOrdinals == null && includeExclude != null) {
            acceptedGlobalOrdinals = includeExclude.acceptedGlobalOrdinals(globalOrds, valuesSource);
        }

        if (acceptedGlobalOrdinals != null) {
            globalOrds = new FilteredOrdinals(globalOrds, acceptedGlobalOrdinals);
        }
       // 返回新的collector
        return newCollector(globalOrds, sub);
    }
```

我在前面的源码里特别注释了下valuesSource 的类型。前文我们提到，大部分Aggregator 都是依赖于FieldData/DocValues 来实现的，而ValueSource 则是他们在ES里的表示。所以了解他们是很有必要的。ValuesSource 全类名是：

     org.elasticsearch.search.aggregations.support.ValuesSource

该类就是ES 为了管理 DocValues 而封装的。它是一个抽象类，内部还有很多实现类，Bytes,WithOrdinals,FieldData,Numeric,LongValues 等等。这些都是对特定类型DocValues类型的 ES 表示。

按上面我们的查询示例来看，`user` 字段对应的是

     org.elasticsearch.search.aggregations.support.ValuesSource.Bytes.WithOrdinals.FieldData

对象。这个对象是ES对Lucene String 类型的DocValues的一个表示。 
你会发现在ValueSource类里，有不同的FieldData。不同的FieldData 可能继承自不同基类从而表示不同类型的数据。在现在这个FieldData 里面有一个对象：

```
protected final IndexOrdinalsFieldData indexFieldData;
```

该对象在user(我们示例中的字段)是String类型的时候，对应的是实现类是

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

当个文件的很好理解，包含了索引中user字段所有的值，其坐标获取也很自然。

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

## buildAggregation

buildAggregation 代码我就不贴了，大体逻辑是：

1. 如果没有user字段，则直接返回。
2. 获取返回值的大小 TopN的N
3. 构建一个优先级队列，获取排序最高terms
4. 构建StringTerms 返回

这个逻辑也反应出了一个问题，TermsAggrator全局排序是不精准的。因为每个Shard 都只取TopN个，最后Merge(Reduce) 之后，不一定是全局的TopN。你可以通过size来控制精准度。

其实也从一个侧面验证了，在聚合结果较大的情况下，ES 还是有局限的。最好的方案是，ES算出所有的term count,然后通过iterator接入Spark,Spark 装载这些数据做最后的全局排序会比较好。

## 总结

我们以一个简单的例子对GlobalOrdinalsStringTermsAggregator 进行了分析，并且提及了ES对Lucene DocValues的封装的方式。
