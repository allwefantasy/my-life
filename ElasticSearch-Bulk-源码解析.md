> 本来应该先有这篇文章,后有[如何提高ElasticSearch 索引速度](http://www.jianshu.com/p/5eeeeb4375d4)才对。不过当时觉得后面一篇文章会更有实际意义一些，所以先写了后面那篇文章。结果现在这篇文章晚了20多天。

## 前言

读这篇文章前，建议先看看[ElasticSearch Rest/RPC 接口解析](http://www.jianshu.com/p/3257b31c46f0),有利于你把握ElasticSearch接受处理请求的脉络。对于RPC类的调用，我会在后文简单提及，只是endpoint不一样，内部处理逻辑还是一样的。这篇只会讲IndexRequest,其他如DeleteRequest,UpdateRequest之类的，我们暂时不涉及。

## 类处理路径

```
RestBulkAction -> 
            TransportBulkAction -> 
                       TransportShardBulkAction
```

其中`TransportShardBulkAction`比较特殊，有个继承结构：

``` 
   TransportShardBulkAction < TransportReplicationAction < TransportAction
```

主入口是TransportAction，具体的业务逻辑实现分布到子类(TransportReplicationAction)和孙子类(TransportShardBulkAction)里了。

另外，我们也会提及`org.elasticsearch.index.engine.Engine`相关的东西，从而让大家清楚的了解ES是如何和Lucene关联上的。

## RestBulkAction

入口自然是`org.elasticsearch.rest.action.bulk.RestBulkAction`,一个请求会构建一个`BulkRequest`对象,`BulkRequest.add`方法会解析你提交的文本。对于类型为`index`或者`create`的(还记得bulk提交的文本格式是啥样子的么？)，都会被构建出`IndexRequest`对象，这些解析后的对象会被放到BulkRequest对象的属性`requests`里。当然如果是`update`,`delete`等则会构建出其他对象，但都会放到`requests`里。

```
public class BulkRequest extends ActionRequest<BulkRequest> implements CompositeIndicesRequest {
    //这个就是前面提到的requests
    final List<ActionRequest> requests = new ArrayList<>();  

//这个复杂的方法就是通过http请求参数解析出
//IndexRequest,DeleteRequest,UpdateRequest等然后放到requests里
public BulkRequest add(BytesReference data, 
@Nullable String defaultIndex, 
@Nullable String defaultType, 
@Nullable String defaultRouting, 
@Nullable String[] defaultFields, 
@Nullable Object payload, boolean allowExplicitIndex) throws Exception {
        XContent xContent = XContentFactory.xContent(data);
        int line = 0;
        int from = 0;
        int length = data.length();
        byte marker = xContent.streamSeparator();
        while (true) {
```


接着通过NodeClient将请求发送到TransportBulkAction类（回忆下之前文章里提到的映射关系，譬如 *** Transport*Action，两层映射关系解析 ** ）。对应的方法如下：


```
//这里的client其实是NodeClient
client.bulk(bulkRequest, new RestBuilderListener<BulkResponse>(channel) {
```

## TransportBulkAction

看这个类的签名：

```
public class TransportBulkAction extends HandledTransportAction<BulkRequest, BulkResponse> {
```

实现了`HandledTransportAction`，说明这个类同时也是RPC接口的逻辑处理类。如果你点进`HandledTransportAction`就能看到ES里经典的`messageReceived`方法了。这个是题外话

该类对应的入口是:

```
protected void doExecute(final BulkRequest bulkRequest, final ActionListener<BulkResponse> listener) {
```
这里的`bulkRequest` 就是前面`RestBulkAction`组装好的。该方法第一步是判断是不是需要自动建索引，如果索引不存在，就自动创建了。

接着通过`executeBulk`方法进入原来的流程。在该方法中，对`bulkRequest.requests` 进行了两次for循环。

第一次判定如果是`IndexRequest`就调用`IndexRequest.process`方法，主要是为了解析出`timestamp`,`routing`,`id`,`parent` 等字段。

第二次是为了对数据进行分拣。大致是为了形成这么一种结构：

```
//这里的BulkItemRequest来源于 IndexRequest等
Map[ShardId, List[BulkItemRequest]]
```

接着对新形成的这个结构(ShardId -> List[BulkItemRequest])做循环，也就是针对每个ShardId里的数据进行统一处理。有了`ShardId`,`bulkRequest`,`List[BulkItemRequest]`等信息后，统一封装成`BulkShardRequest`。从名字看就很好理解，就是对属于同一`ShardId`的数据构建一个新的类似BulkRequest的对象。

接着就到`TransportShardBulkAction`,`TransportReplicationAction`,`TransportAction` 三代人出场了：

```
//这里的shardBulkAction 是TransportShardBulkAction
shardBulkAction.execute(bulkShardRequest, new ActionListener<BulkShardResponse>() {
```

## TransportReplicationAction/TransportShardBulkAction

`TransportAction `是一个通用的主类，具体逻辑还是其子类来实现。虽然前面提到`shardBulkAction `是`TransportShardBulkAction `,但其实流程逻辑还是`TransportReplicationAction`来完成的。入口在该类的doExecute方法:

```
@Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new PrimaryPhase(request, listener).run();
    }
```

我们知道在ES里有主从分片的概念，所以一条数据被索引后需要经过两个阶段：

1. 将数据写入Primary(主分片)
2. 将数据写入Replication(从分片)

至于为什么不直接从Primary进行复制，而是将数据分别写入到Primary和Replication我觉得主要考虑如果一旦Primary是损坏的，不至于影响到Replication（考虑下，如果Primary是损坏的文件，然后所有的Replication如果是直接复制过来，就都坏了）。

又扯远了。我们看到`doExecute` 首先是进入PrimaryPhase阶段，也就是写主分片。

## Primary Phase

在`PrimaryPhase.doRun`方法里，你会看到两行代码：

```
final ShardIterator shardIt = shards(observer.observedState(), internalRequest);
final ShardRouting primary = resolvePrimary(shardIt);
```

其中这个ShardIterator是类似 `shardId->ShardGroup` 的结构。不管这个shardId是什么，它一定是个Replication或者Primary的shardId, ShardGroup 就是Replication和Primary的集合。`resolvePrimary`方法则是遍历这个集合，然后找出Primary的过程。

知道Primary后就可以判断是转发到别的Node或者直接在本Node处理了：

```
routeRequestOrPerformLocally(primary, shardIt);
```

如果Primary就在本节点，直接就处理了：

```
//我去掉了一些无关代码哈
if (primary.currentNodeId().equals(observer.observedState().nodes().localNodeId())) {
                try {
                    threadPool.executor(executor).execute(new AbstractRunnable() {
                         @Override
                        protected void doRun() throws Exception {
                            performOnPrimary(primary, shardsIt);
                        }
            }
```

这里用上了线程池。前面对每个shardId对应的数据集合做处理，其实是顺序循环执行的，这里实现了将数据处理异步化。

在`performOnPrimary`方法中，`BulkShardRequest`被转化成了`PrimaryOperationRequest`,理由也很简单，更加specific了，因为就是针对主分片的Request。接着进入`shardOperationOnPrimary ` 方法,该方法是在孙子类`TransportShardBulkAction`类里实现的。

```
protected Tuple<BulkShardResponse, BulkShardRequest> shardOperationOnPrimary(
ClusterState clusterState, 
PrimaryOperationRequest shardRequest) {
```

到该方法，有两个比较重要的概念会出现：

```
//伟大的版本号，实现了对并发修改的支持
long[] preVersions = new long[request.items().length];
VersionType[] preVersionTypes = new VersionType[request.items().length];
//事物日志，为Shard Recovery以及
//避免过多的Index Commit做出突出贡献，
//同时也是是实现了GetById的实时性
Translog.Location location = null;
```

上面两个概念成就了ES从一个简单的全文检索引擎到类No-SQL的转型(好吧，我好像又扯远了)

接着就是for循环了：
```
//这里的request是BulkShardRequest
//对应的items则是BulkItemRequest集合
for (int requestIndex = 0;
 requestIndex < request.items().length; 
requestIndex++) {
```
循环会根据`BulkItemRequest`的不同类型而有了分支。其实就是`IndexRequest`,`DeleteRequest`,`UpdateRequest`,我们这里依然只讨论`IndexRequest`。如果发现`BulkItemRequest`是`IndexRequest `,进行如下操作：

```
WriteResult<IndexResponse> result = shardIndexOperation(request, 
indexRequest, 
clusterState, 
indexShard, 
true);
```

`shardIndexOperation `里嵌套的核心方法是`executeIndexRequestOnPrimary `,该方法第一步是获取到`Operation`对象,

```
Engine.IndexingOperation operation = prepareIndexOperationOnPrimary(shardRequest, request, indexShard);
```

Engine对象是比较底层的一个对象了，是对Lucene的`IndexWriter`，`Searcher`之类的封装。这里的`Engine.IndexingOperation `对应的是`Create`或者`Index`类。你可以把这两个类理解为待索引的Document,只是还带上了动作。

第二步是判断索引的Mapping是不是要动态更新，如果是，则更新。

第三步执行实际的建索引操作：

```
final boolean created = operation.execute(indexShard);
```

### operation.execute 额外引出的话题

我们会暂时深入到operate.execute方法里，但这个不是主线，看完后记得回到上面那行代码上。

刚才我们说了operation可能是`Create`或者`Index`,我们会以`Create`为主线进行分析。所谓`Create`和`Index`，你可以理解为一个待索引的Document,只是带上动作的语义。

上面对应的execute 方法签名是：

```
@Overridepublic boolean execute(IndexShard shard) {     shard.create(this);   
 return true;
}
```

我们看到这里是反向调用indexShard对象的create方法来进行索引的创建。我们来看看`IndexShard`的`create`方法：

```
//我依然做了删减,体现一些核心代码
public void create(Engine.Create create) {        
        engine().create(create);
    }
```

`engine()`方法返回的是`InternalEngine`实例，`InternalEngine .innerCreate`方法执行到构建索引的操作。这个方法值得分析一下，所以我就贴了一坨的代码。

```
private void innerCreate(Create create) throws IOException {
        if (engineConfig.isOptimizeAutoGenerateId() && create.autoGeneratedId() && !create.canHaveDuplicates()) {
            // We don't need to lock because this ID cannot be concurrently updated:
            innerCreateNoLock(create, Versions.NOT_FOUND, null);
        } else {
            synchronized (dirtyLock(create.uid())) {
                final long currentVersion;
                final VersionValue versionValue;
                versionValue = versionMap.getUnderLock(create.uid().bytes());
                if (versionValue == null) {
                    currentVersion = loadCurrentVersionFromIndex(create.uid());
                } else {
                    if (engineConfig.isEnableGcDeletes() && versionValue.delete() && (engineConfig.getThreadPool().estimatedTimeInMillis() - versionValue.time()) > engineConfig.getGcDeletesInMillis()) {
                        currentVersion = Versions.NOT_FOUND; // deleted, and GC
                    } else {
                        currentVersion = versionValue.version();
                    }
                }
                innerCreateNoLock(create, currentVersion, versionValue);
            }
        }
    }
```

首先，如果满足如下三个条件就无需进行版本检查：

1. index.optimize_auto_generated_id 被设置为true(默认是false,话说注释上说是默认是true,但是我看着觉得像是false)
2. id设置为自动生成(没有人工设置id)
3. create.canHaveDuplicates == false ，该参数一般是false

提这个是主要为了说明，譬如一般的运维日志啥的，就不要自己生成ID了，采用自动生成的ID,可以跳过版本检查，从而提高入库的效率。

第二个指的说的是，如果对应文档在缓存中没有找到(versionMap),那么就会由如下的代码执行实际磁盘查询操作：

```
currentVersion = loadCurrentVersionFromIndex(create.uid());
```

通过对比create对象里的版本号和从索引文件里加载的版本号 ，最终决定是进行update还是create操作。

在`innerCreateNoLock ` 方法里，你会看到熟悉的Lucene操作，譬如：

```
indexWriter.addDocument(index.docs().get(0));
//或者
indexWriter.updateDocument(index.uid(), index.docs().get(0));
```

现在回到`TransportShardBulkAction`的主线上。执行完下面的代码后：

```
final boolean created = operation.execute(indexShard);
```

就能获得对应文档的版本等信息，这些信息会更新对应的IndexRequest等对象。

到目前为止，Primay Phase 完成,接着开始Replication Phase

```
replicationPhase = new ReplicationPhase(shardsIt, 
primaryResponse.v2(), 
primaryResponse.v1(), 
observer, 
primary, 
internalRequest, 
listener, 
indexShardReference);
finishAndMoveToReplication(replicationPhase);
```

最后一行代码会启动`replicationPhase `阶段。

## Replication Phase

Replication Phase 流程大致和Primary Phase 相同,就不做过详细的解决，我这里简单提及一下。

`ReplicationPhase`的`doRun`方法是入口，核心方法是`performOnReplica`,如果发现Replication  shardId所属的节点就是自己的话，异步执行`shardOperationOnReplica`，大体逻辑如下：

```
threadPool.executor(executor).execute(new AbstractRunnable() {
                        @Override
                        protected void doRun() {
                            try {
                                shardOperationOnReplica(shard.shardId(), replicaRequest);
                                onReplicaSuccess();
                            } catch (Throwable e) {
                                onReplicaFailure(nodeId, e);
                                failReplicaIfNeeded(shard.index(), shard.id(), e);
                            }
                        }

```

在Replication阶段，`shardOperationOnReplica` 该方法完成了索引内容解析，mapping动态新增，最后进入索引(和就是前面提到的operation.execute)等动作，所以还是比Primary 阶段更紧凑些。


另外，在Primary Phase 和 Replication Phase, 一个BulkShardRequest 处理完成后(也就是一个Shard 对应的数据集合)才会刷写Translog日志。所以如果发生数据丢失，则可能是多条数据。

## 总结

这篇文章以流程分析为主，很多细节我们依然没有讲解详细，比如Translog和Version。这些争取能够在后续文章中进一步阐述。另外错误之处在所难免，请大家在评论处提出。
