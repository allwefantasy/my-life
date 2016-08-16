> 上周出现了一次故障，recovery的过程比较慢，然后发现Shard 在做恢复的过程一般都是卡在TRANSLOG阶段，所以好奇这块是怎么完成的，于是有了这篇文章

这是一篇源码分析类的文章，大家需要先建立一个整体的概念，建议参看[这篇文章](http://kibana.logstash.es/content/elasticsearch/principle/realtime.html)

另外你可能还需要了解下 Recovery 阶段迁移过程：

> INIT ->  INDEX -> VERIFY_INDEX -> TRANSLOG -> FINALIZE -> DONE

## 概览

Recovery 其实有两种：

1. Primary的迁移/Replication的生成和迁移 
2. Primary的恢复

org.elasticsearch.indices.cluster.IndicesClusterStateService.clusterChanged 被触发后，会触发applyNewOrUpdatedShards 函数的调用，这里是我们整个分析的起点。大家可以跑进去看看，然后跟着文章打开对应的源码浏览。

阅读完这篇文章，我们能够得到：

1. 熟悉整个recovery 流程
2. 了解translog机制
3. 掌握对应的代码体系结构

## Primary的恢复

这个是一般出现故障集群重启的时候可能遇到的。首先需要从Store里进行恢复。

```
if (isPeerRecovery(shardRouting)) {
   ......
}
else {
  //走的这个分支
  indexService.shard(shardId).recoverFromStore(shardRouting, 
  new StoreRecoveryService.RecoveryListener() {
}
```

Primary 进行自我恢复，所以并不需要其他节点的支持。所以判定的函数叫做`isPeerRecovery` 其实还是挺合适的。

indexService.shard(shardId).recoverFromStore  调用的是 org.elasticsearch.index.shard.IndexShard的方法。

```
  public void recoverFromStore(ShardRouting shard, StoreRecoveryService.RecoveryListener recoveryListener) {
    ......
    final boolean shouldExist = shard.allocatedPostIndexCreate();
    storeRecoveryService.recover(this, shouldExist, recoveryListener);
    }
```

逻辑还是很清晰的，判断分片必须存在，接着将任务委托给 org.elasticsearch.index.shard.StoreRecoveryService.recover 方法，该方法有个细节需要了解下：

```
 if (indexShard.routingEntry().restoreSource() != null) {
       indexShard.recovering("from snapshot", 
           RecoveryState.Type.SNAPSHOT, 
           indexShard.routingEntry().restoreSource());
            } else {
       indexShard.recovering("from store", 
           RecoveryState.Type.STORE, 
           clusterService.localNode());
            }
```

ES会根据restoreSource 决定是从SNAPSHOT或者从Store里进行恢复。这里的`indexShard.recovering`并没有执行真正的recovering 操作，而是返回了一个recover的信息对象，里面包含了譬如节点之类的信息。

之后就将其作为一个任务提交出去了：

```
threadPool.generic().execute(new Runnable() {
            @Override
  public void run() {
  try {
  final RecoveryState recoveryState = indexShard.recoveryState();
  if (indexShard.routingEntry().restoreSource() != null) {
     restore(indexShard, recoveryState);
     } else {              
     recoverFromStore(indexShard, indexShouldExists, recoveryState);
    }
```
这里我们只走一条线，也就是进入 recoverFromStore 方法，该方法会执行索引文件的恢复动作，本质上是进入了`INDEX` Stage.

接着进行TranslogRecovery了

```
typesToUpdate = indexShard.performTranslogRecovery(indexShouldExists);
indexShard.finalizeRecovery();
```

继续进入 indexShard.performTranslogRecovery 方法：

```
  public Map<String, Mapping> performTranslogRecovery(boolean indexExists) {
        if (indexExists == false) {            
            final RecoveryState.Translog translogStats = recoveryState().getTranslog();
            translogStats.totalOperations(0);
            translogStats.totalOperationsOnStart(0);
        }
        final Map<String, Mapping> recoveredTypes = internalPerformTranslogRecovery(false, indexExists);     
        return recoveredTypes;
    }
```

这个方法里面，最核心的是 internalPerformTranslogRecovery方法，进入该方法后先进入 `VERIFY_INDEX` Stage,进行索引的校验，校验如果没有问题，就会进入我们期待的`TRANSLOG` 状态了。

进入`TRANSLOG` 后，先进行一些设置：

```
engineConfig.setEnableGcDeletes(false);
engineConfig.setCreate(indexExists == false);
```
这里的GC 指的是tranlog日志的删除问题，也就是不允许删除translog，接着会创建一个新的InternalEngine了，然后返回调用org.elasticsearch.index.shard.TranslogRecoveryPerformer.getRecoveredTypes

不过你看这个代码会比较疑惑，其实我一开始看也觉得纳闷：

```
  if (skipTranslogRecovery == false) {            
       markLastWrite();
        }
 createNewEngine(skipTranslogRecovery, engineConfig);
 return  engineConfig.getTranslogRecoveryPerformer().
                                  getRecoveredTypes();
```

我们并没有看到做translog replay的地方，而从上层的调用方来看：

```
typesToUpdate = indexShard.performTranslogRecovery(indexShouldExists);
indexShard.finalizeRecovery();
```

performTranslogRecovery 返回后，就立马进入扫尾(finalizeRecovery)阶段。 里面唯一的动作是createNewEngine，并且传递了`skipTranslogRecovery ` 参数。 也就说，真正的translog replay动作是在createNewEngine里完成，我们经过探索，发现是在InternalEngine 的初始化过程完成的，具体代码如下：

```
try {
      if (skipInitialTranslogRecovery) {
          commitIndexWriter(writer,
                                       translog, 
                                       lastCommittedSegmentInfos.
                                       getUserData().
                                       get(SYNC_COMMIT_ID));
                } else {
                    recoverFromTranslog(engineConfig, translogGeneration);
                }
            } catch (IOException | EngineException ex) {
              .......
            }
```

里面有个recoverFromTranslog，我们进去瞅瞅：

```
   final TranslogRecoveryPerformer handler = engineConfig.getTranslogRecoveryPerformer();
        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            opsRecovered = handler.recoveryFromSnapshot(this, snapshot);
        } catch (Throwable e) {
            throw new EngineException(shardId, "failed to recover from translog", e);
        }
```

目前来看，所有的Translog recovery 动作其实都是由 TranslogRecoveryPerformer 来完成的。当然这个名字也比较好，翻译过来就是 TranslogRecovery 执行者。先对translog 做一个snapshot,然后根据这个snapshot开始进行恢复，进入 recoveryFromSnapshot 方法我们查看细节，然后会引导你进入
下面的方法：

```
 public void performRecoveryOperation(Engine engine, Translog.Operation operation, boolean allowMappingUpdates) {
        try {
            switch (operation.opType()) {
                case CREATE:
                    Translog.Create create = (Translog.Create) operation;
                    Engine.Create engineCreate = IndexShard.prepareCreate(docMapper(create.type()),
                            source(create.source()).index(shardId.getIndex()).type(create.type()).id(create.id())
                                    .routing(create.routing()).parent(create.parent()).timestamp(create.timestamp()).ttl(create.ttl()),
                            create.version(), create.versionType().versionTypeForReplicationAndRecovery(), Engine.Operation.Origin.RECOVERY, true, false);
                    maybeAddMappingUpdate(engineCreate.type(), engineCreate.parsedDoc().dynamicMappingsUpdate(), engineCreate.id(), allowMappingUpdates);
                    if (logger.isTraceEnabled()) {
                        logger.trace("[translog] recover [create] op of [{}][{}]", create.type(), create.id());
                    }
                    engine.create(engineCreate);
                    break;
```

终于看到了实际的translog replay 逻辑了。这里调用了标准的InternalEngine.create 等方法进行日志的恢复。其实比较有意思的是，我们在日志回放的过程中，依然会继续写translog。这里就会导致一个问题，如果我在做日志回放的过程中，服务器由当掉了(或者ES instance 重启了)，那么就会导致translog 变多了。这个地方是否可以再优化下？

假设我们完成了Translog 回放后，如果确实有重放，那么就行flush动作，删除translog,否则就commit Index。具体逻辑由如下的代码来完成：
```
if (opsRecovered > 0) {
     opsRecovered, translogGeneration == null ? null : translogGeneration.translogFileGeneration, translog
                            .currentFileGeneration());
            flush(true, true);
        } else if (translog.isCurrent(translogGeneration) == false) {
            commitIndexWriter(indexWriter, translog, lastCommittedSegmentInfos.getUserData().get(Engine.SYNC_COMMIT_ID));
        }
```

接着就进入了finalizeRecovery，然后，就没然后了。

```
 indexShard.finalizeRecovery();
            String indexName = indexShard.shardId().index().name();
            for (Map.Entry<String, Mapping> entry : typesToUpdate.entrySet()) {
                validateMappingUpdate(indexName, entry.getKey(), entry.getValue());
            }
            indexShard.postRecovery("post recovery from shard_store");
```

## Primary的迁移/Replication的生成和迁移 

一般这种recovery其实就是发生relocation或者调整副本的时候发生的。所以集群是在正常状态，一定有健康的primary shard存在，所以我们也把这种recovery叫做Peer Recovery。  入口和前面的Primary恢复是一样的，代码如下：

```
if (isPeerRecovery(shardRouting)) {
 //走的这个分支
.....
RecoveryState.Type type = shardRouting.primary() ? RecoveryState.Type.RELOCATION : RecoveryState.Type.REPLICA;
                recoveryTarget.startRecovery(indexShard, type, sourceNode, new PeerRecoveryListener(shardRouting, indexService, indexMetaData));
......           
}
else {
 ......
}
```

核心代码自然是 recoveryTarget.startRecovery。这里的recoveryTarget的类型是： org.elasticsearch.indices.recovery.RecoveryTarget

startRecovery方法的核心代码是：

```
threadPool.generic().execute(new RecoveryRunner(recoveryId));
```

也是启动一个县城异步执行的。RecoveryRunner调用的是RecoveryTarget的 `doRecovery`方法，在该方法里，会发出一个RPC请求：

```
final StartRecoveryRequest request = new StartRecoveryRequest(recoveryStatus.shardId(), recoveryStatus.sourceNode(), clusterService.localNode(),        false, metadataSnapshot, recoveryStatus.state().getType(), recoveryStatus.recoveryId());

recoveryStatus.indexShard().prepareForIndexRecovery();
            recoveryStatus.CancellableThreads().execute(new CancellableThreads.Interruptable() {
                @Override
                public void run() throws InterruptedException {
                    responseHolder.set(transportService.submitRequest(request.sourceNode(), RecoverySource.Actions.START_RECOVERY, request, new FutureTransportResponseHandler<RecoveryResponse>() {
                        @Override
                        public RecoveryResponse newInstance() {
                            return new RecoveryResponse();
                        }
                    }).txGet());
                }
            });
```

这个时候进入 INDEX Stage。 那谁接受处理的呢？ 我们先看看现在的类名叫啥？ RecoveryTarget。 我们想当然的想，是不是有RecoverySource呢？ 发现确实有，而且该类确实也有一个处理类：

```
 class StartRecoveryTransportRequestHandler extends TransportRequestHandler<StartRecoveryRequest> {
        @Override
        public void messageReceived(final StartRecoveryRequest request, final TransportChannel channel) throws Exception {
            RecoveryResponse response = recover(request);
            channel.sendResponse(response);
        }
    }
```

ES里这种通过Netty进行交互的方式，大家可以看看我之前写文章[ElasticSearch Rest/RPC 接口解析](http://www.jianshu.com/p/3257b31c46f0)。 

这里我们进入RecoverSource对象的recover方法：

```
 private RecoveryResponse recover(final StartRecoveryRequest request) {
      .....
      if (IndexMetaData.isOnSharedFilesystem(shard.indexSettings())) {
            handler = new SharedFSRecoverySourceHandler(shard, request, recoverySettings, transportService, logger);
        } else {
            handler = new RecoverySourceHandler(shard, request, recoverySettings, transportService, logger);
        }
        ongoingRecoveries.add(shard, handler);
        try {
            return handler.recoverToTarget();
        } finally {
            ongoingRecoveries.remove(shard, handler);
        }
 }
```
我们看到具体负责处理的类是RecoverySourceHandler，之后调用该类的recoverToTarget方法。我对下面的代码做了精简，方便大家看清楚。

```
public RecoveryResponse recoverToTarget() {
        final Engine engine = shard.engine();
        assert engine.getTranslog() != null : "translog must not be null";
        try (Translog.View translogView = engine.getTranslog().newView()) {
     
            final SnapshotIndexCommit phase1Snapshot;
            phase1Snapshot = shard.snapshotIndex(false);
            phase1(phase1Snapshot, translogView);
                      
            try (Translog.Snapshot phase2Snapshot = translogView.snapshot()) {
                phase2(phase2Snapshot);
            } catch (Throwable e) {
                throw new RecoveryEngineException(shard.shardId(), 2, "phase2 failed", e);
            }

            finalizeRecovery();
        }
        return response;
    }
```

首先创建一个Translog的视图（创建视图的细节我现在也还没研究），接着的话对当前的索引进行snapshot。 然后进入phase1阶段，该阶段是把索引文件和请求的进行对比，然后得出有差异的部分，主动将数据推送给请求方。之后进入文件清理阶段，然后就进入translog 阶段：

```
protected void prepareTargetForTranslog(final Translog.View translogView) {
```

接着进入第二阶段：

```
try (Translog.Snapshot phase2Snapshot = translogView.snapshot()) {
                phase2(phase2Snapshot);           
            }
```

对当前的translogView 进行一次snapshot,然后进行translog发送:

```
int totalOperations = sendSnapshot(snapshot);
```
具体的发送逻辑如下：

```
 cancellableThreads.execute(new Interruptable() {
                    @Override
                    public void run() throws InterruptedException {
                        final RecoveryTranslogOperationsRequest translogOperationsRequest = new RecoveryTranslogOperationsRequest(
                                request.recoveryId(), request.shardId(), operations, snapshot.estimatedTotalOperations());
                        transportService.submitRequest(request.targetNode(), RecoveryTarget.Actions.TRANSLOG_OPS, translogOperationsRequest,
                                recoveryOptions, EmptyTransportResponseHandler.INSTANCE_SAME).txGet();
                    }
                });
```

这里发的请求，都是被 RecoveryTarget的TranslogOperationsRequestHandler 处理器来完成的，具体代码是：

```
 @Override
        public void messageReceived(final RecoveryTranslogOperationsRequest request, final TransportChannel channel) throws Exception {
            try (RecoveriesCollection.StatusRef statusRef = onGoingRecoveries.getStatusSafe(request.recoveryId(), request.shardId())) {
                final ClusterStateObserver observer = new ClusterStateObserver(clusterService, null, logger);
                final RecoveryStatus recoveryStatus = statusRef.status();
                final RecoveryState.Translog translog = recoveryStatus.state().getTranslog();
                translog.totalOperations(request.totalTranslogOps());
                assert recoveryStatus.indexShard().recoveryState() == recoveryStatus.state();
                try {
                    recoveryStatus.indexShard().performBatchRecovery(request.operations());
```

这里调用IndexShard.performBatchRecovery进行translog 的回放。

最后发送一个finalizeRecovery给target 节点，完成recovering操作。


## 关于Recovery translog 配置相关

在如下的类里有：

```
//org.elasticsearch.index.translog.TranslogService
INDEX_TRANSLOG_FLUSH_INTERVAL = "index.translog.interval";
INDEX_TRANSLOG_FLUSH_THRESHOLD_OPS = "index.translog.flush_threshold_ops";
INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE = "index.translog.flush_threshold_size";
INDEX_TRANSLOG_FLUSH_THRESHOLD_PERIOD = "index.translog.flush_threshold_period";
INDEX_TRANSLOG_DISABLE_FLUSH = "index.translog.disable_flush";
```

当服务器恢复时发现有存在的translog日志，就会进入TRANSLOG 阶段进行replay。translog 的recovery 是走的标准的InternalEngine.create/update等方法，并且还会再写translog，同时还有一个影响性能的地方是很多数据可能已经存在，会走update操作，所以性能还是非常差的。这个目前能够想到的解决办法是调整flush日志的频率，保证存在的translog 尽量的少。 上面的话可以看出有三个控制选项：

```
//每隔interval的时间，就去检查下面三个条件决定是不是要进行flush，
//默认5s。时间过长，会超出下面阈值比较大。
index.translog.interval 

//超过多少条日志后需要flush,默认Int的最大值
index.translog.flush_threshold_ops 

//定时flush，默认30m 可动态设置
index.translog.flush_threshold_period

//translog 大小超过多少后flush,默认512m  
index.translog.flush_threshold_size
```

本质上translog的恢复速度和条数的影响关系更大些，所以建议大家设置下 index.translog.flush_threshold_ops，比如多少条就一定要flush,否则积累的太多，
出现故障，恢复就慢了。这些参数都可以动态设置，但建议放到配置文件。
