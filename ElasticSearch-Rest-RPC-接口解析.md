> ElasticSearch 的体系结构比较复杂，层次也比较深，源码注释相比其他的开源项目要少。这是ElasticSearch 系列的第一篇。解析ElasticSearch的接口层，也就是Rest/RPC接口相关。我们会描述一个请求从http接口到最后被处理都经过了哪些环节。


## 一些基础知识

早先ES的HTTP协议支持还是依赖Jetty的,现在不管是Rest还是RPC都是直接基于Netty了。

另外值得一提的是，ES 是使用Google的Guice 进行模块管理，所以了解Guice的基本使用方式有助于你了解ES的代码组织。

ES 的启动类是 `org.elasticsearch.bootstrap.Bootstrap`。在这里进行一些配置和环境初始化后会启动`org.elasticsearch.node.Node`。Node 的概念还是蛮重要的，节点的意思，也就是一个ES实例。RPC 和 Http的对应的监听启动都由在该类完成。

Node 属性里有一个很重要的对象，叫client,类型是 NodeClient,我们知道ES是一个集群，所以每个Node都需要和其他的Nodes 进行交互，这些交互则依赖于NodeClient来完成。所以这个对象会在大部分对象中传递，完成相关的交互。

先简要说下：

* NettyTransport 对应RPC 协议支持
* NettyHttpServerTransport 则对应HTTP协议支持

## Rest 模块解析

首先，NettyHttpServerTransport 会负责进行监听Http请求。通过配置`http.netty.http.blocking_server` 你可以选择是Nio还是传统的阻塞式服务。默认是NIO。该类在配置pipeline的时候，最后添加了HttpRequestHandler，所以具体的接受到请求后的处理逻辑就由该类来完成了。

```
pipeline.addLast("handler", requestHandler);
```

HttpRequestHandler 实现了标准的 `messageReceived(ChannelHandlerContext ctx, MessageEvent e) ` 方法，在该方法中，HttpRequestHandler 会回调`NettyHttpServerTransport.dispatchRequest`方法，而该方法会调用`HttpServerAdapter.dispatchRequest`,接着又会调用`HttpServer.internalDispatchRequest `方法(额，好吧，我承认嵌套挺深有点深)：

```
public void internalDispatchRequest(final HttpRequest request, final HttpChannel channel) {
        String rawPath = request.rawPath();
        if (rawPath.startsWith("/_plugin/")) {
            RestFilterChain filterChain = restController.filterChain(pluginSiteFilter);
            filterChain.continueProcessing(request, channel);
            return;
        } else if (rawPath.equals("/favicon.ico")) {
            handleFavicon(request, channel);
            return;
        }
        restController.dispatchRequest(request, channel);
    }
```

这个方法里我们看到了plugin等被有限处理。最后请求又被转发给      RestController。

RestController 大概类似一个微型的Controller层框架，实现了：

1.  存储了 Method + Path -> Controller 的关系
2.  提供了注册关系的方法
3.  执行Controller的功能。

那么各个Controller(Action) 是怎么注册到RestController中的呢？

在ES中，Rest*Action 命名的类的都是提供http服务的，他们会在RestActionModule 中被初始化，对应的构造方法会注入RestController实例，接着在构造方法中，这些Action会调用`controller.registerHandler` 将自己注册到RestController。典型的样子是这样的：

```
@Inject
    public RestSearchAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_search", this);
        controller.registerHandler(POST, "/_search", this);
        controller.registerHandler(GET, "/{index}/_search", this);
```

每个Rest*Action 都会实现一个handleRequest方法。该方法接入实际的逻辑处理。

```
@Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        SearchRequest searchRequest;
        searchRequest = RestSearchAction.parseSearchRequest(request, parseFieldMatcher);
        client.search(searchRequest, new RestStatusToXContentListener<SearchResponse>(channel));
    }
```

首先是会把 请求封装成一个SearchRequest对象，然后交给 NodeClient 执行。

如果用过ES的NodeClient Java API，你会发现，其实上面这些东西就是为了暴露NodeClient API 的功能，使得你可以通过HTTP的方式调用。


## Transport*Action，两层映射关系解析

我们先跑个题，在ES中，Transport*Action 是比较核心的类集合。这里至少有两组映射关系。

* Action -> Transport*Action
* Transport*Action -> *TransportHandler

第一层映射关系由类似下面的代码在ActionModule中完成：

     registerAction(PutMappingAction.INSTANCE,  TransportPutMappingAction.class);

第二层映射则在类似 SearchServiceTransportAction 中维护。目前看来，第二层映射只有在查询相关的功能才有，如下：

```
transportService.registerRequestHandler(FREE_CONTEXT_SCROLL_ACTION_NAME, ScrollFreeContextRequest.class, ThreadPool.Names.SAME, new FreeContextTransportHandler<>());
```

SearchServiceTransportAction 可以看做是SearchService进一步封装。其他的Transport*Action 则只调用对应的Service 来完成实际的操作。

对应的功能是，可以通过Action 找到对应的Transport*Action，这些Transport*Action 如果是query类,则会调用SearchServiceTransportAction，并且通过第二层映射找到对应的Handler,否则可能就直接通过对应的Service完成操作。

下面关于RPC调用解析这块，我们会以查询为例。

## RPC 模块解析

前面我们提到，Rest接口最后会调用NodeClient来完成后续的请求。对应的代码为：

```
public <Request extends ActionRequest, Response extends ActionResponse, RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        TransportAction<Request, Response> transportAction = actions.get(action);
        if (transportAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        transportAction.execute(request, listener);
    }
```

这里的action 就是我们提到的第一层映射，找到Transport*Action.如果是查询，则会找到TransportSearchAction。调用对应的doExecute 方法，接着根据searchRequest.searchType找到要执行的实际代码。下面是默认的：

```
else if (searchRequest.searchType() == SearchType.QUERY_THEN_FETCH) {    queryThenFetchAction.execute(searchRequest, listener);}
```
我们看到Transport*Action 是可以嵌套的，这里调用了TransportSearchQueryThenFetchAction.doExecute 

```
@Overrideprotected void doExecute(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {   
 new AsyncAction(searchRequest, listener).start();
}
```

在AsyncAction中完成三个步骤：

1. query
2. fetch
3. merge

为了分析方便，我们只分析第一个步骤。

```
@Overrideprotected void sendExecuteFirstPhase(
DiscoveryNode node, 
ShardSearchTransportRequest request, 
ActionListener<QuerySearchResultProvider> listener) {   
     searchService.sendExecuteQuery(node, request, listener);
}
```

这是AsyncAction 中执行query的代码。我们知道ES是一个集群，所以query 必然要发到多个节点去，如何知道某个索引对应的Shard 所在的节点呢？这个是在AsyncAction的父类中完成，该父类分析完后会回调子类中的对应的方法来完成，譬如上面的sendExecuteFirstPhase 方法。

说这个是因为需要让你知道，上面贴出来的代码只是针对一个节点的查询结果，但其实最终多个节点都会通过相同的方式进行调用。所以才会有第三个环节 merge操作，合并多个节点返回的结果。

```
searchService.sendExecuteQuery(node, request, listener);
```

其实会调用transportService的sendRequest方法。大概值得分析的地方有两个：

```
if (node.equals(localNode)) {
                sendLocalRequest(requestId, action, request);
            } else {
                transport.sendRequest(node, requestId, action, request, options);
            }
```

我们先分析，如果是本地的节点，则sendLocalRequest是怎么执行的。如果你跑到senLocalRequest里去看，很简单，其实就是：

```
reg.getHandler().messageReceived(request, channel);
```

reg 其实就是前面我们提到的第二个映射，不过这个映射其实还包含了使用什么线程池等信息，我们在前面没有说明。

这里 reg.getHandler == SearchServiceTransportAction.SearchQueryTransportHandler,所以messageReceived 方法对应的逻辑是：

```
QuerySearchResultProvider result = searchService.executeQueryPhase(request);
channel.sendResponse(result);
```

这里，我们终于看到searchService。 在searchService里，就是整儿八景的Lucene相关查询了。这个我们后面的系列文章会做详细分析。

如果不是本地节点，则会由NettyTransport.sendRequest 发出远程请求。
假设当前请求的节点是A,被请求的节点是B,则B的入口为MessageChannelHandler.messageReceived。在NettyTransport中你可以看到最后添加的pipeline里就有MessageChannelHandler。我们跑进去messageReceived 看看，你会发现基本就是一些协议解析，核心方法是handleRequest，接着就和本地差不多了，我提取了关键的几行代码：

```
final RequestHandlerRegistry reg = transportServiceAdapter.getRequestHandler(action);
threadPool.executor(reg.getExecutor()).execute(new RequestHandler(reg, request, transportChannel));
```

这里被RequestHandler包了一层，其实内部执行的就是本地的那个。RequestHandler 的run方法是这样的：

```
protected void doRun() throws Exception {     reg.getHandler().messageReceived(request, transportChannel);
}
```

这个就和前面的sendLocalRequest里的一模一样了。


## 总结

到目前为止，我们知道整个ES的Rest/RPC 的起点是从哪里开始的。RPC对应的endpoint 是MessageChannelHandler，在NettyTransport 被注册。Rest 接口的七点则在NettyHttpServerTransport,经过层层代理，最终在RestController中被执行具体的Action。 Action 的所有执行都会被委托给NodeClient。 NodeClient的功能执行单元是各种Transport*Action。对于查询类请求，还多了一层映射关系。
