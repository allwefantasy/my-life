> 之前有听过Zero-Copy 技术，而Kafka是典型的使用者。网上找了找，竟然没有找到合适的介绍文章。正好这段时间正在阅读Kafka的相关代码，于是有了这篇内容。这篇文章会简要介绍Zero-Copy技术在Kafka的使用情况，希望能给大家一定借鉴和学习样例。

## 前言

Kafka 我个人感觉是性能优化的典范。而且使用Scala开发，代码写的也很漂亮的。重点我觉得有四个

* NIO
* Zero Copy
* 磁盘顺序读写
* Queue数据结构的极致使用

Zero-Copy 实际的原理，大家还是去Google下。这篇文章重点会分析这项技术是怎么被嵌入到Kafa里的。包含两部分：

1. Kafka在什么场景下用了这个技术
2. Zero-Copy 是如何被调用，并且发挥作用的。

## Kafka在什么场景下使用该技术

答案是:

> 消息消费的时候 

包括外部Consumer以及Follower 从partiton Leader同步数据，都是如此。简单描述就是：

Consumer从Broker获取文件数据的时候，直接通过下面的方法进行channel到channel的数据传输。

```
java.nio.FileChannel.transferTo(
long position, 
long count,                                
WritableByteChannel target)`
```
也就是说你的数据源是一个Channel,数据接收端也是一个Channel(SocketChannel),则通过该方式进行数据传输，是直接在内核态进行的，避免拷贝数据导致的内核态和用户态的多次切换。

## Kafka 如何使用Zero-Copy流程分析

估计看完这段内容，你对整个Kafka的数据处理流程也差不多了解了个大概。为了避免过于繁杂，以至于将整个Kafka的体系都拖进来，我们起始点从KafkaApis相关的类开始。

### 数据的生成

对应的类名称为：

    kaka.server.KafkaApis

该类是负责真正的Kafka业务逻辑处理的。在此之前的，譬如 SocketServer等类似Tomcat服务器一样，侧重于交互，属于框架层次的东西。KafkaApis 则类似于部署在Tomcat里的应用。


```
def handle(request: RequestChannel.Request) {
       ApiKeys.forId(request.requestId) match {
        case ApiKeys.PRODUCE => handleProducerRequest(request)
        case ApiKeys.FETCH => handleFetchRequest(request)
        .....
```

handle 方法是所有处理的入口，然后根据请求的不同，有不同的处理逻辑。这里我们关注`ApiKeys.FETCH `这块，也就是有消费者要获取数据的逻辑。进入 `handleFetchRequest`方法，你会看到最后一行代码如下：

```
replicaManager.fetchMessages(  
       fetchRequest.maxWait.toLong, 
      fetchRequest.replicaId, 
      fetchRequest.minBytes,  
      authorizedRequestInfo,  
      sendResponseCallback)

```

ReplicaManager 包含所有主题的所有partition消息。大部分针对Partition的操作都是通过该类来完成的。

`replicaManager.fetchMessages`  这个方法非常的长。我们只关注一句代码：

```
val logReadResults = readFromLocalLog(fetchOnlyFromLeader, fetchOnlyCommitted, fetchInfo)
```
该方法获取本地日志信息数据。内部会调用`kafka.cluster.Log`对象的read方法：

```
log.read(offset, fetchSize, maxOffsetOpt)
```

Log 对象是啥呢？其实就是对应的一个Topic的Partition. 一个Partition是由很多段(Segment)组成的，这和Lucene非常相似。一个Segment就是一个文件。实际的数据自然是从这里读到的。代码如下：

```
val fetchInfo = entry.getValue.read(startOffset, maxOffset, maxLength, maxPosition)
```

这里的fetchInfo(FetchDataInfo)对象包含两个字段:

* offsetMetadata
* FileMessageSet 

FileMessageSet 其实就是用户在这个Partition这一次消费能够拿到的数据集合。当然，真实的数据还躺在byteBuffer里，并没有记在到内存中。FileMessageSet 里面包含了一个很重要的方法：

```
def writeTo(destChannel: GatheringByteChannel, writePosition: Long, size: Int): Int = {
    ......
    
    val bytesTransferred = (destChannel match {
      case tl: TransportLayer => tl.transferFrom(channel, position, count)
      case dc => channel.transferTo(position, count, dc)
    }).toInt
   
    bytesTransferred
  }
```

这里我们看到了久违的`transferFrom`方法。那么这个方法什么时候被调用呢？我们先搁置下，因为那个是另外一个流程。我们继续分析上面的代码。也就是接着从这段代码开始分析：

```
val logReadResults = readFromLocalLog(fetchOnlyFromLeader, fetchOnlyCommitted, fetchInfo)
```

获取到这个信息后，会执行如下操作：

```
val fetchPartitionData = logReadResults.mapValues(result =>  FetchResponsePartitionData(result.errorCode, result.hw, result.info.messageSet))
responseCallback(fetchPartitionData)
```

 logReadResults 的信息被包装成`FetchResponsePartitionData`, FetchResponsePartitionData 包含了我们的FileMessageSet 对象。还记得么，这个对象包含了我们要跟踪的`tranferTo方法`。然后FetchResponsePartitionData 会给responseCallback作为参数进行回调。

responseCallback 的函数签名如下(我去掉了一些我们不关心的信息)：


```
def sendResponseCallback(responsePartitionData: Map[TopicAndPartition, FetchResponsePartitionData]) {
      val mergedResponseStatus = responsePartitionData ++ unauthorizedResponseStatus

      def fetchResponseCallback(delayTimeMs: Int) {
        val response = FetchResponse(fetchRequest.correlationId, mergedResponseStatus, fetchRequest.versionId, delayTimeMs)
        requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(request.connectionId, response)))
      }

    }
```

我们重点关注这个回调方法里的`fetchResponseCallback `。 我们会发现这里 FetchResponsePartitionData 会被封装成一个`FetchResponseSend ` ,然后由`requestChannel`发送出去。

因为Kafka完全应用是NIO的异步机制，所以到这里，我们无法再跟进去了，需要从另外一部分开始分析。


### 数据的发送

前面只是涉及到数据的获取。读取日志，并且获得对应MessageSet对象。MessageSet 是一段数据的集合，但是该数据没有真实的被加载。
这里会涉及到Kafka 如何将数据发送回Consumer端。

在SocketServer，也就是负责和所有的消费者打交道，建立连接的中枢里，会不断的进行poll操作

```
override def run() {
    startupComplete()
    while(isRunning) {
      try {
        // setup any new connections that have been queued up
        configureNewConnections()
        // register any new responses for writing
        processNewResponses()
```

首先会注册新的连接，如果有的话。接着就是处理新的响应了。还记得刚刚上面我们通过`requestChannel `把`FetchResponseSend `发出来吧。

```
private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while(curr != null) {
      try {
        curr.responseAction match {         
          case RequestChannel.SendAction =>
            selector.send(curr.responseSend)
            inflightResponses += (curr.request.connectionId -> curr)
          
        }
      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }
```

这里类似的，processNewResponses方法会先通过`send`方法把FetchResponseSend注册到selector上。 这个操作其实做的事情如下：

```
//SocketServer.scala    
public void send(Send send) {
        KafkaChannel channel = channelOrFail(send.destination());
        channel.setSend(send);
    }

//KafkaChannel.scala
   public void setSend(Send send) {
         this.send = send;          
         this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);     
    }
```

为了方便看代码，我对代码做了改写。我们看到，其实send就是做了一个WRITE时间注册。这个是和NIO机制相关的。如果大家看的有障碍，不妨先学习下相关的机制。


回到 SocketServer 的`run`方法里，也就是上面已经贴过的代码：

```
  override def run() {
    startupComplete()
    while(isRunning) {
      try {
        // setup any new connections that have been queued up
        configureNewConnections()
        // register any new responses for writing
        processNewResponses()

        try {
          selector.poll(300)
        } catch {
          case...
        }
```

SocketServer 会poll队列，一旦对应的KafkaChannel 写操作ready了，就会调用KafkaChannel的write方法：

```
//KafkaChannel.scala
public Send write() throws IOException {
        if (send != null && send(send)) 
    }
//
//KafkaChannel.scala
private boolean send(Send send) throws IOException {
        send.writeTo(transportLayer);
        if (send.completed())
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        return send.completed();
    }

```
依然的，为了减少代码，我做了些调整，其中write会调用 send方法，对应的Send对象其实就是上面我们注册的`FetchResponseSend ` 对象。

这段代码里真实发送数据的代码是`send.writeTo(transportLayer);`，

对应的writeTo方法为：

```
private val sends = new MultiSend(dest, JavaConversions.seqAsJavaList(fetchResponse.dataGroupedByTopic.toList.map {
    case(topic, data) => new TopicDataSend(dest, TopicData(topic,
                                                     data.map{case(topicAndPartition, message) => (topicAndPartition.partition, message)}))
    }))
override def writeTo(channel: GatheringByteChannel): Long = {
    .....    
     written += sends.writeTo(channel)
    ....
  }
```

这里我依然做了代码简化，只让我们关注核心的。 这里最后是调用了`sends`的writeTo方法，而sends 其实是个`MultiSend `。
这个MultiSend 里有两个东西：

* topicAndPartition.partition: 分区
* message:FetchResponsePartitionData  

还记得这个FetchResponsePartitionData  么？我们的MessageSet 就被放在了FetchResponsePartitionData这个对象里。

TopicDataSend 也包含了sends,该sends 包含了 PartitionDataSend，而 PartitionDataSend则包含了FetchResponsePartitionData。

最后进行writeTo的时候，其实是调用了

```
//partitionData 就是 FetchResponsePartitionData
//messages 其实就是FileMessageSet
val bytesSent = partitionData.messages.writeTo(channel, messagesSentSize, messageSize - messagesSentSize)

```

如果你还记得的话，FileMessageSet 也有个writeTo方法，就是我们之前已经提到过的那段代码：

```
def writeTo(destChannel: GatheringByteChannel, writePosition: Long, size: Int): Int = {
    ......

    val bytesTransferred = (destChannel match {
      case tl: TransportLayer => tl.transferFrom(channel, position, count)
      case dc => channel.transferTo(position, count, dc)
    }).toInt

    bytesTransferred
  }
```

终于走到最底层了，最后其实是通过tl.transferFrom(channel, position, count) 来完成最后的数据发送的。这里你可能比较好奇，不应该是调用`transferTo` 方法么? `transferFrom `其实是Kafka自己封装的一个方法，最终里面调用的也是transerTo:

```
  @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        return fileChannel.transferTo(position, count, socketChannel);
    }
```

## 总结

Kafka的整个调用栈还是非常绕的。尤其是引入了NIO的事件机制，有点类似Shuffle,把流程调用给切断了，无法简单通过代码引用来进行跟踪。Kafka还有一个非常优秀的机制就是DelayQueue机制，我们在分析的过程中，为了方便，把这块完全给抹掉了。
