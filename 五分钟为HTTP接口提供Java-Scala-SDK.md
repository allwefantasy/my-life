> 上次构建Spark 任务发布管理程序时，正好用到了两个yarn的接口。因为不想引入Yarn的client包，所以使用了他的Http接口。那么如何调用这个HTTP接口便是一个问题了


## Case描述

我现在要使用yarn的两个接口，一个是application 列表，一个是根据appId获取这个app的详情。对应的接口大约如此：

> http://[dns]/ws/v1/cluster/apps
> http://[dns]/ws/v1/cluster/apps/{appId}

## 基于HttpClient的初级封装

基于HttpClient的一个典型封装如下：

```
public interface HttpTransportService {

    public SResponse post(Url url, Map data);

    public SResponse post(Url url, Map data, Map<String, String> headers);

    public SResponse post(final Url url, final Map data, final int timeout);

    public SResponse post(final Url url, final Map data, final Map<String, String> headers, final int timeout);

    public SResponse get(final Url url, final int timeout);

    public SResponse get(Url url);

    public SResponse get(Url url, Map<String, String> data);

    public SResponse get(final Url url, final Map<String, String> data, final int timeout);

    public SResponse put(Url url, Map data);
```

更详细的代码可以参看我以前的开源项目里的实现：[HttpTransportService.java](https://github.com/allwefantasy/ServiceFramework/blob/master/src/main/java/net/csdn/modules/transport/DefaultHttpTransportService.java)

然而这种使用比较原始，不直观。

大致使用方法如下：

```
SResponse  response = HttpTransportService.get(new Url("http://[dns]/ws/v1/cluster/apps/"+appId))
if(response.getStatus==200){
  //parse result like JSONObject.parse(response.getContent())
}else{
//blabla
}
```


所以一般如果提供了HTTP 接口的项目都会给你一个SDK,方便你做调用，帮你屏蔽掉HTTP的细节。

总体而言你有如下几种选择：

1. 直接使用上面的封装
2. 使用第三方SDK
3. 自己再做一些封装，转化为普通的方法调用

第三种方案一般会是这样：

```
def app(appName:String):YarnApplication = {
// 实现http接口请求解析逻辑
} 
```

虽然麻烦了些，但是调用者会比较幸福。

## 其实可以更简单

只要三步，就可以实现第三个方案：

1. 定义一个接口
2. 获取这个接口的引用
3. 尽情使用


定义一个接口：

```
trait YarnController {

  @At(path = Array("/ws/v1/cluster/apps"), types = Array(RestRequest.Method.GET))
  def apps(@Param("states") states: String): java.util.List[HttpTransportService.SResponse]

  @At(path = Array("/ws/v1/cluster/apps/{appId}"), types = Array(RestRequest.Method.GET))
  def app(@Param("appId") appId: String): java.util.List[HttpTransportService.SResponse]

}
```

At 注解定义了 path路径以及Action Method。 方法参数决定了传递的参数。

对于同一个http接口，你也可以定义多个方法。比如，


```
trait YarnController {

  @At(path = Array("/ws/v1/cluster/apps"), types = Array(RestRequest.Method.GET))
  def apps(@Param("states") states: String): java.util.List[HttpTransportService.SResponse]

  @At(path = Array("/ws/v1/cluster/apps"), types = Array(RestRequest.Method.GET))
  def runningApps(@Param("states") states: String="running"): java.util.List[HttpTransportService.SResponse]

}
```

这样你直接调用runningApps 就可以拿到特定状态的应用，而无需传递参数。如果参数较多，你还可以指定哪些参数不传，哪些传。

接着初始化 YarnController，获得对象的引用，代码如下：


```
val yarnRestClient: YarnController = 
AggregateRestClient.buildIfPresent[YarnController]
(hostAndPort, 
firstMeetProxyStrategy, 
RestClient.transportService)
```

* hostAndPort 是yarn的地址
* firstMeetProxyStrategy 指定如果后端有多个实例时的访问策略
* RestClient.transportService 就是我上面的最基础的封装HttpTransportService

理论上后面两个参数可以不用传递

这个时候你就可以直接使用获得的`YarnController `对象了。具体使用方式如下：

```
val result:java.util.List[HttpTransportService.SResponse] = yarnRestClient.apps()  
```

上面就是你要做的所有工作，系统自动帮你实现了HTTP调用。


## 我希望返回结果是一个Bean

前面的放回结果是个List[SResponse]对象。我希望它是个Bean对象。所以我定义一个Bean类：

```
class YarnApplication(val id: String,
                            var user: String,
                            var name: String,
                            var queue: String,
                            var state: String,
                            var finalStatus: String,
                            var progress: Long,
                            var trackingUI: String,
                            var trackingUrl: String,
                            var diagnostics: String,
                            var clusterId: Long,
                            var applicationType: String,
                            var applicationTags: String,
                            var startedTime: Long,
                            var finishedTime: Long,
                            var elapsedTime: Long,
                            var amContainerLogs: String,
                            var amHostHttpAddress: String,
                            var allocatedMB: Long,
                            var allocatedVCores: Long,
                            var runningContainers: Long,
                            var memorySeconds: Long,
                            var vcoreSeconds: Long,
                            var preemptedResourceMB: Long,
                            var preemptedResourceVCores: Long,
                            var numNonAMContainerPreempted: Long,
                            var numAMContainerPreempted: Long)
```

然后引入一个隐式转换

```
import ..... SReponseConvertor ._
val result:Map[Map[String,List[YarnApplication]]]= yarnRestClient.apps().extract[Map[Map[String,List[YarnApplication]]]] 

result("apps")("app") //这样就能拿到List[YarnApplication]
```

SReponseConvertor 给List[SReponse]对象添加了一个新的extract 方法。当然前提是List[SReponse] 里是一个JSON格式的数据。

因为yarn的接口返回的格式比较诡异，嵌套了两层，第一层是apps,第二层是app,第三层才是具体的List对象。所以有了上面的复杂形态。那我如何简化呢？每次调用都这么搞，太复杂了。

那么自己实现一个隐式转换就行了，定义一个YarnControllerE类，
```
object YarnControllerE {
  implicit def mapSResponseToObject(response: java.util.List[HttpTransportService.SResponse]): SResponseEnhance = {
    new SResponseEnhance(response)
  }
}

import scala.collection.JavaConversions._

class SResponseEnhance(x: java.util.List[HttpTransportService.SResponse]) {

  private def extract[T](res: String)(implicit manifest: Manifest[T]): T = {
    if (x == null || x.isEmpty || x(0).getStatus != 200) {
      return null.asInstanceOf[T]
    }
    implicit val formats = SJSon.DefaultFormats
    SJSon.parse(res).extract[T]
  }


  def list(): List[YarnApplication] = {
    val item = extract[Map[String, Map[String, List[YarnApplication]]]](x(0).getContent)
    return item("apps")("app")
  }
```

现在你可以很帅气这样调用了：


```
import ..... YarnControllerE ._
val result: List[YarnApplication] = yarnRestClient.apps().list
```

这样我们就可以像RPC一样访问一个HTTP接口了。

##  背后的机制

核心代码其实是这个：

```
val yarnRestClient: YarnController = 
AggregateRestClient.buildIfPresent[YarnController]
(hostAndPort, 
firstMeetProxyStrategy, 
RestClient.transportService)
```

AggregateRestClient 会帮你把YarnController 自动实现了。实现的机制很简单就是 JDK 的 Proxy机制。具体源码可以参看:[AggrateRestClient.scala](https://github.com/allwefantasy/ServiceFramework/blob/master/src/main/java/net/csdn/modules/transport/proxy/AggrateRestClient.scala) 以及[RestClientProxy.java](https://github.com/allwefantasy/ServiceFramework/blob/master/src/main/java/com/alibaba/dubbo/rpc/protocol/rest/RestClientProxy.java)
