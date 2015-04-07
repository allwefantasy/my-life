# 大数据技术栈


## 前言

很多人写这方面的文章都喜欢从大的方面讲，讲体系结构，画出各种好看的图，看多了，感觉大家都差不多的样子。比较典型的，我记得是有一期程序员杂志，专门讲各大电商技术体系相关的内容，如果没记错的话，都是各个公司的技术负责人或者主程写的，我的第一感是，诶，原来大家都大同小异。

这次我打算从服务写起。从底层写到上层，但是不列具体的体系结构。希望介绍的每个系统，服务，以及里面详细的细节，会让看内容的有下面这些感受：

1.  嘿，这服务(系统)不错，我们也可以用上
2.  啊，原来可以这么做，我们也可以借鉴下
3.  哼，我们的方法比这好多了(那欢迎联系我)
4.  啊，原来系统是这么慢慢坐起来的


##  Web服务框架

无论大数据还是普通的服务，都需要通过HTTP或者RPC等方式对外提供服务。所以有个好用顺手的服务框架还是很有必要的。

对开发而言：

1. 让我尽量少些代码
2. 让我尽量少写配置

对运维而言：

1. 部署简单
2. 监控简单

对框架开发者而言：

1. 轻量
2. 容易扩展

所以我们内部自己开发了一个框架，也是我之前一直推荐介绍的  https://github.com/allwefantasy/ServiceFramework 。 这东西，我不吹牛，真的让你省太多代码,太多配置，结合scala,让你写的心旷神怡。

下面的controller代码来自真实的项目代码：


	class SearchController extends ApplicationController with CacheTrait with JsonpTrait with DebugTrait with InstanceMethodsTrait {


	  def buildSearchKey = {
	    val key = request.path()+"-"+request.queryString()+"-"+request.contentAsString()
	    param("_client_")+"_"+Md5.md5(key.toLowerCase())
	  }


	  @At(
	    path = Array("/v2/{index}/{type}/_search"),
	    types = Array(RestRequest.Method.POST, RestRequest.Method.GET)
	  ) # URL路径配置，以及接受的HTTP方法
	  def search = {
          # 计数组件 非常有用的哦，这里统计这个方法的执行次数 
	    StatsManager.addStats("controller", "req-all", 1)

	    val timeStart = System.currentTimeMillis()

          # 强大的策略层，未来我也会介绍
	    val stragetyParams = buildStragetyParams
          # 透过scala强大的函数变成，这里缓存变得异常简单
	    var re = cacheAsString(stragetyParams,buildSearchKey,0,()=>{
	      val searchRList = dispatcher.dispatch(stragetyParams)

	      val tempFields = stragetyParams.get("_tempFields_").asInstanceOf[util.List[String]]
	      if (paramAsBoolean("sql", false)) {
	        tempFields.addAll(stragetyParams.get("_fields_").asInstanceOf[List[String]])
	      }

	      val debugData = configDebugInfo(stragetyParams, searchRList)

	      val result = if(searchRList.size() == 0) dataTransformUtil.forClientAsString(new SearchResult(new util.ArrayList[SearchHit](), 0, 0), tempFields, debugData)
	      else dataTransformUtil.forClientAsString(searchRList.get(0), tempFields, debugData)
	      (true,result)
	    })

          # 计时，上面的代码执行时间统计，timeLog其实就是封装了计数器
	    timeLog(timeStart, "req_time", "search")

	    if(enableCache(stragetyParams) && debugEnable(stragetyParams) ){
	     val temp =  JSONObject.fromObject(re)
	     temp.put("_debug_",stragetyParams.get("_debug_"))
	     re = temp.toString
	    }

         # 渲染输出
	    if (isEmpty(param("callback"))) render(re)
	    else render(jsonp(param("callback"), re), ViewType.string)

	  }



再看看单元测试：

	@Test
	 public void search() throws Exception {
	     RestResponse response = get("/doc/blog/search", map(
	             "tagNames", "_10,_9"
	     ));
	     Assert.assertTrue(response.status() == 200);
	     Page page = (Page) response.originContent();
	     Assert.assertTrue(page.getResult().size() > 0);
	 }
	 
不依赖容器的。因为ServiceFramework是应用包含容器。

部署也很简单，利用bin目录里的脚本

	./release.sh
	./bin/deploy deploy
	./bin/deploy start
	
就完成一个应用了。

其他好处我就不多说了，看看文档吧。

## 消息队列服务器

这东西太重要了，是我们整个系统数据流转的核心。我们使用了比较小众的一个消息队列，建议使用kafaka之类的。我们看看它的作用

1.  服务之间的数据衔接
2.  服务之间的缓冲
3.  基于之上的各种应用，比如计数器

###  服务之间的数据衔接

系统一大了，服务就非常多，数据会在各个服务之间流动和加工。通常，数据不是单线路流动的，而是会开叉的，会流向到不同系统，最后或者消亡，或者进入数据持久层。这个时候我们希望有一种东西，能够衔接这些。

举个简单例子，当用户浏览了一篇博文，我们会做什么？

1. JS会上报该数据到日志服务器
2. Nginx会将数据落地，然后定时同步到集群上
3. Nginx会将数据发往日志服务上
4. 日志服务会将数据投递到消息队列
5. ETL，我们假设配置了五条链路(假设名称为A-E)，也就是我们对同一条数据进行五种不同的处理，我们看到，数据流在这里开叉了
6. 每个链路实际上消费同一条消息 
7. A处理完后将数据再次投递到消息队列K，B处理完后投递到消息队列K1
8. 资源中心会向K要A处理完的数据,索引服务会向K1要数据构建索引
9. 还有更多操作。。。。。

我们看到，通过消息队列可以很好的衔接各个服务，让他们井然有序的处理数据。

###  服务之间的缓冲

假设没有消息队列，A直接将处理完的数据发给B,那么B会很忐忑，我不能挂掉，因为一旦挂掉，可能就造成数据丢失了。我也不能重启，因为重启的也可能会导致丢失几秒的数据。B还要时刻注意A的产能如何，产能如果过高，会压垮自己的。同样，为了解决这个问题，A也很蛋疼，它需要做重试机制，如果B没有反应过来，我需要判定B到底有没有接受到我的数据，总之很麻烦。

消息队列有效的解决了这些麻烦，A处理完直接把数据丢到消息队列中，B根据自己的系统情况尽量去消费。如果重启或者僵死了，下次依然可以记住上次消费的位置，接着消费就行。

不要小看这一点，对于一个快速成长的系统，不断的发布新版本是很正常的，这个时候就不能出现上面的问题。

### 基于之上的各种应用，比如计数器

计数器我后面会讲，它对服务的监控异常重要，简单而有效。比如我发现最近B系统接受数据有异常，这个时候我想还原数据怎么办，可都被B消费了呀。我们可以再起一个系统去消费B消费的主题，就能还原出数据了。还有数据流跟踪。一条数据经过N个系统流转，每个系统的每个模块都会把对这条数据的处理流程上报给消息队列，后端有个服务去消费这个消息队列，然后将数据存储到HBase中，接着做个前端展示，就可以完整的跟踪到一条数据的流转情况了。

而且，一般消息队列都是集群模式，非常稳定，你可以认为这是一个可靠的服务。基于一个可靠的服务去做事，就好比有了基石。


所以，我强烈建议，对于小团队，如果有多个服务，并且他们之间有交互(非查询性质的)，那么最好是引入。



