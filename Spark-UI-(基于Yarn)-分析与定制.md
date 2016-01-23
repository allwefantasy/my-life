>   这篇文章的主旨在于让你了解Spark UI体系，并且能够让你有能力对UI进行一些定制化增强。在分析过程中，你也会深深的感受到Scala语言的魅力。

##前言

有时候我们希望能对Spark UI进行一些定制化增强。并且我们希望尽可能不更改Spark的源码。为了达到此目标，我们会从如下三个方面进行阐述：

1. 理解Spark UI的处理流程
2. 现有Executors页面分析
3. 自己编写一个HelloWord页面

## Spark UI 处理流程

Spark UI 在SparkContext 对象中进行初始化，对应的代码：

```
_ui =  if (conf.getBoolean("spark.ui.enabled", true)) { 
   Some(SparkUI.createLiveUI(this, _conf, listenerBus, _jobProgressListener,      _env.securityManager, appName, startTime = startTime)) 
 } else
 {    
  // For tests, do not enable the UI    None 
 }// Bind the UI before starting the task scheduler to communicate
// the bound port to the cluster manager properly
_ui.foreach(_.bind())
```
这里做了两个动作，

1. 通过`SparkUI.createLiveUI` 创建一个SparkUI实例 `_ui`
2. 通过 `_ui.foreach(_.bind())`启动jetty。bind 方法是继承自WebUI,该类负责和真实的Jetty Server API打交道。

和传统的Web服务不一样，Spark并没有使用什么页面模板引擎，而是自己定义了一套页面体系。我们把这些对象分成两类：

1. 框架类，就是维护各个页面关系，和Jetty API有关联，负责管理的相关类。
2. 页面类，比如页面的Tab,页面渲染的内容等

框架类有:

*  SparkUI，该类继承子WebUI，中枢类，负责启动jetty,保存页面和URL Path之间的关系等。
* WebUI

页面类:

*  SparkUITab(继承自WebUITab) ，就是首页的标签栏
*  WebUIPage，这个是具体的页面。

SparkUI 负责整个Spark UI构建是，同时它是一切页面的根对象。

对应的层级结构为：

     SparkUI -> WebUITab ->  WebUIPage


在SparkContext初始化的过程中，SparkUI会启动一个Jetty。而建立起Jetty 和WebUIPage的桥梁是`org.apache.spark.ui.WebUI`类，该类有个变量如下：

    protected val handlers = ArrayBuffer[ServletContextHandler]()

这个`org.eclipse.jetty.servlet.ServletContextHandler`是标准的jetty容器的handler,而

     protected val pageToHandlers = new HashMap[WebUIPage,   ArrayBuffer[ServletContextHandler]]

`pageToHandlers ` 则维护了WebUIPage到ServletContextHandler的对应关系。

这样，我们就得到了WebUIPage 和 Jetty Handler的对应关系了。一个Http请求就能够被对应的WebUIPage给承接。

从 MVC的角度而言，WebUIPage 更像是一个Controller(Action)。内部实现是WebUIPage被包括进了一个匿名的Servlet. 所以实际上Spark 实现了一个对Servlet非常Mini的封装。如果你感兴趣的话，可以到`org.apache.spark.ui.JettyUtils` 详细看看。

目前spark 支持三种形态的http渲染结果：

*  text/json
* text/html
* text/plain

一般而言一个WebUIPage会对应两个Handler，

```
val renderHandler = createServletHandler(
 pagePath, 
 (request: HttpServletRequest) => page.render(request), 
securityManager,
 basePath)

val renderJsonHandler = createServletHandler(pagePath.stripSuffix("/") + "/json",  (request: HttpServletRequest) => page.renderJson(request), securityManager, basePath)
```

在页面路径上，html和json的区别就是html的url path 多加了一个"/json"后缀。 这里可以看到，一般一个page最好实现

* render
* renderJson

两个方法，以方便使用。

另外值得一提的是，上面的代码也展示了URL Path和对应的处理逻辑(Controller/Action)是如何关联起来的。其实就是pagePath -> Page的render函数。

## Executors页面分析

我们以 `Executors 显示列表页` 为例子，来讲述怎么自定义开发一个Page。

首先你需要定义个Tab,也就是ExecutorsTab，如下：

     private[ui] class ExecutorsTab(parent: SparkUI) extends SparkUITab(parent, "executors")

ExecutorsTab会作为一个标签显示在Spark首页上。
接着定义一个`ExecutorsPage`,作为标签页的呈现内容，并且通过

    attachPage(new ExecutorsPage(this, threadDumpEnabled))

关联上 ExecutorsTab 和  ExecutorsPage。

ExecutorsPage 的定义如下：

     private[ui] class ExecutorsPage(    parent: ExecutorsTab,    threadDumpEnabled: Boolean)  
     extends WebUIPage("")

实现`ExecutorsPage.render`方法：

     def render(request: HttpServletRequest): Seq[Node] 

最后一步调用

     SparkUIUtils.headerSparkPage("Executors (" + execInfo.size + ")",     content, parent)

输出设置页面头并且输出content页面内容。

这里比较有意思的是，Spark 并没有使用类似Freemarker或者Velocity等模板引擎，而是直接利用了Scala对html/xml的语法支持。类似这样，写起来也蛮爽的。

```
val execTable =  <table class={UIUtils.TABLE_CLASS_STRIPED}>    
<thead>      
<th>Executor ID</th>     
<th>Address</th>      
<th>RDD Blocks</th>     
<th><span data-toggle="tooltip" title={ToolTips.STORAGE_MEMORY}>Storage Memory</span>
</th>      
<th>Disk Used</th>      
<th>Active Tasks</th>
```

如果想使用变量，使用`{}`即可。

那最终这个Tag是怎么添加到页面上的呢？
如果你去翻看了源码，会比较心疼,他是在SparkUI的`initialize`方法里定义的：

```
def initialize() {  
attachTab(new JobsTab(this))  attachTab(stagesTab)  
attachTab(new StorageTab(this))  
attachTab(new EnvironmentTab(this))  
attachTab(new ExecutorsTab(this))  
```

那我们新增的该怎么办？其实也很简单啦，通过sparkContext获取到 sparkUI对象，然后调用attachTab方法即可完成，具体如下：

```
sc.ui.getOrElse {  throw new SparkException("Parent SparkUI to attach this tab to not found!")}
.attachTab(new ExecutorsTab) 
```
如果你是在spark-streaming里，则简单通过如下代码就能把你的页面页面添加进去:

```
ssc.start()
new KKTab(ssc).attach()
ssc.awaitTermination()
```

添加新的Tab可能会报错，scala报的错误比较让人困惑，可以试试加入下面依赖：

```
<dependency>    
<groupId>org.eclipse.jetty</groupId>    
<artifactId>jetty-servlet</artifactId>    <version>9.3.6.v20151106</version>
</dependency>
```

## 实现新增一个HelloWord页面

我们的例子很简单，类似下面的图：

![无标题.png](http://upload-images.jianshu.io/upload_images/1063603-9f8d0bd3c11bfdee.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

按前文的描述，我们需要一个Tab页，以及一个展示Tab对应内容的Page页。其实就下面两个类。

`org.apache.spark.streaming.ui2.KKTab`:

```
package org.apache.spark.streaming.ui2

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.ui2.KKTab._
import org.apache.spark.ui.{SparkUI, SparkUITab}
import org.apache.spark.{Logging, SparkException}

/**
 * 1/1/16 WilliamZhu(allwefantasy@gmail.com)
 */
class KKTab(val ssc: StreamingContext)
  extends SparkUITab(getSparkUI(ssc), "streaming2") with Logging {
  private val STATIC_RESOURCE_DIR = "org/apache/spark/streaming/ui/static"
  attachPage(new TTPage(this))

  def attach() {
    getSparkUI(ssc).attachTab(this)
    getSparkUI(ssc).addStaticHandler(STATIC_RESOURCE_DIR, "/static/streaming")
  }

  def detach() {
    getSparkUI(ssc).detachTab(this)
    getSparkUI(ssc).removeStaticHandler("/static/streaming")
  }
}

private[spark] object KKTab {
  def getSparkUI(ssc: StreamingContext): SparkUI = {
    ssc.sc.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
```

`org.apache.spark.streaming.ui2.TTPage` 如下：

```
import org.apache.spark.Logging
import org.apache.spark.ui.{UIUtils => SparkUIUtils, WebUIPage}
import org.json4s.JsonAST.{JNothing, JValue}

import scala.xml.Node

/**
 * 1/1/16 WilliamZhu(allwefantasy@gmail.com)
 */
private[spark] class TTPage(parent: KKTab)
  extends WebUIPage("") with Logging {

  override def render(request: HttpServletRequest): Seq[Node] = {
    val content = <p>TTPAGE</p>
    SparkUIUtils.headerSparkPage("TT", content, parent, Some(5000))
  }
  override def renderJson(request: HttpServletRequest): JValue = JNothing
}
```

记得添加上面提到的jetty依赖。
