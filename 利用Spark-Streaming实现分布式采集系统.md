> 之前我在微信朋友圈发了一段话，说明Spark Streaming 不仅仅是流式计算，也是一类通用的模式，可以让你只关注业务逻辑而无需关注分布式相关的问题而迅速解决业务问题

## 前言

  前两天我刚在自己的一篇文章中鼓吹[数据天生就是流式的](http://www.jianshu.com/p/9574e359ce35),并且指出：

> 批量计算已经在慢慢退化，未来必然是属于流式计算的，数据的流动必定是由数据自己驱动流转的。

而Spark Streaming 在上层概念上，完美融合了批量计算和流式计算，让他们你中有我，我中有你，这种设计使得Spark Streaming 作为流式计算的一个载体，同时也能作为其他一些需要分布式架构的问题提供解决方案。

## Spark Streaming 作为一些分布式任务系统基础的优势

1. 天然就是分布式的，不用再为实现分布式协调而蛋疼
2. 基于Task的任务执行机制，可随意控制Task数量
3. 无需关注机器，是面向资源的，使得部署变得异常简单，申明资源，提交，Over
4. 集成完善的输入输出，包括HDFS/Kafka/ElasticSearch/HBase/MySQL/Redis 等等，这些都无需自己去搞
5. 成熟简单的算子让你对数据的处理变得异常简单
6. [StreamingPro](https://github.com/allwefantasy/streamingpro) 项目让申明式或者复杂的Spark Streaming程序更加简单，同时还可以通过StreamingPro提供的Rest 接口来增强Spark Streaming Driver的交互能力。


现在以标题中的采集系统为例，整个事情你只要实现采集逻辑，至于具体元数据读取，结果存储到哪都可能只要个简单配置或者利用现成的组件，最后部署也只要简单申明下资源就可以在一个可以弹性扩展的集群上。

关于这块的理念，可参考 

* [看不到服务器的年代，一个新的时代](http://www.jianshu.com/p/15c3172c4f0c)
* [Transformer架构解析](http://www.jianshu.com/p/8a88a8bb4700)
* [Spark Streaming 妙用之实现工作流调度器](http://www.jianshu.com/p/89b4f3bf27b2)

## 开发采集系统的动机

目前这个采集系统主要是为了监控使用。但凡一个公司，或者部门内部会有大量的开源系统，每个开源组件都会提供大致三类输出：

1. 标准的metrics 输出，方便你集成到gangila等监控系统上
2. Web UI,比如Spark,Storm,HBase 都提供了自己的Web界面等
3. Rest 接口，主要是 JSon,XML,字符串

但是对于监控来说，前面两个直观易用，但是也都有比较大的问题：

1. metrics 直接输出到监控系统，就意味着没办法定制，如果我希望把多个指标放在一块，这个可能就很难做到。
2. Web UI 则需要人去看了


相反，Rest 接口最为灵活，但是需要自己做写逻辑，比如获取数据，处理，然后做自己的呈现 。问题来了，如果我现在有几千个Rest接口的数据要获取，并且需要一个很方便的手段抽取里面要的值(或者指标)。这便涉及到了两个问题：

1. 收集的接口可能非常多，如何让收集程序是可很横向扩展的？
2. 接口返回的数据形态各异，如何提供一个方便一致的模型，让用户简单通过一个配置就可以抽取出里面的内容？


##  系统处理结构
 

![QQ20160529-1@2x.png](http://upload-images.jianshu.io/upload_images/1063603-dbcb422c5ec9cba8.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

* 采集元数据源，目前存储在ES里
* 采集系统会定时到ES里获取元数据，并且执行特定的收集逻辑
* 通过采集系统的一定的算子，将数据格式化，接入Kafka
* 通过标准(已经存在的)ETL系统完成数据的处理，供后续流程进一步处理

## 通用信息抽取方案

回到上面的一个问题，

> 接口返回的数据形态各异，如何提供一个方便一致的模型，让用户简单通过一个配置就可以抽取出里面的内容

Rest 接口返回的数据，无非四种：

1. HTML
2. JSON
3. XML
4. TEXT

对于1,我们先不探讨。对于JSON,XML 我们可以采用 XPATH，对于TEXT我们可以采用标准的正则或者ETL来进行抽取。

我们在定义一个需要采集的URL时，需要同时配置需要采集的指标以及对应的指标的XPATH路径或者正则。当然也可以交给后端的ETL完成该逻辑。不过我们既然已经基于Spark Streaming做采集系统，自然也可以利用其强大的数据处理功能完成必要的格式化动作。所以我们建议在采集系统直接完成。

## 采集系统

数据源的一个可能的数据结构：


```
 appName      采集的应用名称,cluster1,cluster2
 appType       采集的应用类型,storm/zookeeper/yarn 等
 url                需要采集的接口
 params         可能存在例如post请求之类的，需要额外的请求参数
 method         Get/POST/PUT 等请求方法体
 key_search_qps :  $.store.book[0].author   定义需要抽取的指标名称以及在Response 对应的XPATH 路径
 key_.....  可以有更多的XPATH
 key_.....  可以有更多的XPATH
 extraParams  人工填写一些其他参数
```

采集系统通过我们封装的一个 DInputStream,然后根据batch（调度周期），获取这些数据，之后交给特定的执行逻辑去执行。采用[StreamingPro](https://github.com/allwefantasy/streamingpro),会是这样：


```
"RestCatch": {
    "desc": "RestCatch",
    "strategy": "....SparkStreamingStrategy",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "....ESInputCompositor",
        "params": [
          {
            "es.nodes": "....",
            "es.resource": "monitor_rest/rest"
          }
        ]
      },
      {
        "name": ".....RestFetchCompositor",//发起http请求，获取response
        "params": [
          {
            "resultKey": "result",
            "keyPrefix": "key_"
          }
        ]
      },
      {
        "name": "....JSonExtractCompositor",//根据XPath获取response里的指标
        "params": [
          {
            "resultKey": "result",
            "keyPrefix": "key_"
          }
        ]
      },
      {
        "name": ".....ConsoleOutputCompositor",//输出结果
        "params": []
      }
    ],
    "configParams": {
    }
  }
```

通过上面的配置文件，可以很好看到处理流程。

1. 输入采集源
2. 采集结果
3. 根据XPATH 抽取指标
4. 输出结果


## 制作元数据管理系统

元数据管理系统是必要的，他可以方便你添加新的URL监控项。通过[StreamingPro](https://github.com/allwefantasy/streamingpro),你可以在Spark Streaming 的Driver中添加元数据管理页面，实现对元数据的操作逻辑。我们未来会为 `如何通过StreamingPro` 给Spark Streaming 添加自定义Rest 接口/Web页面提供更好的教程。


## 完结了么？

上面其实已经是试下了一个采集系统的雏形，得益于Spark Streaming天然的分布式，以及灵活的算子，我们的系统是足够灵活，并且可横向扩展。

然而你会发现，

1. 如果我需要每个接口有不同的采集周期该如何？
2. 如果我要实现更好的容错性如何？
3. 如何实现更好的动态扩容？

第一个问题很好解决，我们在元数据里定义采集周期，而Spark Streaming的调度周期则设置为最小粒度。

第二个问题容错性属于业务层面的东西，但是如果有Task失败，Spark Streaming也会把你尝试重新调度和重试。我们建议由自己来完成。

第三个，只要开启了 Dynamic Resource Allocation,则能够根据情况，实现资源的伸缩利用。
