> Spark Streaming 非常适合ETL。但是其开发模块化程度不高，所以这里提供了一套方案，该方案提供了新的API用于开发Spark Streaming程序，同时也实现了模块化，配置化，并且支持SQL做数据处理。

[项目地址](https://github.com/allwefantasy/streamingpro)

## 前言

传统的Spark Streaming程序需要：

* 构建StreamingContext
* 设置checkpoint
* 链接数据源
* 各种transform
* foreachRDD 输出

通常而言，你可能会因为要走完上面的流程而构建了一个很大的程序，比如一个main方法里上百行代码，虽然在开发小功能上足够便利，但是复用度更方面是不够的，而且不利于协作，所以需要一个更高层的开发包提供支持。

##  如何开发一个Spark Streaming程序

我只要在配置文件添加如下一个job配置，就可以作为标准的的Spark Streaming 程序提交运行：

```
{

  "test": {
    "desc": "测试",
    "strategy": "streaming.core.strategy.SparkStreamingStrategy",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "streaming.core.compositor.kafka.MockKafkaStreamingCompositor",
        "params": [
          {
            "metadata.broker.list":"xxx",
            "auto.offset.reset":"largest",
            "topics":"xxx"
          }
        ]
      },
      {
        "name": "streaming.core.compositor.spark.JSONTableCompositor",
        "params": [{"tableName":"test"}
        ]
      },
      {
        "name": "streaming.core.compositor.spark.SQLCompositor",
        "params": [{"sql":"select a from test"}
        ]
      },
      {
        "name": "streaming.core.compositor.RDDPrintOutputCompositor",
        "params": [
          {
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

上面的配置相当于完成了如下的一个流程：

1. 从Kafka消费数据
2. 将Kafka数据转化为表
3. 通过SQL进行处理
4. 打印输出

是不是很简单，而且还可以支持热加载，动态添加job等

## 特性

该实现的特性有：

1. 配置化
2. 支持多Job配置
3. 支持各种数据源模块
4. 支持通过SQL完成数据处理
5. 支持多种输出模块

未来可扩展的支持包含：

1. 动态添加或者删除job更新，而不用重启Spark Streaming
2. 支持Storm等其他流式引擎
3. 更好的多job互操作

## 配置格式说明

该实现完全基于[ServiceframeworkDispatcher](https://github.com/allwefantasy/ServiceframeworkDispatcher) 完成，核心功能大概只花了三个小时。

这里我们先理出几个概念：

1. Spark Streaming 定义为一个App
2. 每个Action定义为一个Job.一个App可以包含多个Job

配置文件结构设计如下：

```
{

  "job1": {
    "desc": "测试",
    "strategy": "streaming.core.strategy.SparkStreamingStrategy",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "streaming.core.compositor.kafka.MockKafkaStreamingCompositor",
        "params": [
          {
            "metadata.broker.list":"xxx",
            "auto.offset.reset":"largest",
            "topics":"xxx"
          }
        ]
      } ,  
    ],
    "configParams": {
    }
  }，
  "job2"：{
   ........
 } 
}
```

一个完整的App 对应一个配置文件。每个顶层配置选项，如job1,job2分别对应一个工作流。他们最终都会运行在一个App上(Spark Streaming实例上)。

* strategy 用来定义如何组织 compositor,algorithm, ref 的调用关系
* algorithm作为数据来源
* compositor 数据处理链路模块。大部分情况我们都是针对该接口进行开发
* ref 是对其他job的引用。通过配合合适的strategy，我们将多个job组织成一个新的job
* 每个组件( compositor,algorithm, strategy) 都支持参数配置

上面主要是解析了配置文件的形态，并且[ServiceframeworkDispatcher](https://github.com/allwefantasy/ServiceframeworkDispatcher) 已经给出了一套接口规范，只要照着实现就行。

## 模块实现

那对应的模块是如何实现的？本质是将上面的配置文件，通过已经实现的模块，转化为Spark Streaming程序。


以SQLCompositor 的具体实现为例：

```
class SQLCompositor[T] extends Compositor[T] {

  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

//策略引擎ServiceFrameStrategy 会调用该方法将配置传入进来
  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

// 获取配置的sql语句
  def sql = {
    _configParams(0).get("sql").toString
  }

  def outputTable = {
    _configParams(0).get("outputTable").toString
  }

//执行的主方法，大体是从上一个模块获取SQLContext(已经注册了对应的table),
//然后根据该模块的配置，设置查询语句，最后得到一个新的dataFrame.
// middleResult里的T其实是DStream,我们会传递到下一个模块，Output模块
//params参数则是方便各个模块共享信息，这里我们将对应处理好的函数传递给下一个模块
  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    var dataFrame: DataFrame = null
    val func = params.get("table").asInstanceOf[(RDD[String]) => SQLContext]
    params.put("sql",(rdd:RDD[String])=>{
      val sqlContext = func(rdd)
      dataFrame = sqlContext.sql(sql)
      dataFrame
    })
    middleResult
  }
}
```

上面的代码就完成了一个SQL模块。那如果我们要完成一个自定义的.map函数呢？可类似下面的实现：

```
abstract class MapCompositor[T,U] extends Compositor[T]{
  private var _configParams: util.List[util.Map[Any, Any]] = _
  val logger = Logger.getLogger(classOf[SQLCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  
  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val dstream = middleResult(0).asInstanceOf[DStream[String]]
    val newDstream = dstream.map(f=>parseLog(f))
    List(newDstream.asInstanceOf[T])
  }
  def parseLog(line:String): U
}

class YourCompositor[T,U] extends MapCompositor[T,U]{

 override def parseLog(line:String):U={
     ....your logical
  }
}
```

同理你可以实现filter,repartition等其他函数。

## 总结

该方式提供了一套更为高层的API抽象,用户只要关注具体实现而无需关注Spark的使用。同时也提供了一套配置化系统，方便构建数据处理流程，并且复用原有的模块，支持使用SQL进行数据处理。

## 广告

这个只是我们大系统的一小部分，愿意和我们一起进一步完善该系统么？欢迎加入我们(请私信我)
