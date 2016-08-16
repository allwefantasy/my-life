>  Spark DataSource API 的提出使得各个数据源按规范实现适配，那么就可以高效的利用Spark 的计算能力。典型如Parquet,CarbonData,Postgrep(JDBC类的都OK)等实现。本文则介绍如何利用Spark DataSource 对标准Rest接口实现读取

## 引子

先说下这个需求的来源。通常在一个流式计算的主流程里，会用到很多映射数据，譬如某某对照关系，而这些映射数据通常是通过HTTP接口暴露出来的,尤其是外部系统，你基本没有办法直接通过JDBC去读库啥的。

上面是一个点，其次是从HTTP读到的JSON数据，我其实需要做扁平化处理的。现在如果SQL作用于JSON数据可以解决简单的嵌套问题，但是更复杂的方式是没有太大办法的。

比如下面格式的：

```
{
  "status":"200",
  "data":[
   "id":1,
   "userid":2,
   "service":{
    "3":{"a":1,"b":2},
    "2":{"a":3,"b":2},
    .....
  }

]
}
```
最好能展开成这种格式才能够被主流程直接join使用：

```
 {id:1,userid:2,service:3,a:1,b:2}
 {id:1,userid:2,service:2,a:3,b:2}
```

所以为了实现同事的需求，我需要第一将Rest接口的获取方式用标准的DataSource API 来实现，其次提供一个能够做如上合并规则的模块，并且允许配置。

最后实现的效果参看： [Rest DataSource](https://gist.github.com/allwefantasy/42fd5afa2dc76f7bcc56e9e72977eaa3)

实现代码可以参看：[RestJSONDataSource](https://github.com/allwefantasy/streamingpro/tree/master/src/main/java/org/apache/spark/sql/execution/datasources/rest/json)

## 实现目标

先看看DataSource API 的样子：

```
val df = SQLContext.getOrCreate(sc).
read.
format("driver class").//驱动程序，类似JDBC的 driver class 
options(Map(....)). //你需要额外传递给驱动的参数
load("url")//资源路径
```

如果做成配置化则是：

```
{
        "name": "streaming.core.compositor.spark.source.SQLSourceCompositor",
        "params": [
          {
            "format": "org.apache.spark.sql.execution.datasources.rest.json",
            "url": "http://[your dns]/path",
            "xPath": "$.data"
          }
        ]
      }
```

## DefaultSource的实现

定义

```
org.apache.spark.sql.execution.datasources.rest.json.DefaultSource
extends RelationProvider 
with DataSourceRegister
```

这是比较典型的命名规范。rest 代表支持的是rest作为接口，json则代表rest接口的数据是json格式的，包的命名让人一目了然。

先看看DefaultSource继承的两个接口

* DataSourceRegister

该接口只有一个`shortName` 方法。我们看到上面的包名是很长的，你可以给一个更简短的名字：

```
org.apache.spark.sql.execution.datasources.rest.json 
==>
restJSON
```

所以具体实现就变成了：

```
override def shortName(): String = "restJSON"
```

* RelationProvider

这个接口也只有一个方法：

```
def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation
```

其返回值BaseRelation对象描述了数据源和Spark SQL交互。createRelation方法允许你根据用户定义的参数`parameters ` 创建一个合适的BaseRelation的实现类。

其实除了这个，还有一些携带更多信息的继承自RelationProvider的类，譬如：

```
SchemaRelationProvider 允许你直接传递Schema信息给BaseRelation实现。
HadoopFsRelationProvider  除了参数帮你加了path等，返回值也帮你约定成HadoopFsRelation. HadoopFsRelation 提供了和HDFS交互的大部分实现
```

在我们的实现里，只要实现基础的RelationProvider就好。

我们来看下DefaultSource.createRelation的具体代码：

```
override def createRelation(
                               sqlContext: SQLContext,
                               //还记的DataSource的options方法么，parameters就是
                               //用户通过options传递过来的
                               parameters: Map[String, String]
                               ): BaseRelation = {
//因为我们并需要用户提供schema
//而是从JSON格式数据自己自己推导出来的
// 所以这里有个采样率的概念
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)
// 还记得DataSource的 path么？ 理论上是应该通过那个传递过来的，然而
//这里是直接通过potions传递过来的。
    val url = parameters.getOrElse("url", "")
// 我们需要能够对通过XPATH语法抽取我们要的数据，比如
//前面的例子，我们需要能够抽取出data那个数组
    val xPath = parameters.getOrElse("xPath", "$")
   //这里是核心
    new RestJSONRelation(None, url, xPath, samplingRatio, None)(sqlContext)
  }
```

源码中已经做了说明。这里RestJSONRelation是整个核心，它实现了Spark SQL 和数据源的交互。RestJSONRelation继承自BaseRelation，TableScan等基类

## RestJSONRelation

先看看RestJSONRelation 的签名：

```
private[sql] class RestJSONRelation(
                           val inputRDD: Option[RDD[String]],
                           val url: String,
                           val xPath: String,
                           val samplingRatio: Double,
                           val maybeDataSchema: Option[StructType]
                           )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {
```

这些参数是你随便定义的。当然，url,xPath,smaplingRatio具体的含义在上一章节都提到了。

和数据源进行交互有两个必要的信息需要获取：

* Schema 信息。只有两种方法：用户告知你，或者程序自己根据数据推导。关于schema信息这块，BaseRelation还提供了几个基础的约定：

    * needConversion，是否需类型转换，因为Spark SQL内部的表示是Row,里面的数据需要特定的类型，比如String会被转化成UTF8String。默认为true,官方也是说不要管他就好。
    * unhandledFilters, 返回一些数据源没有办法pushdown的filter。这样解析器就知道可以在Spark内部做filter了。否则Spark 会傻傻的以为你做了过滤，然后数据计算结果就错了。
    
* 数据扫描的方法。 目前Spark SQL 提供了四种

     * TableScan 全表扫描
     * PrunedScan  可以指定列，其他的列数据源可以不用返回
     * PrunedFilteredScan 指定列，并且还可以加一些过滤条件，只返回满足条件的数据。这个也就是我们常说的数据源`下沉(pushdown)`操作。
     *  CatalystScan 和PrunedFilteredScan类似，支持列过滤，数据过滤，但是接受的过滤条件是Spark 里的Expression。 理论上会更灵活些。话说在Spark源码)里(1.6.1版本)，我没有看到这个类的具体实现案例。
这里我们只要实现一个简单的TableScan就可以了，因为拿的是字典数据，并不需要做过滤。

## Schema推导

BaseRelation是需要你给出Schema的。这里我们会先定义一个dataSchema的lazy属性，这样防止schema方法被反复调用而反复推导。

```
override def schema: StructType = dataSchema
lazy val dataSchema = .....
```

因为我们是根据数据推导Schema,所以首先要获取数据。我们定义一个方法：

```
private def createBaseRdd(inputPaths: Array[String]): RDD[String]
```

inputPaths 我沿用了文件系统的概念，其实在我们这里就是一个URL。我们知道，最终Spark SQL 的直接数据源都是RDD的。所以这里我们返回的也是RDD[String]类型。具体实现很简单，就是通过HttpClient根据inputPaths拿到数据之后makeRDD一下就可以了。

```
//应该要再加个重试机制就更好了
private def createBaseRdd(inputPaths: Array[String]): RDD[String] = {
    val url = inputPaths.head
    val res = Request.Get(new URL(url).toURI).execute()
    val response = res.returnResponse()
    val content = EntityUtils.toString(response.getEntity)
    if (response != null && response.getStatusLine.getStatusCode == 200) {
      //这里是做数据抽取的，把data的数组给抽取出来
      import scala.collection.JavaConversions._
      val extractContent = JSONArray.fromObject(JSONPath.read(content, xPath)).
        map(f => JSONObject.fromObject(f).toString).toSeq
      sqlContext.sparkContext.makeRDD(extractContent)
    } else {
      sqlContext.sparkContext.makeRDD(Seq())
    }
  }
```

有了这个类就能获取到数据，就可以做Schema推导了：

```
 lazy val dataSchema = {
   //我们也允许用户传递给我们Schema,如果没有就自己推导
    val jsonSchema = maybeDataSchema.getOrElse {      
      InferSchema(
        //拿到数据
        inputRDD.getOrElse(createBaseRdd(Array(url))),
       //采样率，其实就是拿sc.sample方法
        samplingRatio,
        sqlContext.conf.columnNameOfCorruptRecord)
    }
    checkConstraints(jsonSchema)

    jsonSchema
  }
```

InferSchema的实现逻辑比较复杂，但最终就是为了返回StructType(fields: Array[StructField]) 这么个东西。我是直接拷贝的spark JSON DataSource的实现。有兴趣的可以自己参看。StructType其实也很简单了，无非就是一个描述Schema的结构，类似你定义一张表，你需要告诉系统字段名称，类型，是否为Null等一些列信息。

现在我们终于搞定了数据表结构了。

## 数据获取

刚才我们说了数据获取的四种类型，我们这里使用的是TableScan,继承自该接口只要实现一个buildScan方法就好：

```
def buildScan(): RDD[Row] = {
    JacksonParser(
      inputRDD.getOrElse(createBaseRdd(Array(url))),
      dataSchema,      sqlContext.conf.columnNameOfCorruptRecord).asInstanceOf[RDD[Row]]
  }
```

其本质工作就是把JSON格式的String根据我们前面已经拿到的Schema转化为Row格式。

具体做法如下：

```
//这个是createBaseRDD返回的RDD[String]
//对应的String 其实是JSON格式
//针对每个分区做处理
json.mapPartitions { iter =>
      val factory = new JsonFactory()
      iter.flatMap { record =>
        try {
          //JSON的解析器
          val parser = factory.createParser(record)
          parser.nextToken()
         //这里开始做类型转换了
          convertField(factory, parser, schema) match {
            case null => failedRecord(record)
            case row: InternalRow => row :: Nil
            case array: ArrayData =>
              if (array.numElements() == 0) {
                Nil
              } else {
                array.toArray[InternalRow](schema)
              }
            case _ =>
              sys.error(
                s"Failed to parse record $record. Please make sure that each line of the file " +
                  "(or each string in the RDD) is a valid JSON object or an array of JSON objects.")
          }
        } catch {
          case _: JsonProcessingException =>
            failedRecord(record)
        }
      }
    }
```

这里的代码还是比较清晰易懂的。但是 convertField(factory, parser, schema) 直接match 到  InternalRow 还是比较让人困惑的，一个字段转换咋就变成了InternalRow了呢？这里确实也有乾坤的。我们进去看看convertField方法：

```
 private[sql] def convertField(
      factory: JsonFactory,
      parser: JsonParser,
      schema: DataType): Any = {
    import com.fasterxml.jackson.core.JsonToken._
    (parser.getCurrentToken, schema) match {
      case (null | VALUE_NULL, _) =>
        null

      case (FIELD_NAME, _) =>
        parser.nextToken()
        convertField(factory, parser, schema)

     .....
     case (START_OBJECT, st: StructType) =>  
       convertObject(factory, parser, st)
```

如果你的JSON是个Map,经过N次匹配case后会进入最后一个case 情况。这里的st:StructType 就是我们之前自己推导出来的dataSchema. convertObject 方法如下：

```
 while (nextUntil(parser, JsonToken.END_OBJECT)) {
      schema.getFieldIndex(parser.getCurrentName) match {
        case Some(index) =>
          row.update(index, convertField(factory, parser, schema(index).dataType))

        case None =>
          parser.skipChildren()
      }
    }
```

到这里就真相大白了。为了能够拿到一条完整的数据，他会while循环直到遇到END_OBJECT 。所谓END_OBJECT 其实就是一个Map 结束了。 在每一次循环里，拿到一个字段，然后通过名字去schema里获取类型信息，然后再回调convertField方法将这个字段转化为row需要的类型，比如字符串类型的就通过UTF8String进行转换。

```
case (VALUE_STRING, StringType) =>  UTF8String.fromString(parser.getText)
```

得到的值通过Row的函数进行更新,这里是 row.update 方法。到END_OBJECT后，就完成了将一个JSON Map 转化为一条Row的功能了。

## 收工

到目前为止，我们已经完成了具体的工作了。现在你已经可以按如下的方式使用：

```
val df = SQLContext.getOrCreate(sc).
read.
format("org.apache.spark.sql.execution.datasources.rest.json").//驱动程序，类似JDBC的 driver class 
options(Map(
"url"->"http://[your dns]/path"
"xPath" -> "$.data"
)). //你需要额外传递给驱动的参数
load("url")//资源路径
```

获取到的Dataframe 你可以做任意的操作。

## 总结

Spark DataSource API的提出，给Spark 构建生态带来了巨大的好处。各个存储系统可以实现统一标准的接口去对接Spark。学会使用自己实现一个DataSoure是的你的存储可以更好的和生态结合，也能得到更好的性能优化。
