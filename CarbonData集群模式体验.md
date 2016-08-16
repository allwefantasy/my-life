> 官方提供了一个快速上手的 [Quick-Start](https://github.com/apache/incubator-carbondata/blob/master/docs/Quick-Start.md) ，不过是采用spark-shell local模式的。我这里在实际集群环境做了下测试，并且记录了下过程，希望对大家有所帮助。

## 前言
之前专门写过一篇CarbonData的文章；[由CarbonData想到了存储和计算的关系](http://www.jianshu.com/p/2c30fd1beadf)。可惜碍于时间问题到现在才开始真正的尝试。

## 编译打包

截止到本文章发出,CarbonData 明确支持的Spark版本是 1.5.2(默认) 以及 1.6.1。 而相应的，hadoop版本有2.2.0 和 2.7.2，理论上大部分2.0 之后的hadoop版本应该都是兼容的。

* 下载源码：

```
git clone https://github.com/apache/incubator-carbondata.git carbondata
```
*  安装 thrift （0.9.3）

> Note:
>Thrift 主要是用来编译carbon-format模块用的，里面都是一些thrift文件，需要生成java文件。其他一些版本应该也是可以的，比如我用的就是0.9版本

* 编译打包

打开pom.xml文件，然后找到<profiles>标签，然后加入

```
<profile>
      <id>hadoop-2.6.0</id>
      <properties>
        <hadoop.version>2.6.0</hadoop.version>
      </properties>
    </profile>
```
之后就可以指定hadoop 2.6.0 编译了。不过这个是可选项，如前所述，理论上大部分版本都是兼容的。

现在可以执行打包指令了：
```
cd carbondata
mvn package -DskipTests -Pspark-1.6.1 -Phadoop-2.6.0
```

我编译过很多次，都没遇到啥问题。如果有问题，不妨留言给我。这个时候你应该得到了carbondata的jar包了:

```
assembly/target/scala-2.10/carbondata_2.10-0.1.0-SNAPSHOT-shade-hadoop2.6.0.jar
```

* 依赖说明

CarbonData 现阶段依赖于Kettle 以及 Hive Metastore。 依赖于Kettle 是因为一些数据处理逻辑Kettle已经有实现(譬如多线程等)，而使用Hive Metastore 则是因为用Hive的人多。后面考虑会去除这些依赖，当前要体验的话，需要额外做些配置。

* Kettle plugins 


```
 cd carbondata 
 cp -r processing/carbonplugins/*  carbondata-kettle
 tar czvf carbondata-kettle.tar.gz carbondata-kettle
```

接着将这个包分发到各个Slave节点上(hadoop集群上)，假定最后的目录是：

```
/data/soft/lib/java/carbondata-kettle
```

配置完成后检查下，确保carbondata-kettle下有个.kettle 的隐藏目录，该目录有kettle.properties文件。各个Slave节点都会加载该配置文件

* Hive MetaStore 配置

首先下载一个[mysql-connector](http://central.maven.org/maven2/mysql/mysql-connector-java/5.1.35/),放到你准备提交Spark任务的机器上(有SPARK_HOME的机器上)的某个目录，比如我这里是：

```
  /Users/allwefantasy/Softwares/spark-1.6.1-bin-hadoop2.6/lib/mysql-connector-java-6.0.3.jar
```

然后将你的Hive 的hive-site.xml 文件拷贝到你的SPAKR_HOME/conf 目录下。conf 目录会被自动打包发送到集群上。另外一种选择是在提交的时候通过--files 指定hive-site.xml文件也是OK的，我们推荐第一种方式。

hive-site.xml文件一般会配置两个目录：

```
hive.exec.scratchdir
hive.metastore.warehouse.dir
```

你需要确保你之后需要运行的程序对着两个目录相应的权限。如果权限不足，程序会较为明显的告诉你问题所在，所以关注下命令行的输出即可。

## 运行CarbonData 

在 SPARK_HOME/lib 下还有三个datanucleus开头的包，我们也通过--jars 参数加上

```
./bin/spark-shell   \
--master yarn-client \
--num-executors 10 \
--executor-cores 3 \
--executor-memory 5G \
--driver-memory 3G \
--jars /Users/allwefantasy/CSDNWorkSpace/incubator-carbondata/assembly/target/scala-2.10/carbondata_2.10-0.1.0-SNAPSHOT-shade-hadoop2.6.0.jar,/Users/allwefantasy/Softwares/spark-1.6.1-bin-hadoop2.6/lib/datanucleus-api-jdo-3.2.6.jar,/Users/allwefantasy/Softwares/spark-1.6.1-bin-hadoop2.6/lib/datanucleus-core-3.2.10.jar,/Users/allwefantasy/Softwares/spark-1.6.1-bin-hadoop2.6/lib/datanucleus-rdbms-3.2.9.jar,/Users/allwefantasy/Softwares/spark-1.6.1-bin-hadoop2.6/lib/mysql-connector-java-5.1.35.jar
```

所以--jars 一共有五个包：

1. 我们编译好的carbondata_2.10-0.1.0-SNAPSHOT-shade-hadoop2.6.0.jar
2. 我们下载的 mysql-connector-java-5.1.35.jar
3. SPARK_HOME/lib/datanucleus-api-jdo-3.2.6.jar
4. SPARK_HOME/lib/datanucleus-core-3.2.10.jar
5. SPARK_HOME/lib/datanucleus-rdbms-3.2.9.jar

然后就运行起来了，进入spark shell。

## 构建CarbonContext 对象

```
import org.apache.spark.sql.CarbonContext
import java.io.File
import org.apache.hadoop.hive.conf.HiveConf

val cc = new CarbonContext(sc, "hdfs://xxx/data/carbondata01/store")
```

CarbonContext 的第二个参数是主存储路径，确保你设置的目录，spark-shell 启动账号是具有写入权限。通常我会做如下操作：

```
hdfs dfs -chmod 777  /data/carbondata01/store
```

一些表信息，索引信息都是存在该目录的。如果写入权限不足，load数据的时候，会出现如下的异常：

```
ERROR 05-07 13:42:49,783 - table:williamtable02 column:bkup generate global dictionary file failed
ERROR 05-07 13:42:49,783 - table:williamtable02 column:bc generate global dictionary file failed
ERROR 05-07 13:42:49,783 - table:williamtable02 column:bid generate global dictionary file failed
ERROR 05-07 13:42:49,783 - generate global dictionary files failed
ERROR 05-07 13:42:49,783 - generate global dictionary failed
ERROR 05-07 13:42:49,783 - main
java.lang.Exception: Failed to generate global dictionary files
	at org.carbondata.spark.util.GlobalDictionaryUtil$.org$carbondata$spark$util$GlobalDictionaryUtil$$checkStatus(GlobalDictionaryUtil.scala:441)
	at org.carbondata.spark.util.GlobalDictionaryUtil$.generateGlobalDictionary(GlobalDictionaryUtil.scala:485)
```



如果下次你在启动spark-shell或者提交新的应用时，需要保持这个路径(storePath)的不变，否则会出现表不存在的问题。类似：

```
AUDIT 05-07 16:12:10,889 - [allwefantasy][allwefantasy][Thread-1]Table Not Found: williamtable02
org.spark-project.guava.util.concurrent.UncheckedExecutionException: org.apache.spark.sql.catalyst.analysis.NoSuchTableException
	at org.spark-project.guava.cache.LocalCache$LocalLoadingCache.getUnchecked(LocalCache.java:4882)
	at org.spark-project.guava.cache.LocalCache$LocalLoadingCache.apply(LocalCache.java:4898)
	at org.apache.spark.sql.hive.HiveMetastoreCatalog.lookupRelation(HiveMetastoreCatalog.scala:394)
	at 
```

## 设置Kettle 相关

因为引入了Kettle的库，而该库需要在运行的服务器上读取一些配置文件(如kettle.properties),所以需要做一个配置。我们前面已经将kettle 分发到各个节点了，现在把路径要告诉Carbon,通过如下的方式：

```
cc.setConf("carbon.kettle.home","/data/soft/lib/java/carbondata-kettle")
```

如果这个目录在Slave节点不存在，你进入Spark 各个节点(Executor)的日志,可以看到很明显的错误，提示 kettle.properties 找不到。 而更明显的现象是，数据载入会不成功。


##  Hive  相关配置

理论上hive-site.xml的配置里已经有这些信息了，不过也可以显示设置下。

```
cc.setConf("hive.metastore.warehouse.dir", "hdfs://cdncluster/user/hive/warehouse")
cc.setConf(HiveConf.ConfVars.HIVECHECKFILEFORMAT.varname, "false")
```

## 生产数据

到目前为止 CarbonContext 已经设置完毕，可以往里面装载数据了。现阶段，CarbonData 支持CSV数据直接装载进CarbonData。

如果你已经有或者可以自己产生csv相关的数据，则可以忽略本节。

另外其实CarbonData 也提供了标准的Spark SQL API(Datasource)方便你导入数据，参看[Carbondata-Interfaces](https://github.com/HuaweiBigData/carbondata/wiki/Carbondata-Interfaces)。内部本质也是帮你把数据转化成csv然后再导入的：

```
def saveAsCarbonFile(parameters: Map[String, String] = Map()): Unit = {
      // To avoid derby problem, dataframe need to be writen and read using CarbonContext
      require(dataFrame.sqlContext.isInstanceOf[CarbonContext],
        "Error in saving dataframe to carbon file, must use CarbonContext to save dataframe"
      )

      val storePath = dataFrame.sqlContext.asInstanceOf[CarbonContext].storePath
      val options = new CarbonOption(parameters)
      val dbName = options.dbName
      val tableName = options.tableName

      // temporary solution: write to csv file, then load the csv into carbon
      val tempCSVFolder = s"$storePath/$dbName/$tableName/tempCSV"
      dataFrame.write
        .format(csvPackage)
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save(tempCSVFolder)
```

这里也介绍另外一种方式，以从ES导出数据为csv为例：

* 下载一个配置文件[配置文件](https://gist.github.com/allwefantasy/5dc8f994499ee3053623a3023fae79de),根据里面的要求进行修改

并且将修改后的配置上传到hdfs上。假设路径是： 

```
hdfs://cluster/tmp/test.json
```

*  下载一个jar包：

```
链接: http://pan.baidu.com/s/1bZWphO 密码: kf5y
```

* 提交到集群

```
./bin/spark-submit   \
--class streaming.core.StreamingApp   \
--name "es导出成csv文件"  \
--master yarn-cluster   \
--executor-memory 2G   \
--driver-memory 6G   \
--conf "spark.locality.wait=10ms"   \
--num-executors 35   \
--executor-cores 3  \\/Users/allwefantasy/CSDNWorkSpace/streamingpro/target/streamingpro-0.2.0-SNAPSHOT-online-1.6.1.jar  \
-streaming.name estocsvn \
-streaming.job.file.path hdfs://cluster/tmp/test.json \
-streaming.platform spark
```

这样你就生成了一个csv格式的数据

## 创建表

```
cc.sql("create table if not exists williamtable04 (sid string,  r double,time string,domain string,month Int,day Int,mid string) STORED BY 'org.apache.carbondata.format'")
```

貌似不支持float,需要用double类型。

## 装载CSV数据

```
cc.sql(s"load data inpath 'hdfs://cluster/tmp/csv-table1/part-00001.csv' into table williamtable04")
```
csv文件需要是.csv 为后缀，并且需要带有header。当然，如果你生成的csv文件没有header,也可以通过在load data时指定FIELDHEADER来完成。

## 查询

```
cc.sql("select count(*) from williamtable04").show
```


## 后话

因为现阶段CarbonData 依赖于Hive/Kettle,所以需要做一些额外配置，自身的配置已经足够简单，只需要个storePath。在集群环境里，我们还需要注意权限相关的问题。
