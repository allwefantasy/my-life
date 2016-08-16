## 准备工作

* 下载[Spark 1.6.2](http://d3kbcqa49mib13.cloudfront.net/spark-1.6.2-bin-hadoop2.6.tgz) 

* 下载[StreamingPro]( http://share.weiyun.com/b53c132ba8a5a97a9242810f0bf6a692)

我们假设你下载的StreamingPro包在/tmp目录下。

## 复制如下模板

```
{
  "esToCsv": {
    "desc": "测试",
    "strategy": "streaming.core.strategy.SparkStreamingStrategy",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "streaming.core.compositor.spark.source.SQLSourceCompositor",
        "params": [
          {
            "format": "org.elasticsearch.spark.sql",
            "path": "索引名称",
            "es.nodes": "这里是填写集群地址哈",
            "es.mapping.date.rich": "false"
          }
        ]
      },
      {
        "name": "streaming.core.compositor.spark.transformation.JSONTableCompositor",
        "params": [
          {
            "tableName": "table1"
          }
        ]
      },
      {
        "name": "streaming.core.compositor.spark.transformation.SQLCompositor",
        "params": [
          {
            "sql": "select * from table1"
          }
        ]
      },
      {
        "name": "streaming.core.compositor.spark.output.SQLOutputCompositor",
        "params": [
          {
            "format": "com.databricks.spark.csv",
            "path": "file:///tmp/csv-table1",
            "header": "true",
            "inferSchema": "true"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```

假设该文件所在路径是  /tmp/esToCSV.json。

## 本机运行

```
cd  $SPARK_HOME

./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name test \
/tmp/streamingpro-0.3.2-SNAPSHOT-online-1.6.1.jar    \
-streaming.name test    \
-streaming.platform spark   \
-streaming.job.file.path file:// /tmp/esToCSV.json
```

## 在集群运行

```
cd  $SPARK_HOME

./bin/spark-submit   --class streaming.core.StreamingApp \
--master yarn-cluster\
--name test \
/tmp/streamingpro-0.3.2-SNAPSHOT-online-1.6.1.jar    \
-streaming.name test    \
-streaming.platform spark   \
-streaming.job.file.path hdfs://clusternameAndPort/tmp/esToCSV.json
```
