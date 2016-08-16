> StreamingPro目前已经涵盖流式/批处理，以及交互查询三个领域，实现配置和SQL化

## 前言

今天介绍利用 [StreamingPro](https://github.com/allwefantasy/streamingpro) 完成批处理的流程。


## 准备工作

* 下载StreamingPro

[README中有下载地址](https://github.com/allwefantasy/streamingpro)

 我们假设您将文件放在了/tmp目录下。

## 填写配置文件

* 实例一,我要把数据从ES导出到HDFS,并且形成csv格式。

[gist](https://gist.github.com/allwefantasy/5dc8f994499ee3053623a3023fae79de) 


## 启动StreamingPro

Local模式：

```
cd  $SPARK_HOME

./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name test \
/tmp/streamingpro-0.2.1-SNAPSHOT-dev-1.6.1.jar    \
-streaming.name test    \
-streaming.platform spark   \
-streaming.job.file.path file:///tmp/test.json
```

访问

```
http://127.0.0.1:4040
```
可进入Spark UI


集群模式：


```
cd  $SPARK_HOME

./bin/spark-submit   --class streaming.core.StreamingApp \
--master yarn-cluster \
--name test \
/tmp/streamingpro-0.2.1-SNAPSHOT-dev-1.6.1.jar    \
-streaming.name test    \
-streaming.platform spark   \
-streaming.job.file.path hdfs://cluster/tmp/test.json
```

这里需要注意的是，配置文件并蓄放到HDFS上，并且需要协商hdfs前缀。这是一个标准的Spark 批处理程序
