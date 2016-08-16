>今天基本算是完成了一个类似spark-jobserver 的功能，当然功能还是比较简单的，不过提供了Web界面。很感慨Spark 用好了，真的是大数据的瑞士军刀

![](http://upload-images.jianshu.io/upload_images/1063603-37409ea2078155e5?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

依托于Spark Streaming /Spark SQL，封装了一套通过配置和SQL就能完成批处理和流式处理的引擎，这样可以很好的完成复杂的ETL处理过程，实现了数据的流转和变换。

完成了数据的流转和变换，接着就是查询了，通过对Spark SQL的封装，我现在可以任意指定多个数据源，举个例子，将ES的索引A` 命名为表A,将来自HDFS 的Parquet 文件命名为表B,这个时候我就可以写SQL作任意的处理了。用户要做的就是选择对应数据来源，接着就是完成SQL就好。

能实现上面的功能得益于Spark

* 统一易用的API，比如RDD/DF/DS
* 功能丰富的组件，比如流式计算/批处理，机器学习,强大的SQL支持

Spark 背后的Databricks公司是我见过最重视

* 用户API设计
* 对领域问题具有高度抽象和设计能力

API 我就不说了，Spark的用户层API都是经过精心设计的，RDD自然不必说，上层的DF/DS 已经很好用，在2.0又更进一步统一了DF/DS (DF 是DS 类型为Row的一个特例)，这样可以让用户进一步减少使用和理解障碍。而且机器学习相关的API 也要慢慢迁移到 DF/DS ,进一步简化用户学习和使用成本。

对领域问题的高度抽象能力，我觉得给我特别印象深刻的是机器学习相关的，几经发展，目前形成了一套完善的ML-Pipelines 的东西，结果是啥呢？ 机器学习通过抽象以下几个概念

 *  Estimator
 *  Transformer
 *  Pipeline
 * Parameter
 * DataFrame

实现了模块化。基于之上，你可以实现配置化来完成机器学习流程。

大数据现阶段在我目前看来从功能上可划分数据处理和机器学习。从架构上而言，则是流式计算和批处理。 Spark 目前的组件已经涵盖了大部分你需要的东西。加上上面我提及的几点，用好了，你会觉得很多事情变得很简单了。

微信链接： [让Spark成为你的瑞士军刀](http://mp.weixin.qq.com/s?__biz=MzIyNzQyNzgxNQ==&mid=2247483652&idx=1&sn=b787f0671ca87b7d94aeb40002047536#rd)
