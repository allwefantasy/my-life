# 大数据技术栈- BI探秘


## 前言

原来BI这块打算很后面写的，先把前面的基础技术介绍清楚。但有不少人匿名评论说，我第一篇<a href="https://github.com/allwefantasy/my-life/blob/master/foundation-1.md">大数据技术栈-Web框架&消息队列</a>  和大数据无关，看着没意思。但实际不是这样子的，消息队列，你可以问问，是任何一家大一点的公司都需要的基础设施，我在原文中重点解释的是消息队列解决的问题，应用场景，带来的便利，并且建议有条件的，都应该引入。

这次就哗众取宠下，直接跳到BI这个产品上。不过后续我会沿着原来的路线写，继续介绍一些底层的系统，包括索引服务，Redis集群，数据网关等的使用经验。


## BI系统构成

BI 已经是很上层的一个东西了。BI系统本身可能很简单，MySQL+Web服务+Web报表组件 就搞定了。但里面涉及了比较多的概念，比如 指标。同时，如果要让BI正常运转起来，依赖的系统是比较多，我们姑且称之为配套系统吧(以BI为出发点)。

我们通过运转流程，来看看都依赖了那些系统：

1. 日志上报  (前端js或者产品服务)
2.  Nginx日志接收
3.  定时同步到HDFS
4.  产品和BI人员定义大量的计算指标
4.  BI人员将指标转化为实际的SQL统计
5.  将SQL 放入集群的任务管理,调度系统中。SQL之间会有依赖，定时等各种需求，所以这个系统很重要
6.  集群将计算结果导入到MySQL中
7.  BIWeb服务从MySQL中取出数据，通过Web报表组件渲染成各种图形界面


从上面我们可以看到，BI依赖：

1. 日志接收服务
2. 集群，核心是Hive
3. 集群任务管理，调度配置系统
4. BI系统展现部分

其中每个环节都有大量细节问题。

## 日志接收

通过Nginx接收的方式，目前是性能比较高的一种方式。然后定时切分，可以按天，也可以按小时，然后将数据同步到集群上。如果需要对日志做一些预处理，还可以后端再挂个日志服务，将Nginx请求打到该日志服务上。日志服务还有一个功能，可以筛选出一些有用的数据，将它放到消息队列中，从而使得这些数据能在整个系统中进行流转和处理。
这个后续ETL的系统我会详细介绍。

这里我们用到的工具很简单：

1. Nginx
2. 日志服务

产生的三种数据：

1. Nginx 日志文件
2. 日志服务预处理后的文件
3. 日志服务投递到消息队列的数据

接收服务比较繁琐的事情是要定义上报规范，这样各个产品才能进行上报。我们当时构建BI的时候的第一步，就是进行上报规范设计。

## 日志处理

日志接受后，除了放到消息队列的数据会被ETL等服务实时处理外，文件类的数据都会被上传到集群中。

集群里很核心的是Hive,允许使用SQL进行各种统计。本质上是将数据库的操作放到分布式环境中做。如果你数据小，也可以直接使用数据库，整套流程还是没有变化的。

那么这里有个问题，也是我们一开始就面临的问题，如果数据的格式千差万别，Hive怎么知道该如何解析呢？我们不可能去写大量的解析器，最好的方式，是有一个通用的解析器，我们可以通过JSON格式进行数据描述。


### 通用Hive日志解析器

比如，现在系统进来了一个新的格式的数据，那么我们只要用JSON写一个描述文件，然后Hive根据这个JSON格式描述文件，就知道怎么解析这个数据了。

比如我们系统中比较典型的格式描述：

	{
	    "sources": {
	                      //支持版本，存在某种情况，日志格式发生了变化，系统知道是用v0还是v1格式去解析
				    "v0": {
					  "type":"custom", //该字段为自定义格式
							  "ref":[
					          {
					              "name": "ip",
					              "type": "string",
								  "start": 26 
	 
					          },
					          {
					              "name": "agt",
					              "type": "string" 
					          },
					          {
					              "name": "curl",
					              "type": "string" 
					          },					        	         				   {
					              "name": "dt", //支持复杂的日期格式
					              "type": "time",
								  "original":"yyyy-MM-dd HH:mm:ss",
								  "target":"yyyyMMdd HH:mm:ss" 
					          }
							  ]
		     
							},
	        "v1": {
	            "type": "custom",
	            "ref": [
	                  {
	                    "name": "url", //这个功能很核心。比如用户上报了一条记录，记录里面包含了一个URL字段，
	                    //URL字段可以带非常多的参数，
	                    //这些参数我们想让他直接成为Hive里的一个字段，你只要这样定义，系统会自动给你打平。
	                    //我们看到，这种定义字段也是支持版本的。
	                    "type": "custom",
	                    "ref": {
	                        "v0": {
	                            "uid": {
	                                "default": "-",
									"name":"user_name" 
	                            },
	                            "ref": {
	                                "type": "string",
									"name":"referrer"
	                            },
	                            "pid": {
	                                "type": "string"                                
	                            },
	                         "tos": {
	                                "type": "string" 
	                            },
				    "sid": {
					"name": "session_id",
	                                "type": "string" 
	                            }
	                        }
	                    }
	                },
	                {
	                    "name": "curl",
	                    "type": "string" 
	                },
	                {
	                    "name": "agt",
	                    "type": "string" 
	                }
	            ]           
	        }
	    },
	    "array_splitter":"\t",   //可以支持多级分割
		"second_array_splitter":"\\u0000"
	}



除了可以配置按普通按分隔符分割的，同时也支持URL参数解析，JSON格式解析映射。非常方便，基本上有了这套系统，一个数据过来，BI人员完全不需要去代码解析数据，只要写个配置文件，就能自动映射到Hive表，直接进行查询。
这里解决了一个比较蛋疼的问题。另外就是，通常一套BI,可能会几百上千的计算指标。那么需要的SQL任务会非常多，并且SQL任务有先后顺序，甚至结果存在互相依赖，那么如何解决这个问题呢？


###  Hive管理调度系统

一个Hive管理调度系统至少要满足下面几个因子：

1. Web化，我在界面直接操作就好了，这里采用的是ExtJS
2. 存储任务。我可以在系统添加很多SQL任务
3. 定时执行任务。我可以设置定时，什么时候执行什么任务
4. 设置任务依赖。任务存在依赖，一个任务的输出可能是另外一个任务的输入。

这套系统能让所有Task自动运转起来，每天只要有数据进来，就能根据SQL进行各项指标计算，将结果生成，进入到MySQL中。可以选择自己开发一套，也可以找些开源的项目。找的方式也比较简单，看看哪些公司使用了Hadoop集群
，他们一定需要这样的系统，做的好的，就会开源出来。

说到这个，我们还有一套系统和这个相关，也是任务调度的，但不是通过人工去配置的，而是通过HTTP接口向集群提交SQL任务。具体做法是：

1.  有一个Web服务接收HTTP请求
2.  Web服务将请求经过一定的处理，拼装成SQL
3.  将SQL放入到MySQL中，设置为未执行
4.  通过Hive管理调度系统定时去扫描MySQL里的SQL任务
5.  集群执行SQL任务并且更新MySQL里的任务

这其实是一个简单的集群任务发布系统。目前应用在比如EDM等项目中。


## BI展示服务

BI展示服务有两个职责：

1. 从MySQL获取已经计算好的指标
2. 通过合适的方式生成图形，报表


具体内容并没有太多可以说的，把它当做一个普通的Web项目做就可以了。



## 一个Hive计算实例

事实上，Hive除了可以计算各种指标，如果某种数据是常态，那么用Hive来进行定时数据生成也是非常有效的。

下面公式中，d 为文档，q 为query串，现在我们要形成如下格式的数据，用来做机器学习的语料。

    p(d|q) \t [query feature] [article feature]

p(d|q) 表示 q 出现时 d出现的概率。
p(d,q) 表示 q,d 同时出现的次数
p(q) 表示q 出现次数

计算公式为:
p(d|q)=p(d,q)/p(q)

我们已经有日志数据file_pv_track 以及 博客内容数据，
所以通过简单的sql语句就可以得到上面的结果:

	CREATE  TABLE IF NOT EXISTS temp_search_query (
	query_v STRING,
	blog_v  STRING
	);//创建临时表，用来存放 query 以及博客的向量组合

	INSERT OVERWRITE TABLE temp_search_query
	SELECT
	vec(extract_query_from_ref(fpt.ref),"14000_word",4000000,1), //vec,extract_query_from_ref 就是我们
	//自定义的函数了，基本作用就是形成一个14000维度的向量
	vec(concat(blog.title,blog.body),"14000_word",4000000,13398)
	FROM file_pv_track fpt join blog  ON (fpt.curl=blog.url)
	WHERE extract_query_from_ref(fpt.ref) is not null AND trim(extract_query_from_ref(fpt.ref))!="" ;

	CREATE  TABLE IF NOT EXISTS temp_query (
	query_v STRING,
	query_c  STRING
	);

	CREATE  TABLE IF NOT EXISTS temp_blog (
	query_v STRING,
	blog_v  STRING,
	q_b_c INT //query 和 blog内容共同出现的次数
	);

	CREATE  TABLE IF NOT EXISTS query_blog_vector (
	score DOUBLE,
	query_v STRING,
	blog_v  STRING,
	q_b_c INT, //query 和 blog内容共同出现的次数
	q_c INT//query 单独出现次数
	);

	CREATE  TABLE IF NOT EXISTS query_blog_vector_result (
	score DOUBLE,//结果数据表
	query_v STRING,
	blog_v  STRING
	)
	row format delimited
	fields terminated by ' ';

	INSERT OVERWRITE TABLE temp_query
	       SELECT query_v,count(query_v) from temp_search_query group by query_v;

	INSERT OVERWRITE TABLE temp_blog
	       SELECT query_v, blog_v, COUNT(query_v) FROM temp_search_query GROUP BY query_v, blog_v;

	INSERT OVERWRITE TABLE query_blog_vector
	SELECT  tb.q_b_c/tq.query_c , tb.query_v,tb.blog_v,tb.q_b_c,tq.query_c from temp_blog tb join temp_query tq on (tb.query_v=tq.query_v)
	where trim(tb.query_v)!="" AND trim(tb.query_v) is not null;

	INSERT OVERWRITE TABLE query_blog_vector_result
	SELECT  score,query_v,blog_v 
	FROM query_blog_vector;

	DROP TABLE  temp_query;
	DROP TABLE  temp_blog;
	

接着把数据放到SVM或者Spark里做线性回归计算就Ok了，能得到一个query和一篇博文的亲密度，也就是分数，方便做预测。