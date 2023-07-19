# Chapter7 综合实践

> 王洲烽

## 7. 1 面试题

### 7.1.1 简述Hadoop小文件弊端



### 7.1.2 HDFS中DataNode挂掉如何处理？



### 7.1.3 HDFS中DataNode挂掉如何处理？



### 7.1.4 HBase读写流程？



### 7.1.5 MapReduce为什么一定要有Shuffle过程



### 7.1.6 MapReduce中的三次排序



### 7.1.7 MapReduce为什么不能产生过多小文件



### 7.1.8 hive外部表和内部表的区别



### 7.1.9 简述对Hive桶的理解？



### 7.1.10 HBase和Hive的区别？



### 7.1.11 简述Spark宽窄依赖



### 7.1.12  Hadoop和Spark的相同点和不同点



### 7.1.13 Spark为什么比MapReduce块？



### 7.1.14 说说你对Hadoop生态的认识



## 7.2 签到检测实战

需要使用 **MRJob** 对在线社交网络的数据集进行数据分析。

### 7.2.1 数据集

数据集包含用户的签到历史，其中每条记录的格式为“ userID，locID，check_in_time”，其中 userID (字符串类型)是用户的 ID，locID (字符串类型)是位置的 ID，check _ in _ time (字符串类型)是用户在该位置签到的时间戳。示例文件如下:

```
u1,l1,t1
u1,l1,t2 
u1,l2,t3 
u2,l1,t4 
u2,l3,t5 
u3,l2,t6 
u3,l2,t7 
u3,l3,t8 
```

### 7.2.2 作业详情

我们用 $n_{\operatorname{loc}_i}^{u_j}$ 来表示 用户 $u_{j}$ 在位置 $loc_{i}$ 的签到次数，用 $n_{u_{j}}$ 来表示用户 $u_{j}$ 的签到总次数。因此，$n_{u_j}= \sum_{l o c_i \in L_{u_j}} n_{l o c_i}^{u_j}$ ，其中 $L_{u_{j}}$ 为 用户 $u_{j}$ 签到过的位置的集合。

用户 $u_{j}$ 在位置 $loc_{i}$ 的签到概率为 $prob_{l o c_i}^{u_j}=\frac{n_{l o c_i}^{u_j}}{n_{u_j}}$ 。您的任务是为每个用户计算该用户访问过的每个位置 的签到概率 $prob_{l o c_i}^{u_j}$。

**PS：解决方案源码填空示例与结果验证数据在`\juicy-bigdata\experiments\06 期中大作业`目录下**

**同时提前安装`MRJob`与`MRStep`包**

### 7.2.3 输出格式

将结果存储在 HDFS 中，格式为"$loc_i$\t $u_j,prob_{loc_i}^{u_j}$"。结果首先按升序按位置 ID 排序，然后按降序按用户的签到概率排序。如果两个用户具有相同的概率，则按照他们的 ID 以升序排序。

示例文件结果输出如下：

```
"l1"	"u1,0.6666666666666666"
"l1"	"u2,0.5"
"l2"	"u3,0.6666666666666666"
"l2"	"u1,0.3333333333333333"
"l3"	"u2,0.5"
"l3"	"u3,0.3333333333333333"
```

运行指令：`python project.py  -r hadoop <~/test_case/TKY_sample1000.csv > output`



> 希望大家带着轻松愉快的态度去学习，积极面对困难，持久化学习！！！

## 7.3 词频统计实战

从新闻文章中发现热门话题和趋势话题是舆论监督的一项重要任务。在这个项目中，你的任务是使用新闻数据集进行文本数据分析，使用 Python 中 pySpark 的 RDD 和 DataFrame 的 API。问题是计算新闻文章数据集中每年各词的权重，然后选择每年 top-**k**个最重要的词。

**PS：解决方案源码填空示例与结果验证数据在`\juicy-bigdata\experiments\10 期末大作业`目录下**

**同时提前安装`pyspark`包**

### 7.3.1 数据集

您将使用的数据集包含多年来发布的新闻标题数据。在这个文本文件中，每一行都是一篇新闻文章的标题，格式为“ date,term1 term2... ...”。日期和文本用逗号分隔，文本用空格字符分隔。示例文件如下:

```
20030219,council chief executive fails to secure position
20030219,council welcomes ambulance levy decision
20030219,council welcomes insurance breakthrough
20030219,fed opp to re introduce national insurance
20040501,cowboys survive eels comeback
20040501,cowboys withstand eels fightback
20040502,castro vows cuban socialism to survive bush
20200401,coronanomics things learnt about how coronavirus economy
20200401,coronavirus at home test kits selling in the chinese community
20200401,coronavirus campbell remess streams bear making classes
20201015,coronavirus pacific economy foriegn aid china
20201016,china builds pig apartment blocks to guard against swine flu
```

### 7.3.2 文本权重计算

您需要忽略诸如“ to”、“ the”和“ in”之类的停用词。该文件存储了停用词。

为了计算一个文本的为期一年的权重，请使用 TF/IDF 模型。具体而言，TF 和 IDF 可计算为:

$TF(文本 t, 年份 y) = 在 y 年份中，包含文本 t 的新闻标题数量$

$IDF(文本 t,数据集 D) = log10 (在数据集D中年份的总数/含有文本t的年份总数) $

最后，文本 t 对于年份 y 的权重计算如下:

$Weight(文本 t, 年份 y, 数据集 D) = TF(文本 t, 年份 y)* IDF(文本 t, 数据集 D) $

请使用 `import math` 并使用 `math.log10()`计算文本权重，并将结果四舍五入到小数点后6位。

### 7.3.3 输出格式

如果数据集中有 N 年，那么您应该在最终输出文件中输出正好 N 行，并且这些行按年份升序排序。在每一行中，您需要以`<term, weight> `的格式输出 k 对list，并且这些对按照文本权重降序排序。如果两个文本具有相同的权重，则按字母顺序对它们进行排序。具体来说，每行的格式类似于: 
“year**\t** Term1,Weight1;Term 2,Weight2;… …;Termk,Weightk” 。例如，给定上述数据集和 **k** = 3，输出应该是:

```
2003 council,1.431364;insurance,0.954243;welcomes,0.954243
2004 cowboys,0.954243;eels,0.954243;survive,0.954243
2020 coronavirus,1.908485;china,0.954243;economy,0.954243
```

### 7.3.4 运行指令

**Dataframe:**

```
spark-submit project_df.py file:///testcase_top_20/sample.txt file:///res_df file:///stopwords.txt k
```

**RDD:**

```
spark-submit project_rdd.py file:///testcase_top_20/sample.txt file:///res_rdd file:///stopwords.txt k
```

其中 `file://`后跟随本地文件目录地址。



> 看到这里的小伙伴们，给坚持这么久的自己点个赞鼓鼓掌，希望你能永远保持开源学习的动力，这也是我们的初衷。