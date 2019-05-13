# 可交互式分析的Spark Shell

## 基础使用

Spark的shell可以方便我们去学习API，也是一个强有力的数据分析交互式工具。在spark根目录下运行下面代码：

```shell
./bin/pyspark
```

Spark的主要抽象是一种被称为Dataset的分布式集合。你可以从`Hadoop InputFormats`（如`HDFS文件`）创建Datasets，或者从其它Datasets转化（transform）。由于Python动态的本质，我们不需要Dataset是个强类型的。所以，在Python中，所有的Datasets都是Dataset[Row]，我们称之为DataFrame，在`Pandas`和`R`中也这样称呼。现在，在打开的shell中，我们读取Spark根目录下的README文件来创建一个`DataFrame`。

```python
>>> textFile = spark.read.text("README.md")
```

你可以从DataFrame直接获得数据，通过调用actions或者transform来获取一个新的DataFrame。更多细节，请参阅[API doc](http://spark.apache.org/docs/latest/api/python/index.html#pyspark.sql.DataFrame)。

```python
>>> textFile.count()  # Number of rows in this DataFrame
126

>>> textFile.first()  # First row in this DataFrame
Row(value=u'# Apache Spark')
```

现在，我们通过transform函数来获取一个新的DataFrame，我们使用`filter`来过滤一些行。

```python
>>> linesWithSpark = textFile.filter(textFile.value.contains("Spark"))
```

也可以把transforms和actions操作合并成一个链式调用。

```python
>>> textFile.filter(textFile.value.contains("Spark")).count()  # How many lines contain "Spark"?
15
```

## DataSet的操作

使用Dataset的action和transform来完成复杂计算，比如，找出整篇文档出现最多单词的一行的单词个数。

```python
>>> from pyspark.sql.functions import *
>>> textFile.select(size(split(textFile.value, "\s+")).name("numWords")).agg(max(col("numWords"))).collect()
[Row(max(numWords)=15)]
```

首先找出每一行有多少个单词，这是一个整数，我们把它取名为`numWords`，`textFile.select`返回一个DataFrame实例。接着调用DataFrame的`agg`方法，找出最大numWords行。`select`和`agg`的参数都是[Column](http://spark.apache.org/docs/latest/api/python/index.html#pyspark.sql.Column)，我们可以用df.colName获取一列(df是一个DataFrame实例)。我们导入了`pyspark.sql.functions`，它提供了很多便捷函数，帮助我们更好的构建一个新的或者老的Column。

一种常见数据流模式是`MapReduce`，由Hadoop推广。Spark也可以很容易地实现MapReduce流：

```python
>>> wordCounts = textFile.select(explode(split(textFile.value, "\s+")).alias("word")).groupBy("word").count()
```

这里，我们在select语句中使用`explode`函数，把一个行Dataset转化成词Dataset，然后将`groupBy`和`count`结合起来，将文件中的每个单词计数计算为一个包含两列的DataFrame："word"和"count"。使用`collect`方法收集所有单词计数数据。

```python
>>> wordCounts.collect()
[Row(word=u'online', count=1), Row(word=u'graphs', count=1), ...]
```

## Caching

Spark还支持把数据集拉入集群范围内的内存缓存中。当一份数据被重复访问，比如查询一个小的热点数据集或者跑一个迭代算法(如`PageRank`)，缓存将会非常有用。一个简单的例子，缓存`linesWithSpark`dataset。

```python
>>> linesWithSpark.cache()

>>> linesWithSpark.count()
15

>>> linesWithSpark.count()
15
```

使用Spark浏览和缓存一个100行文本看起来似乎很愚蠢。有趣的是，这些类似的函数可以用于非常大的数据集，即使它们跨了数十个或数百个节点。你也可以在shell里连接一个集群来做这些，更多请参阅[RDD编程指南](../RDDs/README.md)。

