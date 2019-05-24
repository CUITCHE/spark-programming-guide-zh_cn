# 弹性分布式数据集RDDs

Spark围绕`弹性分布式数据集（RDD）`的概念展开，RDD是一个可以并行运算的容错元素集合。创建RDD有两种方法：并行化驱动程序中的现有集合，或引用外部存储系统中的数据集，例如共享文件系统、HDFS、HBase或提供Hadoop输入格式的任何数据源。

## 并行集合

`并行集合`通过调用`SparkContext.parallelize()`方法创建，入参是在驱动程序中现有的`iterable`或`集合`。入参的数据会被复制一份，形成可并行运算的分布式数据集。例如，下面介绍如何创建一个包含数字1到5的并行集合：

```python
data = [1, 2, 3, 4, 5]
distData = sc.parallelize（data）
```

一旦创建，分布式数据集（distData）就可以并行运算。例如，我们可以调用`distData.reduce(lambda a, b: a + b)`计算所有数字之和。稍后我们再介绍分布式数据集的运算。

并行集合一个重要参数就是`partitions`（分区）的数值，需要把数据集切分成几块。Spark将为集群的每个分区运行一个任务，通常，需要为集群中的每个CPU分配2-4个分区。但是，也可以通过将其作为第二个参数传递给parallelize（例如：sc.parallelize(data, 10)）来手动设置它。

代码中的某些地方使用术语slice（partition的同义词）来保持向后兼容性。（译者注：中文翻译的时候，我会以分区（partition）和切片（slice）来区分）

## 外部数据集

`PySpark`可以从Hadoop支持的任何存储源创建分布式数据集，包括本地文件系统、HDFS、Cassandra、HBase、Amazon S3等。Spark支持文本文件、序列文件（SequenceFiles）和任何其他Hadoop输入格式。

`sc.textFile`方法创建文本文件RDD，按照传入文件的URI（计算机上的本地路径或hdfs://、s3a://等URI）按行读取为集合。下面是一个调用例子：

```shell
>>> distFile = sc.textFile("data.txt")
```

创建后，就可以使用数据集运算对distFile进行运算了。比如，我们可以用`map`和`reduce`来统计每行的长度并把它们加起来：

```python
distFile.map(lambda s: len(s)).reduce(lambda a, b: a + b)
```

关于使用spark读取文件的一些注意事项：

- 如果在本地文件系统上使用路径，则该文件也必须可以在工作节点上的同一路径上可以被访问。要么将文件复制到所有worker上，要么使用网络安装的共享文件系统。
- Spark所有基于文件的输入访问，包括textFile，支持访问目录路径、压缩文件和通配符。例如，可以使用`textFile("/my/directory")`、 `textFile("/my/directory/*.txt")`和`textFile("/my/directory/*.gz")`。
- `textFile`方法通过第二个可选参数来控制文件的分区数。默认情况下，Spark为文件的每个块创建一个分区（HDFS中的块blocks默认是128MB），但你也可以通过传递较大的值来获取更多的分区。但请**注意**，分区不能少于blocks。

除了文本文件外，Spark的Python API还支持其他几种数据格式：

- `SparkContext.wholeTextFiles`读取一个目录下所有的文本文件，然后返回<文件名, 文件内容>对（Pair）。它不同于`textFile`，`textFile`是读取文件内容并按行返回。

- `RDD.saveAsPickleFile`和`SparkContext.pickleFile`支持以一种简单格式保存RDD，由pickle模块序列化python对象。批处理用于pickle序列化，默认批处理大小为10。

- `SequenceFiles`和`Hadoop`输入/输出格式

  **注意：**此功能当前标记为`实验性`，适用于高级用户。在将来，它可能会被基于`Spark SQL`的读/写支持替代，在这种情况下，`Spark SQL`是首选方法。

### Writable支持

`PySpark SequenceFile`支持在Java中加载一个键值对RDD，将可读写转换为基本的Java类型，并使用[Pyrolite](https://github.com/irmen/Pyrolite/)压缩（pickle）生成的Java对象。把一个键值对的RDD保存到SequenceFile时，PySpark执行相反的运算。它将python对象解压为Java对象，然后将他们转换为Writables。下面的`Writables`将会被自动转换：

| Writable Type   | Python Type |
| :-------------- | :---------- |
| Text            | unicode str |
| IntWritable     | int         |
| FloatWritable   | float       |
| DoubleWritable  | float       |
| BooleanWritable | bool        |
| BytesWritable   | bytearray   |
| NullWritable    | None        |
| MapWritable     | dict        |

`Arrays`不是开箱即用的。用户在读取或写入时，需要指定自定义的`ArrayWritable`子类型。写入时，用户需要指定自定义转换器，将数组转换为自定义的`ArrayWritable`子类型；读取时，默认转换器会把自定义的`ArrayWritable`子类型转换为Java`Object[]`，然后将其改为python元组。要获取python基础类型`array`，用户需要指定自定义转换器。

### 保存、加载SequenceFiles

与文本文件相似，可通过指定路径来保存和加载SequenceFiles。需要指定key和value的类，但对于标准`Writable`来说不是必需的。

```shell
>>> rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
>>> rdd.saveAsSequenceFile("path/to/file")
>>> sorted(sc.sequenceFile("path/to/file").collect())
[(1, u'a'), (2, u'aa'), (3, u'aaa')]
```

###保存、加载其它Hadoop输入/输出格式

PySpark也能用新的、老的Hadoop API读取/写入任何Hadoop输入/输出格式。如果需要，Hadoop配置可以作为python dict传入。下面是使用`Elasticsearch ESinputformat`的示例：

```shell
$ ./bin/pyspark --jars /path/to/elasticsearch-hadoop.jar
>>> conf = {"es.resource" : "index/type"}  # assume Elasticsearch is running on localhost defaults
>>> rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",
                             "org.apache.hadoop.io.NullWritable",
                             "org.elasticsearch.hadoop.mr.LinkedMapWritable",
                             conf=conf)
>>> rdd.first()  # the result is a MapWritable that is converted to a Python dict
(u'Elasticsearch ID',
 {u'field1': True,
  u'field2': u'Some Text',
  u'field3': 12345})
```

注意，如果InputFormat仅仅依赖于Hadoop配置和（或）输入路径，并且可以根据上述表格轻松地转换key-value，那么这种方法就没啥问题。

如果你有自定义的序列化二进制数据（例如从Cassandra / HBase加载数据），那么首先需要将Scala/Java侧上的数据转换为可由`Pyrolite’s pickler`处理的数据。为此，官方提供了 [Converter](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.api.python.Converter)特性。只需扩展这个特性并在`convert`方法中实现转换代码。记住，要确保这个类以及访问InputFormat所需的任何依赖项都要打包到Spark作业jar中去，并包含在pyspark类路径中。

请参阅 [Python examples](https://github.com/apache/spark/tree/master/examples/src/main/python) 和 [Converter examples](https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples/pythonconverters)的例子，它们用自定义的`convertors`访问Cassandra / HBase的输入/输出格式。

## RDD的运算

RDDs支持两种类型的运算：

- 转换（*transformations*），从一个数据集创建新的数据集。
- 处理（*actions*）<sup>[1]</sup>，在数据集上计算处理，并返回一个值。

比如，`map`是一种转换运算，它通过一个函数传递每个数据集的元素，并返回一个表示结果的新的RDD。另一方面，`reduce`是一种处理运算，它使用某个函数聚合RDD上的所有元素，并将最终结果返回给驱动程序（尽管还有一个并行`reduceByKey`返回分布式数据集）。

在Spark中，所有的转换都是惰性的（lazy），因为它们不会立即计算结果。相反，它们只**记住**应用于某些基本数据集（例如文件）的**转换**。只有当前运算要求将结果返回到驱动程序是，才会计算转换。这种设计使Spark能够更有效地运行。例如，可以意识到通过map创建的数据集将用于`reduce`，并且只将`reduce`的结果返回给驱动程序，而不是更大的映射数据集。（译者言：惰性运算省去了保存中间数据集的开销。有时不必全部计算一遍，可能会提前返回）

默认情况下，每次对每个已转换的RDD作处理时，都会触发重新计算。但是，也可以使用`persist`或`cache`方法在内存中保留RDD（即缓存），在这种情况下，Spark会在下次查询时会更快地访问集群上的元素。还支持在磁盘上持久化RDD，或跨多个节点复制RDD。

### 基础知识

要说明RDD基础知识，请考虑下面的示例程序：

```python
lines = sc.textFile("data.txt")
lineLengths = lines.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)
```

第一行，从外部文件创建了一个基本RDD。`lines`并没有加载'data.txt'文件到内存，它只是一个指针指向了文件。

第二行，定义了`lineLengths`，保存`map`转换的结果。再提一下，`lineLengths`并没有立即被计算，因为懒加载。

最后，我们运行`reduce`，这是一个处理运算。Spark将计算分解为在不同的机器上运行的任务，并且每台机器都执行`map`和`reduce`返回它计算的结果。

若想在后面继续用到`lineLengths`，那么可以把它缓存起来：

```python
lineLengths.persist()
```

注意是在`map`后`reduce`前调用。在进行`reduce`之前，这将导致在第一次计算后将`lineLengths`保存到内存中。

### 向Spark传递函数

Spark的API在很大程度上依赖于在集群上运行的驱动程序中传递函数。这里有三种方法可以做到：

- [Lambda表达式](https://docs.python.org/2/tutorial/controlflow.html#lambda-expressions)，可作为表达式写入的简单函数。（lambda不支持多语句函数或者不返回值的语句，Only in Python）
- Local functions，本地函数，在方法内部定义的函数。
- Top-Level functions，模块级别的函数。

例如，需要传入的函数比较复杂，就用函数（本例就是Local function），而不是lambda：

```python
"""MyScript.py"""
if __name__ == "__main__":
    def myFunc(s):
        words = s.split(" ")
        return len(words)

    sc = SparkContext(...)
    sc.textFile("file.txt").map(myFunc)
```

请注意，虽然也可以传递对类实例中某个方法的引用（与单例对象不同），但这需要将包含该类的对象与该方法一起发送。例如，这样：

```python
class MyClass(object):
    def func(self, s):
        return s
    def doStuff(self, rdd):
        return rdd.map(self.func)
```

在这里，如果我们创建了一个`MyClass`对象，并调用`doStuff`，`map`函数引用了`MyClass`对象的`func`方法，所以整个实例对象都将被发送至集群。

（译者言：**这里是个坑，实战中请留意下面两个例子**）

相似地，访问外部对象的字段将引用整个对象：

```python
class MyClass(object):
    def __init__(self):
        self.field = "Hello"
    def doStuff(self, rdd):
        return rdd.map(lambda s: self.field + s)
```

为了避免这种情况，简单的作法就是复制一份到局部变量中，而不是从外部访问它：

```python
def doStuff(self, rdd):
    field = self.field
    return rdd.map(lambda s: field + s)
```

### 理解闭包（Closure）

Spark的难点之一是在集群中执行代码时，了解变量和方法的范围和声明周期。在其范围之外修改变量的RDD操作可能是一种常见的混淆源。在下面的示例中，我们将看到使用`foreach()`递增计数器的代码，但其他操作也可能出现类似的问题。

#### 例子

考虑下面`天真`的RDD元素求和操作。根据是否在同一个JVM中执行，这些元素的行为可能会有所不同。一个常见的例子是，当以本地模式运行spark时（`--master=local[n]`），而不是将spark应用程序不熟到集群时（例如，通过spark-submit传输到YARN集群）。

```python
counter = 0
rdd = sc.parallelize(data)

# Wrong: Don't do this!!
def increment_counter(x):
    global counter
    counter += x
rdd.foreach(increment_counter)

print("Counter value: ", counter)
```

#### 本地 vs 集群

上述代码行为未定义，可能无法按预期工作。为了执行这个作业，Spark将RDD运算分解为多个任务，每个任务由执行器（Executor）执行。在执行之前，Spark计算任务的闭包。闭包是执行器在RDD上执行计算时必须可见的变量和方法（在本例中是`foreach()`）。这个闭包被序列化并发送给每个执行器。

发送给每个执行器的闭包中的变量现在是副本，因此，当`foreach`函数中引用`counter`时，它不再是驱动程序节点上的`counter`了。驱动程序节点的内存中仍有一个`counter`，但执行器将无法再看到它！执行器只能从序列化的闭包中看到副本。所以，`counter`的最终值仍然为0，因为`counter`上的所有操作都引用了序列化闭包中的值。

在本地模式下，在某些情况下，`foreach`函数将在与驱动程序相同的JVM中执行，引用相同的原始的`counter`，并且能确确实实地更新它。

为了确保这些场景中有正确的行为，应该使用累加器（[`Accumulator`](http://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators)）。Spark中的累加器专门用于提供一种机制，在集群中跨工作节点拆分执行时能安全地更新变量。本教程的**累加器**部分将更详细地讨论这些问题。

通常，闭包——类似于循环或本地定义的方法的结构，不应该改变某些全局的状态。Spark不定义或保证从闭包外部引用的对象的突变行为。执行此操作的某些代码可能在本地模式下可以很好的工作，但这只是偶然发生的，这样的代码在分布式模式下不会像预期的那样工作。如果需要一些全局的聚合，请改用累加器。

#### 打印RDD的元素

另一个常见的习惯用法是使用`rdd.foreach(println)`或 `rdd.map(println)`打印出RDD的元素。在一台机器上，这将生成预期的输出并打印所有RDD元素。但是，在集群模式下，执行器调用的stdout现在正在写入执行器的stdout，而不是驱动程序上的。因此，驱动程序上的stdout不会显示这些内容！要打印驱动程序上的所有元素，可以使用`collect()`方法，将RDD带到驱动程序节点，即`rdd.collect().foreach(println)`。但是，这可能会导致驱动程序耗尽内存，因为`collect()`将整个RDD提取到一台机器上；如果只需要打印RDD的几个元素，则更安全的方法是使用`take()`：`rdd.take(100).foreach(println)`。

### 使用键值对

虽然大多数Spark运算都在包含任意类型对象的RDD上工作，但只有少数特殊运算只在键值对RDD上可用。最常见的是分布式**混洗（Shuffle）**运算，例如通过`key`分组或聚合元素。

在python中，这些运算在包含内置元组的RDD上工作。只需创建这样的元组，然后调用所需的操作。

例如，一下代码使用键值对的`reduceByKey`运算来计算文件中每行文本出现的次数：

```python
lines = sc.textFile("data.txt")
pairs = lines.map(lambda s: (s, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
```

再例如，我们还可以使用`counts.sortByKey()`按字母顺序对这些pairs进行排序，最后使用`counts.collect()`将他们作为对象列表返回驱动程序。

### 转换（Transformation）

下表列出了Spark支持的常见转换。有关详细信息，请参考RDD API文档 （[Scala](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD), [Java](http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/JavaRDD.html), [Python](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD), [R](http://spark.apache.org/docs/latest/api/R/index.html)）和pairRDD函数文档（[Scala](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions), [Java](http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/JavaPairRDD.html)）。

| Transformation                                               | Meaning                                                      |
| :----------------------------------------------------------- | :----------------------------------------------------------- |
| **map**(*func*)                                              | 返回一个新的分布式数据集，该数据集由函数func传递source的每个元素而形成。 |
| **filter**(*func*)                                           | 返回一个新的数据集，过滤掉func返回false的元素。              |
| **flatMap**(*func*)                                          | 与map相似，但每个输入项都可以映射到0个或多个输出项（因此func应该返回一个序列而不是单个元素）。 |
| **mapPartitions**(*func*)                                    | 与map类似，但它在RDD的每个分区（块）上单独运行，因此当在T类型的RDD上运行时，func必须是`(Iterator<T> => Iterator<U>)`。 |
| **mapPartitionsWithIndex**(*func*)                           | 与mapPartitions类似，但func的参数多了一个integer类型，代表分区的index，因此当在T类型的RDD上运行时，func必须是`(Int, Iterator<T> => Iterator<U>)`。 |
| **sample**(*withReplacement*, *fraction*, *seed*)            | 对RDD采样，以及是否需要替换。                                |
| **union**(*otherDataset*)                                    | 生成一个新的RDD，包含两个RDD中的所有元素。此操作会导致两个RDD中相同的元素也会被包含进去，想要去重请使用`distinct()`。 |
| **intersection**(*otherDataset*)                             | 求两个RDD的交集，只返回两个RDD中都有的元素。因为会通过网络混洗数据，所以性能较差。 |
| **distinct**([*numPartitions*]))                             | 去除RDD中重复的元素，有可能会触发网络混洗。                  |
| **groupByKey**([*numPartitions*])                            | 当调用`Pair<K, V>`（键值对）的RDD时，返回`Pair<K, Iterable<V>>`。**注意：**如果分组是为了对每个键执行聚合操作（如求和或求平均值），则使用`reduceByKey`会带来更好的性能表现。**注意：**默认情况下，输出中的并行度级别取决于父RDD的分区数。可以传递一个可选的`numPartitions`参数来设置不同数量的任务。**一句话总结：对具有相同K的V进行分组。** |
| **reduceByKey**(*func*, [*numPartitions*])                   | 当对`Pair<K, V>`元素的RDD调用时，返回一个`Pair<K, V>`的RDD，其中每个K的值使用给定的reduce函数func来聚合，该func的类型必须为`(V, V) => V`。与`groupByKey`中一样，reduce任务的数量可以通过可选的第二个参数配置。**一句话总结：合并相同K的V**。 |
| **aggregateByKey**(*zeroValue*)(*seqOp*, *combOp*, [*numPartitions*]) | 当对`Pair<K, V>`元素的RDD调用时，返回一个`Pair<K, V>`的RDD，其中每个K的值使用给定的组合函数和中性『零』值进行聚合。允许与输入类型不同的聚合值类型，同时避免不必要的分配。与`groupByKey`类似，reduce任务的数量可以通过可选的第二个参数进行配置。 |
| **sortByKey**([*ascending*], [*numPartitions*])              | 当在K有序的`Pair<K, V>`元素的RDD上调用时，返回一个由K升序或降序`Pair<K, V>`的RDD。**一句话总结：返回一个根据K排序的RDD**。 |
| **join**(*otherDataset*, [*numPartitions*])                  | 内连接。把元素是`Pair<K, V>`和`Pair<K, W>`的两个RDD连接起来，返回元素是`Pair<K, (V, W)>`的RDD。也支持外连接，比如：`leftOuterJoin`, `rightOuterJoin`和`fullOuterJoin`。 |
| **cogroup**(*otherDataset*, [*numPartitions*])               | 当调用`Pair<K, V>`和`Pair<K, W>`的RDD时，返回元素是元组`(K, Iterable<V>, Iterable<W>)`的RDD，此操作也可以使用`groupWith`（内部实现就是`cogroup`）来完成 |
| **cartesian**(*otherDataset*)                                | When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements). |
| **pipe**(*command*, *[envVars]*)                             | 通过shell命令（例如Perl或者Bash脚本）对RDD的每个分区进行管道连接。RDD的元素写入进程的stdin，输出到stdout的行(lines)作为字符串的RDD返回。 |
| **coalesce**(*numPartitions*)                                | Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset. |
| **repartition**(*numPartitions*)                             | Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network. |
| **repartitionAndSortWithinPartitions**(*partitioner*)        | Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys. This is more efficient than calling `repartition` and then sorting within each partition because it can push the sorting down into the shuffle machinery. |

### 处理（Action）

下表列出了Spark支持的常见处理（actions）。有关详细信息，请参考RDD API文档 （[Scala](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD), [Java](http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/JavaRDD.html), [Python](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD), [R](http://spark.apache.org/docs/latest/api/R/index.html)）和pairRDD函数文档（[Scala](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions), [Java](http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/JavaPairRDD.html)）。

| Action                                             | Meaning                                                      |
| :------------------------------------------------- | :----------------------------------------------------------- |
| **reduce**(*func*)                                 | Aggregate the elements of the dataset using a function *func* (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel. |
| **collect**()                                      | Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data. |
| **count**()                                        | Return the number of elements in the dataset.                |
| **first**()                                        | Return the first element of the dataset (similar to take(1)). |
| **take**(*n*)                                      | Return an array with the first *n* elements of the dataset.  |
| **takeSample**(*withReplacement*, *num*, [*seed*]) | Return an array with a random sample of *num* elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed. |
| **takeOrdered**(*n*, *[ordering]*)                 | Return the first *n* elements of the RDD using either their natural order or a custom comparator. |
| **saveAsTextFile**(*path*)                         | Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file. |
| **saveAsSequenceFile**(*path*)  (Java and Scala)   | Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc). |
| **saveAsObjectFile**(*path*)  (Java and Scala)     | Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using`SparkContext.objectFile()`. |
| **countByKey**()                                   | Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key. |
| **foreach**(*func*)                                | Run a function *func* on each element of the dataset. This is usually done for side effects such as updating an [Accumulator](http://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators) or interacting with external storage systems.  **Note**: modifying variables other than Accumulators outside of the `foreach()` may result in undefined behavior. See [Understanding closures ](http://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures-a-nameclosureslinka)for more details. |

### 混洗（shuffle）操作

Spark中某些操作会触发一个称为shuffle的事件。Shuffle是SPark重新分配数据的机制，因此它在分区之间的分组方式不同。这通常涉及到跨执行器和机器拷贝数据，使得shuffle成为一个复杂且昂贵的操作。

> shuffle可以理解为扑克牌中的洗牌操作。后面未将shuffle直接翻译成混洗（洗牌）。

#### 背景

为了理解shuffle过程中会发生什么，我们以`reduceByKey`为例。`reduceByKey`生成一个新的RDD，其中一个Key的所有值组合成一个元组：(Key，以及Key对应的所有值执行reduce函数的结果)。挑战在于，并非所有单个Key的值都必须位于同一分区，甚至同一台机器上，但它们必须位于同一位置上才能计算结果。

在Spark中，数据通常不分布在分区之间，而分布在特定操作所需的位置。在计算过程中，单个任务将在单个分区上操作——因此，要组织所有数据以执行单个`reduceByKey`的reduce任务，spark需要执行一个`all-to-all`的操作。它必须从所有分区中读取以查找所有键的所有值，然后将各个分区中的值组合在一起以计算每个键的最终结果——这被称为`Shuffle`。

虽然新`shuffle`数据的每个分区中的元素集是确定的，分区本身的顺序也是确定的，但这些元素的顺序不是。如果希望在`shuffle`后按预期的顺序排列数据，那么可以使用：

- `mapPartitions`，用于对每个分区进行排序，例如，使用`.sorted`
- `repartitionAndSortWithinPartitions`，在重新分区的同时对分区进行有效排序
- `sortBy`，对RDD做全局排序

可能导致`shuffle`操作：`repartition`和`coalesce`操作；所有以**ByKey**为后缀的操作（但不包括`countByKey`），如`groupByKey`、`reduceByKey`等；**join**操作，如`cogroup`、`join`等。

#### 性能影响

`Shuffle`是一项昂贵的操作，因为它设计磁盘I/O、数据序列化和网络I/O。要为`shuffle`操作组织数据，spark会生成一组`map`以**组织**数据，以及一组`reduce`任务以**聚合**数据。这个术语来自`MapReduce`，但与spark的`map`和`reduce`操作没有直接关系。

在内部，来自单个`map`任务结果保存在内存中，直到它们无法容纳为止。然后根据目标分区对它们进行排序，并将其写入单个文件。在`reduce`方面，`reduce`任务读取相关的排序块。

某些`shuffle`操作可能会消耗大量堆内存，因为它们在传输记录之前或之后使用内存中的数据结构来组织记录。具体来说，`reuceByKey`和`aggregateByKey`在`map`端创建这些结构，而**ByKey**操作在`reduce`端生成这些结构。当数据在内存中放不下时，spark会将这些数据存在磁盘上，从而产生额外的磁盘I/O开销和增加垃圾回收操作。

`shuffle`还会在磁盘上生成大量中间文件。从Spark1.3开始，这些文件将被保留，直到不再使用相应的RDD并被回收。这样，如果重新计算，则无需重新创建`shuffle`文件。如果应用程序保留对这些RDD的引用，或者GC不经常启动，垃圾回收可能只在很长一段时间之后发生。这意味着，长时间运行的Spark作业可能会消耗大量的磁盘空间。配置`Spark context`时，临时存储目录有`spark.local.dir`配置参数指定。

可以通过调整各种配置参数来调整`shuffle`行为。请参阅[Spark Configuration Guide](http://spark.apache.org/docs/latest/configuration.html)中『shuffle行为』部分。

## RDD持久化

在Spark中最重要的一项能力就是跨操作在内存中持久化（缓存）数据集。当持久化一个RDD时，每个节点将其计算的任何分区存储在内存中，并在该数据集（或从该数据集派生的数据集）上的其他操作中重用它们。这使得以后的`action`操作更快（通常操作10倍）。缓存是迭代算法和快速交互使用的关键工具。

可以使用`persist()`或`cache()`方法将RDD标记为持久化。第一次在操作中计算它时，它将保存在节点的内存中。Spark的缓存是容错的——如果RDD的任何分区丢失，它将使用最初创建它的`转换`自动重新计算。

此外，每个持久化的RDD可以使用不同的存储级别（levels）来存储，例如，将数据集保存在磁盘上、内存中，但作为序列化的Java对象（以节省空间）将其复制到节点上。调用`persist()`的时候将`StorageLevel`对象（Scala、Java、Python）传入来设置level。`cache()`方法是使用默认存储级别的简写，即`StorageLevel.MEMORY_ONLY`（将反序列化对象存储在内存中）。完整的存储级别列表：

| Storage Level                          | Meaning                                                      |
| :------------------------------------- | :----------------------------------------------------------- |
| MEMORY_ONLY                            | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. This is the default level. |
| MEMORY_AND_DISK                        | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the partitions that don't fit on disk, and read them from there when they're needed. |
| MEMORY_ONLY_SER  (Java and Scala)      | Store RDD as *serialized* Java objects (one byte array per partition). This is generally more space-efficient than deserialized objects, especially when using a [fast serializer](http://spark.apache.org/docs/latest/tuning.html), but more CPU-intensive to read. |
| MEMORY_AND_DISK_SER  (Java and Scala)  | Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in memory to disk instead of recomputing them on the fly each time they're needed. |
| DISK_ONLY                              | Store the RDD partitions only on disk.                       |
| MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc. | Same as the levels above, but replicate each partition on two cluster nodes. |
| OFF_HEAP (experimental)                | Similar to MEMORY_ONLY_SER, but store the data in [off-heap memory](http://spark.apache.org/docs/latest/configuration.html#memory-management). This requires off-heap memory to be enabled. |

**注意：**在Python中，存储的对象将始终使用`pickle`库进行序列化，因此选择序列化级别并不重要。Python中可能的存储级别包括`MEMORY_ONLY`、` MEMORY_ONLY_2`、 `MEMORY_AND_DISK`、 `MEMORY_AND_DISK_2`、 `DISK_ONLY`和`DISK_ONLY_2`。

Spark还在`shuffle`操作中自动保留一些中间数据（如`reduceByKey`），即使没有调用`persisit`。这样做是为了在`shuffle`期间，避免节点失败时重新计算整个输入。我们仍然建议用户在计划重用结果RDD时，对其调用`persist`。

### 选择合适的StorageLevel

Spark的存储级别旨在内存使用率和CPU效率之间提供不同的权衡。我们建议通过以下过程来选择一个合适的：

- 如果你的RDD适合默认的存储级别（**MEMORY_ONLY**），那么就这样。这是CPU效率最高的选项，允许RDD上的操作以尽可能快的速度运行。
- 如果没有，请尝试使用**MEMORY_ONLY_SER**并选择一个快速序列化库，以使对象更节省空间，但访问速度仍然相当快。（Java和Scala）
- 除非计算数据集的函数很昂贵，或者它们过滤了大量数据，否则不要溢出到磁盘上。否则，重新计算分区的速度可能与从磁盘读取分区的速度一样快。
- 如果希望快速故障恢复（例如，如果使用Spark来服务来自Web应用程序的请求），请使用复制的存储级别。所有存储级别都通过重新计算丢失的数据提供了完全的容错性，但是复制的存储级别允许继续在RDD上运行任务，而不必等待重新计算丢失的分区。

### 移除数据

Spark自动监控每个节点上缓存使用情况，并使用流行的LRU（Least-Recently-Used，最近最久未使用）算法移除老旧数据分区。如果要手动删除RDD而不是等待它从缓存中被抹掉，请使用`rdd.unpersist()`方法。
