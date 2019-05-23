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

- `SparkContext.wholeTextFiles`读取一个目录下所有的文本文件，然后返回<文件名, 文件内容>对（pair）。它不同于`textFile`，`textFile`是读取文件内容并按行返回。

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

### 键值对

虽然大多数Spark运算都在包含任意类型对象的RDD上工作，但只有少数特殊运算只在键值对RDD上可用。最常见的是分布式**混洗（Shuffle）**运算，例如通过`key`分组或聚合元素。

在python中，这些运算在包含内置元组的RDD上工作。只需创建这样的元组，然后调用所需的操作。

例如，一下代码使用键值对的`reduceByKey`运算来计算文件中每行文本出现的次数：

```python
lines = sc.textFile("data.txt")
pairs = lines.map(lambda s: (s, 1))
counts = pairs.reduceByKey(lambda a, b: a + b)
```

再例如，我们还可以使用`counts.sortByKey()`按字母顺序对这些pairs进行排序，最后使用`counts.collect()`将他们作为对象列表返回驱动程序。

### 转换

下表列出了Spark支持的常见转换。有关详细信息，请参考RDD API文档 （[Scala](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD), [Java](http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/JavaRDD.html), [Python](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.RDD), [R](http://spark.apache.org/docs/latest/api/R/index.html)）和pairRDD函数文档（[Scala](http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions), [Java](http://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/api/java/JavaPairRDD.html)）。

| Transformation                                               | Meaning                                                      |
| :----------------------------------------------------------- | :----------------------------------------------------------- |
| **map**(*func*)                                              | 返回一个新的分布式数据集，该数据集由函数func传递source的每个元素而形成。 |
| **filter**(*func*)                                           | 返回一个新的数据集，过滤掉func返回false的元素。              |
| **flatMap**(*func*)                                          | Similar to map, but each input item can be mapped to 0 or more output items (so *func* should return a Seq rather than a single item).与map相似，但每个输入项都可以映射到0个或多个输出项（因此func应该返回一个序列而不是） |
| **mapPartitions**(*func*)                                    | Similar to map, but runs separately on each partition (block) of the RDD, so *func* must be of type Iterator<T> => Iterator<U> when running on an RDD of type T. |
| **mapPartitionsWithIndex**(*func*)                           | Similar to mapPartitions, but also provides *func* with an integer value representing the index of the partition, so *func* must be of type (Int, Iterator<T>) => Iterator<U> when running on an RDD of type T. |
| **sample**(*withReplacement*, *fraction*, *seed*)            | Sample a fraction *fraction* of the data, with or without replacement, using a given random number generator seed. |
| **union**(*otherDataset*)                                    | Return a new dataset that contains the union of the elements in the source dataset and the argument. |
| **intersection**(*otherDataset*)                             | Return a new RDD that contains the intersection of elements in the source dataset and the argument. |
| **distinct**([*numPartitions*]))                             | Return a new dataset that contains the distinct elements of the source dataset. |
| **groupByKey**([*numPartitions*])                            | When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs.  **Note:** If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using `reduceByKey` or `aggregateByKey` will yield much better performance.  **Note:** By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional `numPartitions` argument to set a different number of tasks. |
| **reduceByKey**(*func*, [*numPartitions*])                   | When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function *func*, which must be of type (V,V) => V. Like in `groupByKey`, the number of reduce tasks is configurable through an optional second argument. |
| **aggregateByKey**(*zeroValue*)(*seqOp*, *combOp*, [*numPartitions*]) | When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in `groupByKey`, the number of reduce tasks is configurable through an optional second argument. |
| **sortByKey**([*ascending*], [*numPartitions*])              | When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean `ascending` argument. |
| **join**(*otherDataset*, [*numPartitions*])                  | When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through `leftOuterJoin`, `rightOuterJoin`, and `fullOuterJoin`. |
| **cogroup**(*otherDataset*, [*numPartitions*])               | When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called `groupWith`. |
| **cartesian**(*otherDataset*)                                | When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements). |
| **pipe**(*command*, *[envVars]*)                             | Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings. |
| **coalesce**(*numPartitions*)                                | Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset. |
| **repartition**(*numPartitions*)                             | Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network. |
| **repartitionAndSortWithinPartitions**(*partitioner*)        | Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys. This is more efficient than calling `repartition` and then sorting within each partition because it can push the sorting down into the shuffle machinery. |

### 处理

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

