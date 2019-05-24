# 共享变量

通常，当传递给Spark操作（如`map`或`reduce`）的函数在远程集群节点上执行时，它在函数中使用的所有变量的单独副本上工作。这些变量被复制到每台机器上，对于远程机器上的变量的任何更新都不会传播回驱动程序。支持跨任务的通用读写共享变量将是低效的。然而，spark确实为两种常见的使用模式提供了两种有限类型的共享变量：广播变量（Broadcat Variables）和累加器（Accumulators）。

## 广播变量（Broadcat Variables）

广播变量允许程序员在每台计算机上缓存只读变量，而不是将其副本与任务一起发送。例如，它们可以有效地为每个节点提供一个大型输入数据集的副本。Spark还尝试使用有效的广播算法来分配广播变量，以降低通信成本。

Spark `actions`通过一组阶段（stages）执行，由分布式『shuffle』操作分隔。spark自动广播每个阶段中任务所需的公共数据。以这种方法广播的数据以序列化形式缓存，并在运行每个任务之前进行反序列化。这意味着，仅当跨多个阶段的任务需要相同的数据或以反序列化形式缓存数据时，显示创建广播变量才有用。

广播变量是通过调用 `SparkContext.broadcast(v)`从变量`v`创建的。广播变量是`v`的包装器，它的值可以通过调用`value`方法来访问。请看下面的示例代码：

```python
>>> broadcastVar = sc.broadcast([1, 2, 3])
<pyspark.broadcast.Broadcast object at 0x102789f10>

>>> broadcastVar.value
[1, 2, 3]
```

创建广播变量后，应在集群上运行的任何函数中使用它，而不是使用值`v`，这样`v`就不会多次发送到节点。此外，对象`v`在广播后不应进行修改，以确保所有节点都获得广播变量的相同值（例如，如果变量稍后被发送到新节点）。

## 累加器（Accumulators）

累加器是只通过关联和交换操作『添加』到的变量，因此可以有效地并行支持。它们可以用来实现计数器（如`MapReduce`）或求和。Spark原生支持数字类型的累加器，程序员可以添加对新类型的支持。

作为用户，您可以创建命名或未命名的累加器。如下图所示，对于修改累加器的阶段，Web UI中将显示一个命名的累加器（在此实例`counter`中）。Spark在『tasks』表中显示由任务修改的每个累加器的值。

![spark-webui-accumulators](/Users/hejunqiu/Documents/github-repo/spark-programming-guide-zh_cn/assets/images/spark-webui-accumulators.png)

在UI中跟踪累加器对于了解运行阶段的进度很有用（注意：Python尚不支持）。

通过调用 `SparkContext.accumulator(v)`，从初始值`v`创建一个累加器。然后，在集群上运行的任务可以使用`add`方法或+=运算符添加到集群中。但是，它们无法读取其值。只有驱动程序才能使用其值方法读取累加器的值。

下面的代码显示了用于添加数组元素的累加器：

```python
>>> accum = sc.accumulator(0)
>>> accum
Accumulator<id=0, value=0>

>>> sc.parallelize([1, 2, 3, 4]).foreach(lambda x: accum.add(x))
...
10/09/29 18:41:08 INFO SparkContext: Tasks finished in 0.317106 s

>>> accum.value
10
```

虽然这段代码使用了对int类型的累加器的内置支持，但是程序员也可以通过子类化[AccumulatorParam](http://spark.apache.org/docs/latest/api/python/pyspark.html#pyspark.AccumulatorParam)来创建自己的类型。AccumulatorParam接口有两种方法：`zero`用于为数据类型提供『零值』，`addInPlace`用于将两个值相加。例如，假设我们有一个表示数学向量的`Vector`类，我们可以写：

```python
class VectorAccumulatorParam(AccumulatorParam):
    def zero(self, initialValue):
        return Vector.zeros(initialValue.size)

    def addInPlace(self, v1, v2):
        v1 += v2
        return v1

# Then, create an Accumulator of this type:
vecAccum = sc.accumulator(Vector(...), VectorAccumulatorParam())
```

对于**仅在操作内部**执行的累加器更新，spark保证每个任务对累加器的更新只应用一次，即重新启动的任务不会更新该值。在转换中，用户应该知道，如果重新执行任务或作业阶段，每个任务的更新可能会应用多次。

累加器不改变Spark的惰性评估模型。如果在RDD上的操作中更新它们，则只有在将RDD作为操作的一部分进行计算后，才会更新它们的值。因此，当在像`map()`这样的惰性转换中进行时，累加器的更新不保证被执行。下面的代码片段演示了此属性：

```python
accum = sc.accumulator(0)
def g(x):
    accum.add(x)
    return f(x)
data.map(g)
# Here, accum is still 0 because no actions have caused the `map` to be computed.
```