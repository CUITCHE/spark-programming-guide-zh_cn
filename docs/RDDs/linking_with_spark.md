# 引入Spark

Spark2.4.3需要Python2.7+或者Python3.4+，它用标准CPython解释器，所以一些C语言库，比如`NumPy`，还可以用PyPy2.3+运行。

对Python2.6的支持，已在Spark2.2.0后移除了。

Python中的Spark应用程序可以由包含Spark运行时的`bin/spark-submit`脚本调用，也可以将其包含在`setup.py`中，如下所示：

```python
install_requires=[
        'pyspark=={site.SPARK_VERSION}'
]
```

若没有安装pip的pyspark，请使用Spark目录中的`bin/spark-submit`运行你的脚本。它会加载Spark的Java/Scala库，并可以让你的应用程序提交到集群。你还可以使用`bin/pyspark`来启动交互式的`python shell`。

若想访问HDFS数据，需要构建一个带有HDFS的PySpark。Spark主页提供了通用HDFS版本的Spark，你可以从 [Prebuilt packages](https://spark.apache.org/downloads.html)下载。

最后，只需在你的程序代码中导入Spark就行了。

```python
from pyspark import SparkContext, SparkConf
```

pyspark在驱动程序和worker中都需要相同的次要版本(比如驱动程序是Python3.4，那么worker程序也至少需要Python3.4，3.3就不行)。它默认使用path环境变量中的python版本，你可以手动指定pypark_python要使用的python版本，例如：

```shell
$ PYSPARK_PYTHON=python3.4 bin/pyspark
$ PYSPARK_PYTHON=/opt/pypy-2.5/bin/pypy bin/spark-submit examples/src/main/python/pi.py
```

