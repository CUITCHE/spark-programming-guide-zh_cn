# 初始化Spark

Spark程序必须要做的第一件事：创建一个`SparkContext`对象，它负责和集群沟通。通过构建一个`SparkConf`对象来配置SparkContext。

```python
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
```

- appName：给你的应用程序取的名字，会在集群UI上会显示它。
- master：它是一个URL，或者是特殊的"local"字符串，以运行在本地。URL规则请参阅[Master URL](http://spark.apache.org/docs/latest/submitting-applications.html#master-urls)。

实际操作中，当运行在一个集群上时，我们往往不会把master写死到代码中，而是在通过`spark-submit`启动脚本时传入master参数。关于`spark-submit`的用法请参考[官方文档](http://spark.apache.org/docs/latest/submitting-applications.html#launching-applications-with-spark-submit)。

## 使用Shell

在PySpark shell中，已经默认为你创建了一个SparkContext对象，名字叫`sc`。你自己创建的SparkContext不会起作用。

你可以设置`--master`参数来决定连接到哪个master上；通过向`--py-files`传递一个用逗号分隔的列表，将`python.zip、.egg、或.py文件`添加到运行时路径；通过向`--packages`参数提供一个用逗号分隔的`maven包地址`列表，向`shell session`添加依赖项(例如Spark包)。

任何可能存在依赖关系的附加repositories(例如sonatype)都可以通过`—repositories`传递；必要时，spark包中的任何python依赖项(在该包的requirements.txt中列出)须使用pip手动安装。

例如，使用4个核心运行`bin/pyspark`：

```shell
$ ./bin/pyspark --master local[4]
```

将`code.py`添加到搜索路径中(方便以后能够导入代码)：

```shell
$ ./bin/pyspark --master local[4] --py-files code.py
```

完整的参数选项，请运行`pyspark —help`获取。在幕后，`pyspark`调用了更通用的`spar-submit script`。

为了提高Python解释器的便利性，`PySpark Shell`完全支持`IPython 1.0.0+`。为了使用它，在当前shell添加环境变量：

```shell
$ PYSPARK_DRIVER_PYTHON=ipython ./bin/pyspark
```

对于想使用`Jupyter notebook`(你需要知道它怎么用)：

```shell
$ PYSPARK_DRIVER_PYTHON=jupyter PYSPARK_DRIVER_PYTHON_OPTS=notebook ./bin/pyspark
```

通过设置`PYSPARK_DRIVER_PYTHON_OPTS`自定义`ipython`或`jupyter`命令。

启动`Jupyter Notebook`后，可以从`Files`选项卡创建新的"Python 2"notebook。在notebook内，在开始使用前，将命令`%pylab inline`作为notebook的一部分直接输入。(译者注：这段表示没太明白，不使用Jupyter的话，就跳过吧)。