# 编写独立应用程序

假设，我们希望用Spark的API编写一个独立应用程序。我们将在Scala(使用sbt)、Java(使用Maven)、Python(使用pip)完成一个简单的应用。(这里只给出了Python)

在`setup.py`中添加以下内容：

!FILENAME setup.py
```python
install_requires=[
        'pyspark=={site.SPARK_VERSION}'
]
```

我们创建一个简单的Spark application示例代码

!FILENAME SimpleApp.py

```python
from pyspark.sql import SparkSession

logFile = "YOUR_SPARK_HOME/README.md"  # Should be some file on your system
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
```

以上代码分别计算了'a'和'b'出现的行的数量。注意，你需要把`YOUR_SPARK_HOME`替换成你的Spark安装的路径(或者你找个其它的文本文件)。

对于使用自定义类或第三方库的应用程序，我们还可以通过 `spark-submit --py-files`参数将代码依赖项打包到zip文件中(有关详细信息，请参阅`spark-submit —help`)。SimpleApp已经足够简单了，所以我们不用添加其它代码依赖。

用`bin/spark-submit`运行脚本：

```shell
# Use spark-submit to run your application
$ YOUR_SPARK_HOME/bin/spark-submit \
  --master local[4] \
  SimpleApp.py
...
Lines with a: 46, Lines with b: 23
```

如果你是通过`pip`安装PySpark到环境变量(比如`pip install pyspark`)，你就可以用python解释器直接运行你的app，或者使用提供的`spark-submit`，凭你喜好了。

```shell
# Use the Python interpreter to run your application
$ python SimpleApp.py
...
Lines with a: 46, Lines with b: 23
```

