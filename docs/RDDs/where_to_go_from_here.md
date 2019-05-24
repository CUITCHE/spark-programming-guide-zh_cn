# 下一步

您可以在Spark网站上看到一些[Spark程序示例](https://spark.apache.org/examples.html)。此外，在`example`目录（Scala、Java、Python、R）中包括几个示例。您可以通过将类名传递给Spark的`bin/run-example`脚本来运行Java和Scala示例，例如：

```shell
./bin/run-example SparkPi
```

对于python的示例，使用`spark-submit`：

```shell
./bin/spark-submit examples/src/main/python/pi.py
```

对于R的示例，使用`spark-submit`：

```shell
./bin/spark-submit examples/src/main/r/dataframe.R
```

有关优化程序的帮助，[配置](http://spark.apache.org/docs/latest/configuration.html)和[调整](http://spark.apache.org/docs/latest/tuning.html)指南提供了有关最佳实践的信息。它们对于确保数据以有效的格式存储在内存中尤为重要。有关部署的帮助，[集群模式概述](http://spark.apache.org/docs/latest/cluster-overview.html)描述了分布式操作中涉及的组件和支持的集群管理器。

最后，完整的API文档，请参阅[Scala](http://spark.apache.org/docs/latest/api/scala/#org.apache.spark.package)、[Java](http://spark.apache.org/docs/latest/api/java/)、[Python](http://spark.apache.org/docs/latest/api/python/)和[R](http://spark.apache.org/docs/latest/api/R/)。