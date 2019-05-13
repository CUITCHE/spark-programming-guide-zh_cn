# 下一步

恭喜你成功运行了第一个Spark应用！

- 有关API的深入概述，请从[RDD编程指南](../RDDs/README.md)和[SQL编程指南](../spark_sql_guide/README.md)开始，或者其它的内容。

- 对于在集群上运行应用程序，请参阅[部署概述](http://spark.apache.org/docs/latest/cluster-overview.html)。

- 最后，在`examples`目录下有一些示例(Scala、Java、Python、R)，你可以像这样运行它们：

  ```shell
  # For Scala and Java, use run-example:
  ./bin/run-example SparkPi
  
  # For Python examples, use spark-submit directly:
  ./bin/spark-submit examples/src/main/python/pi.py
  
  # For R examples, use spark-submit directly:
  ./bin/spark-submit examples/src/main/r/dataframe.R
  ```

  