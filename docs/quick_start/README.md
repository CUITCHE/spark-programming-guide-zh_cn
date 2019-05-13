# 快速开始

本教程介绍了的Spark的基本使用。首先是Spark交互式Shell的API，接着介绍如何使用Python编写Spark应用。

在教程开始前，从[官方网站](https://spark.apache.org/downloads.html)下载Spark。本教程不会使用到HDFS，所以你可以下载带任意版本Hadoop的Spark Release。

注意，在Spark2.0前，Spark的主要程序接口是`弹性分布式数据集`(Resilient Distributed Dataset，RDD)。在Spark2.0后，Dataset替代了RDDs，它就像强类型的RDD，但在内部提供了更丰富的优化。RDD接口仍然可以使用，你可以从[RDD programming guide](http://spark.apache.org/docs/latest/rdd-programming-guide.html)获得更多了解。然而，我们仍强烈建议你使用Dataset，它拥有比RDD更好的性能。更多关于Dataset的介绍，请参考[SQL编程指南](../spark_sql_guide/README.md)。

