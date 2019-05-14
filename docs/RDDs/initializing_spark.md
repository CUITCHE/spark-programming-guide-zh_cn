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

