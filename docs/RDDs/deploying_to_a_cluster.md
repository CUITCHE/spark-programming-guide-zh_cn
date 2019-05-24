# 部署到集群

 [Application Submission Guide](http://spark.apache.org/docs/latest/submitting-applications.html)描述了如何将应用程序提交到集群。简而言之，一旦你把你的应用程序打包成一个JAR（对于Java/Scala）或者一组.py或.zip文件（对于Python），`bin/spark-submit`脚本就可以将它提交给任何支持的集群管理器。