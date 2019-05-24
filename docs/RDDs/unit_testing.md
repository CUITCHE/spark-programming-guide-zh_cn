# 单元测试

Spark对任何流行的单元测试框架的单元测试都很友好。只需在测试中创建一个`sparkContext`，并将主URL设置为`local`，运行操作，然后调用`sparkContext.stop()`将其删除。确保停止`finally`块或测试框架的`tearDown`方法中的上下文，因为Spark不支持在同一程序中同时运行的两个上下文。