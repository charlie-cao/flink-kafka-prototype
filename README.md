# flink-kafka-prototype

# kafka setup

# flink setup

# build 

mvn clean package

# run

submit to http://localhost:8081/


---

终于把整个流运算流程走通了.好费劲啊.

scrapy -> kafka -> flink -> hdfs.

flink 打包直接用mvn. 然后在页面上提交到任务
http://localhost:8081/

开启多个窗口,就可以看到不同的topic中传输过来的数据在flink里的一个窗口期中运算完毕并保存到本地文件中.
![](./res/Snip20191225_7.png "cool")

关键源码参考了这位的.
https://github.com/mabdulrazzak/flink-kafka-prototype/tree/master/src/main/java

我发先java的包管理好用是好用,但是导致系统变得异常复杂.版本依赖.再加上IDE,死的更惨.

所以还是离开这些东西,从头开始整,才能搞清楚.

下一步用新的java语法写,会舒服很多.不过还是熟悉一下java语法.以及流运算的架构用法.这些.

最终发现还是在github上找源码看来的更直接有效. 看别人的教程实在是太绕弯了.