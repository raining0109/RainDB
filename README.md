# RainDB

RainDB 是一个 Java 实现的简单的数据库，部分原理参照自 MySQL、PostgreSQL。

## 运行方式

注意首先需要在 pom.xml 中调整编译版本，如果导入 IDE，请更改项目的编译版本以适应你的 JDK

首先执行以下命令编译源码：

```shell
mvn compile
```

接着执行以下命令以 `/home/rain/coding/RainDB/db_test/raindb` 作为路径创建数据库：

```shell
mvn exec:java -Dexec.mainClass="com.raining.raindb.backend.Launcher" -Dexec.args="-create /home/rain/coding/RainDB/db_test/raindb"
```

随后通过以下命令以默认参数启动数据库服务：

```shell
mvn exec:java -Dexec.mainClass="com.raining.raindb.backend.Launcher" -Dexec.args="-open /home/rain/coding/RainDB/db_test/raindb"
```

这时数据库服务就已经启动在本机的 9988 端口。重新启动一个终端，执行以下命令启动客户端连接数据库：

```shell
mvn exec:java -Dexec.mainClass="com.raining.raindb.client.Launcher"
```

测试

```sql
create table user id int32,name string,age int32 (index id name)
insert into user values 10002 "zhangsan" 21
```

可以通过下面的命令将RainDB通过链接分享出去，访问者从浏览器就可以体验。