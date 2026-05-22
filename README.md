# TDengineDbSync

TDengine 3.0+ 数据库数据导出与导入工具，基于 Java 21 + Spring Boot 3.4.5 构建，命令行模式运行。

## 构建与测试

```bash
mvn clean test
```

## 运行方式

```bash
# 导出
mvn spring-boot:run -Dspring-boot.run.arguments="--mode=export --database=test --table=meters --file=/tmp/meters.csv"

# 导入
mvn spring-boot:run -Dspring-boot.run.arguments="--mode=import --database=test --table=meters --file=/tmp/meters.csv --batch-size=500"
```

## 数据源配置示例

通过 Spring Boot 标准 JDBC 配置连接 TDengine（3.0+）：

```properties
spring.datasource.url=jdbc:TAOS://127.0.0.1:6030/test
spring.datasource.username=root
spring.datasource.password=taosdata
```
