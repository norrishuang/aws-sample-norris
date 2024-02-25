### Flink CDC & Import Data to Hudi With KDA

[TOC]

## 代码说明：
本项目代码，可用于运行在 Amazon Kinesis Data Analytics 中。

### 1.FlinkCDCPostgres

通过FlinkCDC实时采集Postgresql的数据，写入Kafka。

### 2.IcebergApplication

通过FlinkSQL，将mysql数据实时摄入 Iceberg。
由于KDA与Iceberg集成存在问题参见 [#3044](https://github.com/apache/iceberg/issues/3044)。本项目提供 workround 解决该问题。

**解决方案**
1. 参考 pom.xml 文件，通过 **relocation** 将冲突的类替换。 
2. 重写 **HadoopUtils** 类

[参考](https://gist.github.com/mgmarino/19a4a26a40dfbc7f4249e3c567d32afa#file-hadooputils-java)

### 3.HuidApplication

消费Kafka的数据，以Hudi格式写入S3。MSK 先用无认证的模式。

**执行：**

1. 编译后将target目录下的 jar文件 上传至S3目录下。

2. 创建Kinesis Data Application项目，Flink 选择 **1.5** 版本。

3. VPC 选择 MSK 所在的VPC

4. **Runtime properties** 配置如下

   | Group                      | **Key**       | **Value**                                                    |
   | -------------------------- | ------------- | ------------------------------------------------------------ |
   | FlinkApplicationProperties | brokers       | MSK Boostrap Server                                          |
   | FlinkApplicationProperties | kafka-topic   | 需要消费的topic name                                         |
   | FlinkApplicationProperties | s3Path        | Hudi写入的S3目录（用s3a：例如s3a://[your bucket name]/data） |
   | FlinkApplicationProperties | hivemetastore | 用于同步hive元数据的thriftserver                             |
   
   

保存配置后，点击【**Run**】