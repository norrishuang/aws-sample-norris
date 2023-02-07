### Flink CDC & Import Data to Hudi With KDA

[TOC]

### 代码说明：

#### FlinkCDCPostgres

通过FlinkCDC实时采集Postgresql的数据，写入Kafka。



#### HuidApplication

消费Kafka的数据，以Hudi格式写入S3。MSK 先用无认证的模式。

**执行：**

1. 编译后将target目录下的 jar文件 上传至S3目录下。

2. 创建Kinesis Data Application项目，Flink 选择 **1.5** 版本。

3. VPC 选择 MSK 所在的VPC

4. **Runtime properties** 配置如下

   | Group                      | **Key**     | **Value**                                                    |
   | -------------------------- | ----------- | ------------------------------------------------------------ |
   | FlinkApplicationProperties | brokers     | MSK Boostrap Server                                          |
   | FlinkApplicationProperties | kafka-topic | 需要消费的topic name                                         |
   | FlinkApplicationProperties | s3Path      | Hudi写入的S3目录（用s3a：例如s3a://<your bucket name>/data） |

   

保存配置后，点击【**Run**】