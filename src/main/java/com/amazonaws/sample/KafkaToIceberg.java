/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file has been extended from the Apache Flink project skeleton.
 */

package com.amazonaws.sample;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/** Iceberg Flink Streaming Job Run in KDA. */
public class KafkaToIceberg {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaToIceberg.class);

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final ParameterTool parameter = ParameterTool.fromArgs(args);

    // read the parameters from the Kinesis Analytics environment
    Map<String, Properties> applicationProperties =
        KinesisAnalyticsRuntime.getApplicationProperties();
    Properties flinkProperties = null;

    String kafkaTopic = parameter.get("kafka-topic", "AWSKafkaTutorialTopic");
    String brokers = parameter.get("brokers", "");
    //		String s3Path = parameter.get("s3Path", "");
    String Warehouse = parameter.get("warehouse", "");
    String ConsumGroup = parameter.get("consumgroup", "flink-workshop-group-01");
    String sinkTableName = parameter.get("sinkTable", "aws_iceberg_sink_table");

    if (applicationProperties != null) {
      flinkProperties = applicationProperties.get("FlinkApplicationProperties");
    }

    if (flinkProperties != null) {
      kafkaTopic = flinkProperties.get("kafka-topic").toString();
      brokers = flinkProperties.get("brokers").toString();
      //			s3Path = flinkProperties.get("s3Path").toString();
      Warehouse = flinkProperties.get("warehouse").toString();
      ConsumGroup = flinkProperties.get("consumgroup").toString();
      sinkTableName = flinkProperties.get("sinkTable").toString();
    }

    // Process stream using sql API
    CDCIcebergSqlExample.createAndDeployJob(env, Warehouse, brokers, kafkaTopic, ConsumGroup, sinkTableName);
  }

  public static class CDCIcebergSqlExample {

    public static void createAndDeployJob(
        StreamExecutionEnvironment env,
        String warhehouse,
        String kafkaBoostrapServer,
        String topic,
        String consumGroup,
        String sinkTableName) {
      StreamTableEnvironment streamTableEnvironment =
          StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().build());

      Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
      configuration.setString("execution.checkpointing.interval", "1 min");

      final String icebergCatalog =
          String.format(
              "CREATE CATALOG glue_catalog WITH ( \n"
                  + "'type'='iceberg', \n"
                  + "'warehouse'='%s', \n"
                  + "'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog', \n"
                  + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO');",
              warhehouse);

      LOG.info(icebergCatalog);
      streamTableEnvironment.executeSql(icebergCatalog);

      // kafka source
      final String sourceSQLTopic1 =
          String.format("CREATE TABLE kafka_source_table_01 (\n"
                  + "                                    uuid STRING,\n"
                  + "                                    user_name STRING,\n"
                  + "                                    phone_number BIGINT,\n"
                  + "                                    product_id INT,\n"
                  + "                                    product_name STRING,\n"
                  + "                                    product_type STRING,\n"
                  + "                                    manufacturing_date INT,\n"
                  + "                                    price FLOAT,\n"
                  + "                                    unit INT,\n"
                  + "                                    ts TIMESTAMP(3)\n"
                  + ") with (\n"
                  + "'connector' = 'kafka',\n"
                  + "'topic' = '%s',\n"
                  + "'properties.bootstrap.servers' = '%s',\n"
                  + "'scan.startup.mode' = 'earliest-offset',\n"
                  + "'properties.group.id' = '%s',\n"
                  + "'json.timestamp-format.standard' = 'ISO-8601',\n"
                  + "'format' = 'json'\n"
                  + ");", "datafaker_user_order_list_01", kafkaBoostrapServer, consumGroup);
      LOG.info(sourceSQLTopic1);
      streamTableEnvironment.executeSql(sourceSQLTopic1);

      final String sourceSQLTopic2 =
              String.format(
                      "CREATE TABLE kafka_source_table_02 (\n"
                              + "                                    uuid STRING,\n"
                              + "                                    user_name STRING,\n"
                              + "                                    phone_number BIGINT,\n"
                              + "                                    product_id INT,\n"
                              + "                                    product_name STRING,\n"
                              + "                                    product_type STRING,\n"
                              + "                                    manufacturing_date INT,\n"
                              + "                                    price FLOAT,\n"
                              + "                                    unit INT,\n"
                              + "                                    ts TIMESTAMP(3)\n"
                              + ") with (\n"
                              + "'connector' = 'kafka',\n"
                              + "'topic' = '%s',\n"
                              + "'properties.bootstrap.servers' = '%s',\n"
                              + "'scan.startup.mode' = 'earliest-offset',\n"
                              + "'properties.group.id' = '%s',\n"
                              + "'json.timestamp-format.standard' = 'ISO-8601',\n"
                              + "'format' = 'json'\n"
                              + ");",
                      "datafaker_user_order_list_02", kafkaBoostrapServer, consumGroup);

      LOG.info(sourceSQLTopic2);
      streamTableEnvironment.executeSql(sourceSQLTopic2);

      //      String sinkTableName = "user_order_list_kafka_02";

      final String IcebergSink =
          String.format(
              "CREATE TABLE IF NOT EXISTS glue_catalog.icebergdb.%s (\n"
                  + "                                    uuid STRING,\n"
                  + "                                    user_name STRING,\n"
                  + "                                    phone_number BIGINT,\n"
                  + "                                    product_id INT,\n"
                  + "                                    product_name STRING,\n"
                  + "                                    product_type STRING,\n"
                  + "                                    manufacturing_date INT,\n"
                  + "                                    price FLOAT,\n"
                  + "                                    unit INT,\n"
                  + "                                    ts TIMESTAMP_LTZ(3),"
                  + "									 DT STRING\n"
                  + ") PARTITIONED BY (DT) WITH (\n"
                  + "'type'='iceberg',\n"
                  + "'catalog-name'='glue_catalog',\n"
                  + "'write.metadata.delete-after-commit.enabled'='true',\n"
                  + "'write.metadata.previous-versions-max'='5',\n"
                  + "'write.distribution-mode'='hash',\n"
                  + "'format-version'='2');",
              sinkTableName);
      LOG.info(IcebergSink);

      streamTableEnvironment.executeSql(IcebergSink);

      StatementSet stmtSet = streamTableEnvironment.createStatementSet();

      final String insertSql01 =
              String.format("insert into glue_catalog.icebergdb.%s /*+ OPTIONS('write-parallelism'='4') */ \n"
                      + "select *,DATE_FORMAT(ts, 'yyyyMMdd') AS DT from default_catalog.default_database.kafka_source_table_01;"
                      , sinkTableName);
//
      stmtSet.addInsertSql(insertSql01.trim());

      final String insertSql02 =
              String.format("insert into glue_catalog.icebergdb.%s \n"
                      + "select *,DATE_FORMAT(ts, 'yyyyMMdd') AS DT from default_catalog.default_database.kafka_source_table_02;"
                      , sinkTableName);
//
      stmtSet.addInsertSql(insertSql02.trim());


      stmtSet.execute();

    }
  }
}
