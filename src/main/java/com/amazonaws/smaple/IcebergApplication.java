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

package com.amazonaws.smaple;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.Map;

/**
 * Skeleton for a Hudi Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the pom.xml file (simply search for 'mainClass').
 *
 * <p>Disclaimer: This code is not production ready.</p>
 */
public class IcebergApplication {
	private static final Logger LOG = LoggerFactory.getLogger(IcebergApplication.class);

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool parameter = ParameterTool.fromArgs(args);

		//read the parameters from the Kinesis Analytics environment
		Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
		Properties flinkProperties = null;

//		String kafkaTopic = parameter.get("kafka-topic", "AWSKafkaTutorialTopic");
//		String brokers = parameter.get("brokers", "");
//		String s3Path = parameter.get("s3Path", "");
//		String hiveMetaStore = parameter.get("hivemetastore", "");

		if (applicationProperties != null) {
			flinkProperties = applicationProperties.get("FlinkApplicationProperties");
		}

//		if (flinkProperties != null) {
//			kafkaTopic = flinkProperties.get("kafka-topic").toString();
//			brokers = flinkProperties.get("brokers").toString();
//			s3Path = flinkProperties.get("s3Path").toString();
//			hiveMetaStore = flinkProperties.get("hivemetastore").toString();
//		}

//		LOG.info("kafkaTopic is :" + kafkaTopic);
//		LOG.info("brokers is :" + brokers);
//		LOG.info("s3Path is :" + s3Path);
//		LOG.info("hiveMetaStore is :" + hiveMetaStore);
//
//		//Create Properties object for the Kafka consumer
//		Properties kafkaProps = new Properties();
//		kafkaProps.setProperty("bootstrap.servers", brokers);

		//Process stream using sql API
		CDCIcebergSqlExample.createAndDeployJob(env);
	}


	public static class CDCIcebergSqlExample {

		public static void createAndDeployJob(StreamExecutionEnvironment env)  {
			StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
					env, EnvironmentSettings.newInstance().build());

			Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
			configuration.setString("execution.checkpointing.interval", "1 min");

//			final String createTableStmt = "CREATE TABLE IF NOT EXISTS CustomerTable (\n" +
//					"  `event_time` TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- from Debezium format\n" +
//					"  `origin_table` STRING METADATA FROM 'value.source.table' VIRTUAL, -- from Debezium format\n" +
//					"  `record_time` TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,\n" +
//					"  `CUST_ID` BIGINT,\n" +
//					"  `NAME` STRING,\n" +
//					"  `MKTSEGMENT` STRING,\n" +
//					"   WATERMARK FOR event_time AS event_time\n" +
//					") WITH (\n" +
//					"  'connector' = 'kafka',\n" +
//					"  'topic' = '"+ kafkaTopic +"',\n" +
//					"  'properties.bootstrap.servers' = '"+  kafkaProperties.get("bootstrap.servers") +"',\n" +
//					"  'properties.group.id' = 'kdaConsumerGroup',\n" +
//					"  'scan.startup.mode' = 'earliest-offset',\n" +
//					"  'value.format' = 'debezium-json'\n" +
//					")";

			//hive catalog
//			final String icebergCatalog = String.format("CREATE CATALOG hive_catalog WITH (\n" +
//					"   'type'='iceberg',\n" +
//					"   'warehouse'='s3://myemr-bucket-01/data/iceberg-folder/',\n" +
//					"   'catalog-impl'='hive',\n" +
//					"   'clients'='5'," +
//					"	'uri'='thrift://ip-10-192-11-165.ec2.internal:9083'" +
//					" )");

			//在KDA中使用Glue catalog 存在问题
			final String icebergCatalog = String.format("CREATE CATALOG glue_catalog WITH (\n" +
					"   'type'='iceberg',\n" +
					"   'warehouse'='s3://myemr-bucket-01/data/iceberg-folder/',\n" +
					"   'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',\n" +
					"   'io-impl'='org.apache.iceberg.aws.s3.S3FileIO'\n" +
					" )");
			streamTableEnvironment.executeSql(icebergCatalog);

			final String sourceSQL = String.format("CREATE TABLE default_catalog.default_database.customer_info \n" +
					"(\n" +
					"    id BIGINT,\n" +
					"    user_name STRING,\n" +
					"    country STRING,\n" +
					"    province STRING,\n" +
					"    city BIGINT,\n" +
					"    street STRING,\n" +
					"    street_name STRING,\n" +
					"    created_at TIMESTAMP_LTZ(3),\n" +
					"    updated_at TIMESTAMP_LTZ(3),\n" +
					"    company STRING,\n" +
					"    PRIMARY KEY (id) NOT ENFORCED \n" +
					") WITH ( \n" +
					"    'connector' = 'mysql-cdc', \n" +
					"    'hostname' = 'mysql-db-01-v8.cghfgy0zyjlk.us-east-1.rds.amazonaws.com', \n" +
					"    'port' = '3306', \n" +
					"    'username' = 'admin', \n" +
					"    'password' = 'Amazon123', \n" +
					"    'database-name' = 'norrisdb', \n" +
					"    'table-name' = 'customer_info' \n" +
					")");
			streamTableEnvironment.executeSql(sourceSQL);

			final String s3Sink = "CREATE TABLE glue_catalog.iceberg_db.customer_info_flinksql(\n" +
					"    id BIGINT,\n" +
					"    user_name STRING,\n" +
					"    country STRING,\n" +
					"    province STRING,\n" +
					"    city BIGINT,\n" +
					"    street STRING,\n" +
					"    street_name STRING,\n" +
					"    created_at timestamp_ltz(3),\n" +
					"    updated_at timestamp_ltz(3),\n" +
					"    company STRING,\n" +
					"    PRIMARY KEY (id) NOT ENFORCED)\n" +
					"with(\n" +
					"   'format-version'='2' \n" +
//					"    'connector'='iceberg' \n" +
//					"    'catalog-name'='hive_catalog',\n" +
//					"    'catalog-database'='icebergdb',\n" +
//					"    'catalog-table'='customer_info_flinksql',\n" +
//					"    'catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',\n" +
//					"    'warehouse'='s3://myemr-bucket-01/data/iceberg-folder/'\n" +
					")";

			streamTableEnvironment.executeSql(s3Sink);

			final String insertSql = "insert into glue_catalog.iceberg_db.customer_info_flinksql \n" +
					"select * from default_catalog.default_database.customer_info";
			streamTableEnvironment.executeSql(insertSql);
		}
	}

}



