package com.amazonaws.sample;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Flink CDC 接入 Postgresql 的数据，写入Kafka
 */
public class FlinkCDCPorstgres {

    public static void main(String[] args) {
        //设置flink表环境变量
        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();

        //获取flink流环境变量
        StreamExecutionEnvironment exeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        exeEnv.setParallelism(1);

        //表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(exeEnv, fsSettings);

        Date date = new Date();
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyyMMddhhmmss");
        String curTime = dateFormat.format(date);
        //拼接souceDLL
        String sourceDDL =String.format(
                "CREATE TABLE pgsql_source_portfolio (\n" +
                        "        id INT,\n" +
                        "        reward INT,\n" +
                        "        channels STRING,\n" +
                        "        difficulty STRING,\n" +
                        "        duration STRING,\n" +
                        "        offer_type STRING,\n" +
                        "        offer_id STRING,\n" +
                        "        modify_time STRING,\n" +
                        "        create_time STRING\n" +
                        ") WITH (\n" +
                        " 'connector' = 'postgres-cdc',\n" +
                        " 'hostname' = 'database-1.cghfgy0zyjlk.us-east-1.rds.amazonaws.com',\n" +
                        " 'port' = '5432',\n" +
                        " 'username' = 'postgres',\n" +
                        " 'password' = 'nakYK2fV',\n" +
                        " 'database-name' = 'norrisdb',\n" +
                        " 'schema-name' = 'public',\n" +
                        " 'decoding.plugin.name' = 'pgoutput',\n" +
                        " 'debezium.slot.name' = 'flinksql_%s',\n" +
                        " 'table-name' = 'portfolio'\n" +
                        ")",curTime);

        String strKafkaBostrap = "b-1.mskworkshopcluster.hpukmd.c1.kafka.us-east-1.amazonaws.com:9092,b-2.mskworkshopcluster.hpukmd.c1.kafka.us-east-1.amazonaws.com:9092,b-3.mskworkshopcluster.hpukmd.c1.kafka.us-east-1.amazonaws.com:9092";
        String sinkKafkaDDL = String.format("CREATE TABLE kafka_pg_portfolio_json (\n" +
                "        id INT,\n" +
                "        reward INT,\n" +
                "        channels STRING,\n" +
                "        difficulty STRING,\n" +
                "        duration STRING,\n" +
                "        offer_type STRING,\n" +
                "        offer_id STRING,\n" +
                "        modify_time STRING,\n" +
                "        create_time STRING,\n" +
                "     PRIMARY KEY (id) NOT ENFORCED\n" +
                ") with (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'pg_portfolio',\n" +
                "    'properties.bootstrap.servers' = '%s',\n" +
                "    'properties.group.id' = 'kafka_portfolio_pg_json_gid_001',\n" +
//                "     'kafka.security.protocol' = 'SASL_SSL',\n"  +
//                "     'kafka.sasl.mechanism' = 'software.amazon.msk.auth.iam.IAMLoginModule required;',\n"  +
//                "     'kafka.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler',\n"  +
//                " 'format' = 'changelog-json'\n" +
                " 'format' = 'debezium-json',\n" +
                " 'debezium-json.ignore-parse-errors'='true'\n" +
                ")",strKafkaBostrap);



        String transformSQL =
                "INSERT INTO kafka_pg_portfolio_json " +
                        "SELECT * " +
                        "FROM pgsql_source_portfolio";

        //执行source表ddl
        tableEnv.executeSql(sourceDDL);
        //执行sink表ddl
        tableEnv.executeSql(sinkKafkaDDL);
        //执行逻辑sql语句
        TableResult tableResult = tableEnv.executeSql(transformSQL);

    }
}


