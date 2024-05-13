package com.amazonaws.sample;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KDS_to_Hudi {

    private static final Logger LOG = LoggerFactory.getLogger(HudiApplication.class);


    private static String _region = "us-east-1";
    private static String _inputStreamName = "ExampleInputStream";
    private static String _s3SinkPath = "s3a://ka-app-code-<username>/data";

    private static String _catalogPath = "s3a://ka-app-code-<username>/catalog";


    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        final ParameterTool parameter = ParameterTool.fromArgs(args);

        _inputStreamName = args[0];
        _region = args[1];
        _s3SinkPath = args[2];
        _catalogPath = args[3];


        KafkaHudiSqlExample.createAndDeployJob(env);
        //Process stream using sql API
    }


    public static class KafkaHudiSqlExample {

        public static void createAndDeployJob(StreamExecutionEnvironment env)  {
            StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().build());

            Configuration configuration = streamTableEnvironment.getConfig().getConfiguration();
            configuration.setString("execution.checkpointing.interval", "1 min");

            final String glueCatalog = String.format("CREATE CATALOG glue_catalog_for_hudi WITH \n" +
                    "(\n" +
                    "    'type' = 'hudi',\n" +
                    "    'mode' = 'hms',\n" +
                    "    'table.external' = 'true',\n" +
                    "    'default-database' = 'hudi',\n" +
                    "    'hive.conf.dir' = '/etc/hive/conf.dist',\n" +
                    "    'catalog.path' = '%s'\n" +
                    ");", _catalogPath);

            streamTableEnvironment.executeSql(glueCatalog);

            //

            final String sourceSQL = String.format("CREATE TABLE IF NOT EXISTS default_catalog.default_database.kinesis_stream (\n" +
                    "  customerId INT, \n" +
                    "  transactionAmount INT,\n" +
                    "  sourceIp STRING,\n" +
                    "  status STRING,\n" +
                    "  transactionTime TIMESTAMP(3),\n" +
                    "  WATERMARK FOR transactionTime AS transactionTime - INTERVAL '5' SECOND \n" +
                    ") WITH (\n" +
                    "  'connector' = 'kinesis',\n" +
                    "  'stream' = '%s',\n" +
                    "  'aws.region' = '%s',\n" +
                    "  'json.timestamp-format.standard' = 'ISO-8601',\n" +
                    "  'scan.stream.initpos' = 'LATEST',\n" +
                    "  'format' = 'json'\n" +
                    ");", _inputStreamName, _region);
            streamTableEnvironment.executeSql(sourceSQL);

            final String hudi_sink = "CREATE TABLE glue_catalog_for_hudi.hudi.kds_hudi_table_01 (\n" +
                    "  customerId INT, \n" +
                    "  transactionAmount INT,\n" +
                    "  sourceIp STRING,\n" +
                    "  status STRING,\n" +
                    "  transactionTime TIMESTAMP(3),\n" +
                    " PRIMARY KEY (customerId,transactionTime) NOT ENFORCED \n" +
                    "    ) \n" +
                    "    WITH (\n" +
                    "    'connector' = 'hudi',\n" +
                    "    'compaction.tasks'='2',\n" +
                    "    'write.task.max.size'='4096',\n" +
                    "    'write.bucket_assign.tasks'='4',\n" +
                    "    'compaction.delta_seconds'='120',\n" +
                    "    'compaction.delta_commits'='2',\n" +
                    "    'hoodie.clustering.inline'='false', \n" +
                    "    'hoodie.clustering.schedule.inline'='false', \n" +
                    "    'hoodie.clustering.async.enabled'='true', \n" +
                    "    'compaction.trigger.strategy'='num_or_time',\n" +
                    "    'compaction.max_memory'='2048',\n" +
                    "    'write.merge.max_memory'='1024',\n" +
                    "    'write.tasks' = '4',\n" +
                    "    'hive_sync.enable' = 'true',\n" +
                    "    'hive_sync.db' = 'hudi',\n" +
                    "    'hive_sync.table' = 'kds_hudi_table_01',\n" +
                    "    'hive_sync.mode' = 'glue',\n" +
                    "    'hive_sync.use_jdbc' = 'false',\n" +
                    "    'path' = '" + _s3SinkPath + "',\n" +
                    "    'table.type' = 'COPY_ON_WRITE', \n" +
                    "    'hoodie.datasource.write.operation' = 'insert' \n" +
                    "    )";

            streamTableEnvironment.executeSql(hudi_sink);

            final String insertSql = "insert into glue_catalog_for_hudi.hudi.kds_hudi_table_01 " +
                    "select * from default_catalog.default_database.kinesis_stream";
            streamTableEnvironment.executeSql(insertSql);
        }
    }
}
