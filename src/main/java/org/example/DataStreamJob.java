
package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


		TableResult resultTable = tableEnv.executeSql("CREATE TABLE user_behavior (\n" +
				"  col0 BIGINT,\n" +
				"  col1 STRING\n" +
				") WITH (\n" +
				" 'connector' = 'filesystem',\n" +
				" 'path' = '/root/data/user_behavior.orc',\n" +
				" 'format' = 'orc'\n" +
				")");
		// interpret the updating Table as a changelog DataStream
		// execute Table
		TableResult tableResult2 = tableEnv.sqlQuery("SELECT * FROM user_behavior").execute();
		tableResult2.print();

	}
}
