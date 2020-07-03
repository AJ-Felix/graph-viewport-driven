package Temporary;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.log4j.BasicConfigurator;

public class Flink_AdjLoader {
public static void main(String[] args) throws Exception {
		
		//Logging
		BasicConfigurator.configure();
		
		//connect Flink to HBase
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
		
		fsTableEnv.sqlUpdate(
				"CREATE TABLE tempTable (" + 
				"  id INT," + 
				"  data_row ROW<column1 INT, column2 INT, column3 INT, column4 INT, column5 INT>" +  
				") WITH (" + 
				"  'connector.type' = 'hbase'," + 
				"  'connector.version' = '1.4.3'," + 
				"  'connector.table-name' = 'adjacency_test'," +
				"  'connector.zookeeper.quorum' = 'localhost:2181'" +
				")");
		Table table = fsTableEnv.sqlQuery("SELECT * from tempTable");
		table.printSchema();
		RowTypeInfo adjListInfo = new RowTypeInfo(new TypeInformation[]{Types.INT, Types.ROW(Types.INT, Types.INT, Types.INT, Types.INT, Types.INT)}, 
				new String[] {"rowkey", "row"});
		DataStream<Row> dsRow = fsTableEnv.toAppendStream(table, adjListInfo);
		dsRow.print();
		
		fsTableEnv.execute("load_adjMatrix");
	}
}
