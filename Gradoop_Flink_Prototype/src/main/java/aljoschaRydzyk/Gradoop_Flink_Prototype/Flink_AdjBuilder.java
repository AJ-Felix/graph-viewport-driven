package aljoschaRydzyk.Gradoop_Flink_Prototype;

//This class creates a 5x5 matrix in HBASE when main is executed

import java.util.ArrayList;
import java.util.List;

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

public class Flink_AdjBuilder {
	public static void main(String[] args) throws Exception {
		
		//Logging
		BasicConfigurator.configure();
		
		//connect Flink to HBase
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
		
		//Build Adjacency Matrix
		List<Row> adjList = new ArrayList<>();
		RowTypeInfo adjListInfo = new RowTypeInfo(new TypeInformation[]{Types.INT, Types.ROW(Types.INT, Types.INT, Types.INT, Types.INT, Types.INT)}, 
				new String[] {"rowkey", "row"});
		adjList.add(Row.of(1, Row.of(0, 1, 1, 1, 0)));
		adjList.add(Row.of(2, Row.of(1, 0, 1, 0, 0)));
		adjList.add(Row.of(3, Row.of(1, 1, 0, 0, 0)));
		adjList.add(Row.of(4, Row.of(1, 0, 0, 0, 1)));
		adjList.add(Row.of(5, Row.of(0, 0, 0, 1, 0)));
		
		//Convert Matrix to Flink Table
		DataStream<Row> dsRow = fsEnv.fromCollection(adjList).returns(adjListInfo);
		Table inputTable = fsTableEnv.fromDataStream(dsRow);
		
		//Create Flink Table Sink
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
		
		//Sink data to HBase
		inputTable.insertInto("tempTable");
		
		//Execution
		fsEnv.execute("adjacency_to_hbase");
		
	}
}
