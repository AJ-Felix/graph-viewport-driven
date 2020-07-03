package Temporary;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.BasicConfigurator;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.table.descriptors.HBase;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.sinks.CsvBatchTableSinkFactory;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class Flink_Hbase {
	public static void main(String[] args) throws Exception {
		
		BasicConfigurator.configure();
		
		//connect Flink to HBase
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
		StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
		
//		CsvTableSource csvTable = CsvTableSource.builder()
//				.path("C:\\Daten\\\\Masterarbeit\\TestDaten\\test_csv.csv")
//				.field("counter", DataTypes.INT())
//				.field("description", DataTypes.STRING())
//				.build();
		Schema schema = new Schema()
				.field("counter", DataTypes.STRING())
				.field("description", DataTypes.STRING());
		try {
	         File file = new File("C:\\Daten\\Masterarbeit\\TestDaten\\test_csv.csv");
	         System.out.println(file.exists());
	      } catch(Exception e) {
	         e.printStackTrace();
	      }
//		fsTableEnv.connect(new FileSystem()
//				.path("C:\\Daten\\Masterarbeit\\TestDaten\\test_csv.csv"))
//				.withFormat(new OldCsv().field("counter", DataTypes.STRING()).field("description", DataTypes.STRING()).fieldDelimiter(";"))
//				.withSchema(schema)
//				.createTemporaryTable("inputTable");
//		Table inputTable = fsTableEnv.from("inputTable");
//		inputTable.map(new MapFunction<Row, Row>() {
//		    @Override
//		    public Row map(Row value) throws Exception {
//		    	Row row = new Row(0, new Row(value.getField(0), value.getField(1)));
//		        return row;
//		    }
//		});

		List<Row> testData1 = new ArrayList<>();
		RowTypeInfo testTypeInfo1 = new RowTypeInfo(new TypeInformation[]{Types.INT, Types.ROW(Types.INT, Types.STRING)}, 
				new String[] {"rowkey", "row"});
		testData1.add(Row.of(1, Row.of(1, "blasd")));
		testData1.add(Row.of(2, Row.of(33, "text")));
		DataStream<Row> dsRow = fsEnv.fromCollection(testData1).returns(testTypeInfo1);
		new Row(2);
//		fsTableEnv.createTemporaryView("inputTable", dsRow);
		Table inputTable = fsTableEnv.fromDataStream(dsRow);
		inputTable.printSchema();
//		DataStream<Row> dsRow = fsTableEnv.toAppendStream(inputTable, Row.class);
//		dsRow.print();
//		dsRow.map(new MapFunction<Row, Row>() {
//		    @Override
//		    public Row map(Row value) throws Exception {
//		    	Row row = new Row(0, new Row(value.getField(0), value.getField(1)));
//		        return row;
//		    }
//		}
//		);
//		dsRow.print();
//		dsRow.transform("operator", myNestedRowStream.getType(), operator)
//		TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING, Types.STRING);
//				DataStream<Tuple2<String, Integer>> dsTuple = 
//				fsTableEnv.toAppendStream(inputTable, tupleType);
//		dsTuple.print();
//		fsTableEnv.execute("test");
//		System.out.println("\n");
//		inputTable.printSchema();
		
//		ExecutionEnvironment fbenv = ExecutionEnvironment.getExecutionEnvironment();
//		BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(fbenv);										
				//BatchTableEnvironment only allows BatchTableSinks or BatchTableOutputFormat which cannot be created with '.createTemporaryTable'
		
//		HBase hbase = new HBase().version("1.4.3").zookeeperQuorum("localhost:2181").tableName("testTable").zookeeperNodeParent("/home/aljoscha/hbase");
//		FormatDescriptor format = new FormatDescriptor();
//		fsTableEnv.connect(hbase).withSchema(schema).withFormat(new OldCsv().field("counter", DataTypes.INT()).field("description", DataTypes.STRING())).createTemporaryTable("tempTable");
				//"The connector org.apache.flink.table.descriptors.HBase requires a format description" but with format: "Unsupported field for HBaseSinkFactory"
		fsTableEnv.sqlUpdate(
				"CREATE TABLE tempTable (" + 
				"  id INT," + 
				"  cf ROW<counter INT,description STRING>" +  
				") WITH (" + 
				"  'connector.type' = 'hbase'," + 
				"  'connector.version' = '1.4.3'," + 
				"  'connector.table-name' = 'test'," +
				"  'connector.zookeeper.quorum' = 'localhost:2181'" +
				")");

		Table sink = fsTableEnv.from("tempTable");
		sink.printSchema();
		
		inputTable.insertInto("tempTable");
		fsEnv.execute("test");

		
	
	}
}
