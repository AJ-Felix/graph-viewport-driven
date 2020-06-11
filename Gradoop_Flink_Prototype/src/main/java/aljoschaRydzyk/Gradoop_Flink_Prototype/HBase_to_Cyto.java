package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class HBase_to_Cyto {
	public static void main(String[] args) {
		
		//create Flink Table Stream Configuration
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
		
		//create vertices Cytoscype CSV file
		String hbase_table_name_vertices = "test_vertices_cyto2";
		fsTableEnv.sqlUpdate(
				"CREATE TABLE loadTempTableVertices (" + 
				"  id INT," + 
				"  cf ROW<v_label STRING, group_id INT, X STRING, Y STRING>" +  
				") WITH (" + 
				"  'connector.type' = 'hbase'," + 
				"  'connector.version' = '1.4.3'," + 
				"  'connector.table-name' = '" + hbase_table_name_vertices + "'," +
				"  'connector.zookeeper.quorum' = 'localhost:2181'" +
				")");
		String query_vertices = "SELECT * from loadTempTableVertices";
		Table table_vertices = fsTableEnv.sqlQuery(query_vertices);
		RowTypeInfo rowTypeInfo_vertices = new RowTypeInfo(new TypeInformation[]{Types.INT, Types.ROW(Types.STRING, Types.INT, Types.STRING, Types.STRING)},
				new String[] {"v_id", "rest"});
		DataStream<Row> dsRow_vertices = fsTableEnv.toAppendStream(table_vertices, rowTypeInfo_vertices);
		TypeInformation<String> ds_string_typeInfo_vertices = TypeInformation.of(String.class);
		DataStream<String> dsString_vertices = dsRow_vertices.map(new MapFunction<Row, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public String map(Row value) throws Exception {
				Integer v_id = (Integer) value.getField(0);
				String label = (String) ((Row) value.getField(1)).getField(0);
				Integer group = (Integer) ((Row) value.getField(1)).getField(1);
				String X = (String) ((Row) value.getField(1)).getField(2);
				String Y = (String) ((Row) value.getField(1)).getField(3);
				return new String(v_id + " " + label + " "+ group + " " + X+ " " + Y);
			}
		}).returns(ds_string_typeInfo_vertices);
		dsString_vertices.writeAsText("/home/aljoscha/test_vertices_cyto2").setParallelism(1);
		
		//create edges Cytoscap CSV file
		String hbase_table_name_edges = "test_edges_cyto";
		fsTableEnv.sqlUpdate(
				"CREATE TABLE loadTempTableEdges (" + 
				"  edge_id STRING," + 
				"  cf ROW<v_id_source STRING, v_id_target STRING>" +  
				") WITH (" + 
				"  'connector.type' = 'hbase'," + 
				"  'connector.version' = '1.4.3'," + 
				"  'connector.table-name' = '" + hbase_table_name_edges + "'," +
				"  'connector.zookeeper.quorum' = 'localhost:2181'" +
				")");
		String query_edges = "SELECT * from loadTempTableEdges";
		Table table_edges = fsTableEnv.sqlQuery(query_edges);
		RowTypeInfo rowTypeInfo_edges = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.ROW(Types.STRING, Types.STRING)},
				new String[] {"edge_id", "rest"});
		DataStream<Row> dsRow_edges = fsTableEnv.toAppendStream(table_edges, rowTypeInfo_edges);
		TypeInformation<String> ds_string_typeInfo_edges = TypeInformation.of(String.class);
		DataStream<String> dsString_edges = dsRow_edges.map(new MapFunction<Row, String>(){
			private static final long serialVersionUID = 1L;
			@Override
			public String map(Row value) throws Exception {
				String edge_id = value.getField(0).toString();
				String v_id_source = (String) ((Row) value.getField(1)).getField(0);
				String v_id_target = (String) ((Row) value.getField(1)).getField(1);
				return new String(edge_id + " " + v_id_source + " "+ v_id_target);
			}
		}).returns(ds_string_typeInfo_edges);
		dsString_edges.writeAsText("/home/aljoscha/test_vertices_cyto").setParallelism(1);
		
		try {
			fsEnv.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
