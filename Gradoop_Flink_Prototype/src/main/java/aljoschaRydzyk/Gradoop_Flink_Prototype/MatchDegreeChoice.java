package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.flink.addons.hbase.HBaseOptions;
import org.apache.flink.addons.hbase.HBaseTableSchema;
import org.apache.flink.addons.hbase.HBaseUpsertTableSink;
import org.apache.flink.addons.hbase.HBaseWriteOptions;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.AggregatedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.HBase;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

public class MatchDegreeChoice {
	
	public static class VertexAccum {
		String bool;
		String v_id;
		String degree;
	}
	
	//create AggregateFunction to make downstream join on single rows possible
	public static class CurrentVertex extends AggregateFunction<Tuple2<String, String>, VertexAccum>{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public VertexAccum createAccumulator() {
			return new VertexAccum();
		}

		@Override
		public Tuple2<String, String> getValue(VertexAccum accumulator) {
			return new Tuple2<String, String>(accumulator.bool, accumulator.degree);
		}
		
		public void accumulate(VertexAccum accumulator, String bool,  String v_id, String degree) {
			accumulator.bool = bool;
			accumulator.v_id = v_id;
			accumulator.degree = degree;
		}
	}
	
	public static void match(
			StreamExecutionEnvironment fsEnv,
			StreamTableEnvironment fsTableEnv, 
			DataStream<Tuple2<Boolean, Row>> ds_degree,
			LogicalGraph log) throws Exception {
		
		//create flink table from degree hbase table
		TupleTypeInfo<Tuple3<String, String,String>> ds_tuple_typeInfo = new TupleTypeInfo<Tuple3<String, String, String>>(new TypeInformation[]{Types.STRING,Types.STRING, Types.STRING});
		
		//convert DataStream to enable attribute comparison inn table join
		DataStream<Tuple3<String,String,String>> ds_tuple = ds_degree.map(new MapFunction<Tuple2<Boolean,Row>, Tuple3<String,String,String>>() {
			private static final long serialVersionUID = -7026515741245426370L;

			@Override
			public Tuple3<String, String, String> map(Tuple2<Boolean, Row> value) throws Exception {
				String b = value.f0.toString();
				String i = value.f1.getField(0).toString();
				String d = ((Row) value.f1.getField(1)).getField(0).toString();
				// TODO Auto-generated method stub
				return new Tuple3<String, String, String>(b, i, d);
			}
		}).returns(ds_tuple_typeInfo).setParallelism(1);				//parallelism must be set to 1, else the order of true/false statements is wrong and aggregation wont work
		Table table = fsTableEnv.fromDataStream(ds_tuple).as("bool, v_id, degree");
		//debugging: convert table to stream and print
		RowTypeInfo rowTypeInfo_debug_vertices = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING}, 
				new String[] {"bool", "v_id", "degree"});
		DataStream<Row> ds_stream_debug_vertices = fsTableEnv.toAppendStream(table, rowTypeInfo_debug_vertices);
//		ds_stream_debug_vertices.print().setParallelism(1);
		
		
		fsTableEnv.registerFunction("currentVertex", new CurrentVertex());
		AggregatedTable table7 = table.groupBy("v_id").aggregate("currentVertex(bool, v_id, degree) as (bool, degree)");		//sammeln von bool verändert Verhalten kritisch!
		Table table8 = table7.select("v_id, degree, bool").filter("bool = 'true'");
		
		//debugging: convert table to stream and print
		RowTypeInfo rowTypeInfo_debug2_vertices = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING}, 
				new String[] {"v_id", "degree", "bool"});
		DataStream<Tuple2<Boolean, Row>> ds_stream_debug2_vertices = fsTableEnv.toRetractStream(table8, rowTypeInfo_debug2_vertices);
//		ds_stream_debug2_vertices.print().setParallelism(1);

		
		//create flink table of vertices from LogicalGraph
		DataSet<EPGMVertex> dset_vertices = log.getVertices();
		System.out.println("Initial vertices count: " + ((Long) dset_vertices.count()).toString());
		List<EPGMVertex> list_vertices = dset_vertices.collect();
		Map<String, String> map = new HashMap<String, String>();
		List<Tuple5<String, String, Integer, String, String>> list_tuple_vertices = new ArrayList<Tuple5<String, String, Integer, String, String>>();
		for (int i = 0; i < list_vertices.size(); i++) {
			String vertex_id = list_vertices.get(i).getId().toString();
			map.put(vertex_id, ((Integer) i).toString());
			Integer x = list_vertices.get(i).getPropertyValue("X").getInt();
			String x_str = x.toString();
			Integer y = list_vertices.get(i).getPropertyValue("Y").getInt();
			String y_str = y.toString();
			String label = list_vertices.get(i).getLabel();
			list_tuple_vertices.add(new Tuple5<String, String, Integer, String, String>(vertex_id, label, i, x_str, y_str));
		}	
		DataStreamSource<Tuple5<String, String, Integer, String, String>> ds_tuple_vertices = fsEnv.fromCollection(list_tuple_vertices);
		Table table3 = fsTableEnv.fromDataStream(ds_tuple_vertices).as("v_id_2, v_label, v_id_layout, X, Y");
		
		//create flink table of edges from LogicalGraph
//		DataSet<EPGMEdge> ds_edges = log.getEdges();
//		List<EPGMEdge> list_edges = ds_edges.collect();
//		List<Tuple5<String, String, String, String, String>> list_tuple_edges = new ArrayList<Tuple5<String, String, String, String, String>>();
//		for (int i = 0; i < list_edges.size(); i++) {
//			String id = list_edges.get(i).getId().toString();
//			String node_1 = list_edges.get(i).getSourceId().toString();
//			String i_1 = map.get(node_1);
//			String node_2 = list_edges.get(i).getTargetId().toString();		
//			String i_2 = map.get(node_2);
//			Tuple5<String, String, String, String, String> tuple = Tuple5.of(id, node_1, node_2, i_1, i_2);
//			list_tuple_edges.add(tuple);
//		}
//		DataStreamSource<Tuple5<String, String, String, String, String>> ds_tuple_edges = fsEnv.fromCollection(list_tuple_edges);
//		Table table5 = fsTableEnv.fromDataStream(ds_tuple_edges).as("edge_id, v_id_source, v_id_target, v_id_source_new, v_id_target_new");
		
		//table joins for vertex table
		Table table6 = table8.join(table3).where("v_id = v_id_2").select("v_label, X, Y, v_id_layout, degree, v_id");
		
		//debugging: convert table to stream and print
		RowTypeInfo rowTypeInfo_debug3_vertices = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.STRING}, 
				new String[] {"v_label", "X", "Y", "v_id_layout", "degree", "v_id"});
		DataStream<Tuple2<Boolean, Row>> ds_stream_debug3_vertices = fsTableEnv.toRetractStream(table6, rowTypeInfo_debug3_vertices);
//		ds_stream_debug3_vertices.print().setParallelism(1);
		
		//table joins for edges table
//		Table table9 = table8.join(table5).where("v_id = v_id_source").select("edge_id, v_id_source, v_id_target, v_id_source_new, v_id_target_new");
//		Table table10 = table8.join(table9).where("v_id = v_id_target").select("edge_id, v_id_source_new, v_id_target_new");
		
		//convert joined vertex table to sinkable data stream
		RowTypeInfo rowTypeInfo_vertices = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.STRING, Types.STRING}, 
				new String[] {"v_label", "X", "Y", "v_id_layout", "degree", "v_id"});
		DataStream<Tuple2<Boolean, Row>> ds_stream_vertices = fsTableEnv.toRetractStream(table6, rowTypeInfo_vertices);
		
		//convert joined vertex table to String stream 
//		TypeInformation<String> stringInfo = TypeInformation.of(new TypeHint<String>() {});
//		DataStream<String> string_stream_vertices = ds_stream_vertices.map(new MapFunction<Tuple2<Boolean,Row>, String>() {
//			private static final long serialVersionUID = -7023515741245426370L;
//			@Override
//			public String map(Tuple2<Boolean, Row> value) throws Exception {
//				String result = value.f0.toString() + " " + value.f1.getField(3) + " " + value.f1.getField(0) + " " + value.f1.getField(3) + " " + 
//							value.f1.getField(1) + " " + value.f1.getField(2);
//				return result;
//			}
//		}).returns(stringInfo);
		
		//sink vertex string stream to file by redirecting stdout			CAUTION: Does still not have exactly-once semantic
//		string_stream_vertices.print().setParallelism(1);
		//writeAsCsv also does not provide exactly one semantic
//		ds_stream_vertices.writeAsCsv("/home/aljoscha/debug/tmp/tables/test_run8_vertices");

		//sink vertex string stream to file using StreamingFileSink which has exactly-once semantic --- DOES NOT MATERIALIZE FILES!!!
//		Encoder<String> encoder = new SimpleStringEncoder<String>("UTF-8");
//		final StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(new Path("/home/aljoscha/debug/tmp/tables/test_run9_vertices"), encoder)
//				.withRollingPolicy(
//						DefaultRollingPolicy.builder()
//			            .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//			            .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//			            .withMaxPartSize(1024 * 1024 * 1024)
//			            .build())
//				.build();
//		string_stream_vertices.addSink(sink);
	
		//convert joined edge table to sinkable data stream
//		RowTypeInfo rowTypeInfo_edges = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING}, 
//				new String[] {"edge_id", "v_id_source", "v_id_target"});
//		DataStream<Tuple2<Boolean, Row>> ds_stream_edges = fsTableEnv.toRetractStream(table10, rowTypeInfo_edges);
		
		//convert joined vertex table stream to HBase-compatible data stream			CAUTION: Apparently also does not have exactly-once semantics
		RowTypeInfo vertices_nested_rowTypeInfo = new RowTypeInfo(new TypeInformation[]{Types.INT, Types.ROW(Types.STRING, Types.INT, Types.STRING, Types.STRING)});
		TupleTypeInfo<Tuple2<Boolean, Row>> vertices_nested_tuple_typeInfo = new TupleTypeInfo<Tuple2<Boolean, Row>>(Types.BOOLEAN, vertices_nested_rowTypeInfo);
		DataStream<Tuple2<Boolean, Row>> ds_stream_nested_vertices = ds_stream_vertices.map(new MapFunction<Tuple2<Boolean,Row>, Tuple2<Boolean,Row>>() {
			private static final long serialVersionUID = -7026515741345426370L;

			@Override
			public Tuple2<Boolean, Row> map(Tuple2<Boolean, Row> value) throws Exception {
				Integer rowkey = (Integer) value.f1.getField(3);
				return new Tuple2<Boolean, Row>(value.f0, Row.of(rowkey, Row.of(value.f1.getField(0), rowkey, value.f1.getField(1), value.f1.getField(2))));
			}


		}).returns(vertices_nested_tuple_typeInfo);
		//declare HBaseTableUpsertSink and sink data to HBase
		// order is important!!
		HBaseTableSchema hbaseTableSchema = new HBaseTableSchema();
		hbaseTableSchema.setRowKey("v_id_layout", Integer.class);
		hbaseTableSchema.addColumn("cf", "v_label", String.class);
		hbaseTableSchema.addColumn("cf", "group_id", Integer.class);
		hbaseTableSchema.addColumn("cf", "X", String.class);
		hbaseTableSchema.addColumn("cf", "Y", String.class);
		HBaseOptions hbaseOptions = new HBaseOptions.Builder().setZkQuorum("localhost:2181").setTableName("test_vertices_cyto4").build();
		HBaseWriteOptions hbaseWriteOptions = new HBaseWriteOptions.Builder().build();
		HBaseUpsertTableSink hbaseUpsertTableSink = new HBaseUpsertTableSink(hbaseTableSchema, hbaseOptions, hbaseWriteOptions);	
		hbaseUpsertTableSink.emitDataStream(ds_stream_nested_vertices);
		
		//convert joined edges table stream to HBase-compatible data stream			CAUTION: Apparently also does not have exactly-once semantics
//		RowTypeInfo edges_nested_rowTypeInfo = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.ROW(Types.STRING, Types.STRING)});
//		TupleTypeInfo<Tuple2<Boolean, Row>> edges_nested_tuple_typeInfo = new TupleTypeInfo<Tuple2<Boolean, Row>>(Types.BOOLEAN, edges_nested_rowTypeInfo);
//		DataStream<Tuple2<Boolean, Row>> ds_stream_nested_edges = ds_stream_edges.map(new MapFunction<Tuple2<Boolean,Row>, Tuple2<Boolean,Row>>() {
//			private static final long serialVersionUID = -7026515741345426370L;
//			@Override
//			public Tuple2<Boolean, Row> map(Tuple2<Boolean, Row> value) throws Exception {
//				String rowkey = value.f1.getField(0).toString();
//				return new Tuple2<Boolean, Row>(value.f0, Row.of(rowkey, Row.of(value.f1.getField(1), value.f1.getField(2))));
//			}
//		}).returns(edges_nested_tuple_typeInfo);
		//declare HBaseTableUpsertSink and sink data to HBase
		// order is important!!
		
//		ds_stream_nested_edges.print().setParallelism(1);
//		
//		HBaseTableSchema hbaseTableSchema = new HBaseTableSchema();
//		hbaseTableSchema.setRowKey("edge_id", String.class);
//		hbaseTableSchema.addColumn("cf", "v_id_source", String.class);
//		hbaseTableSchema.addColumn("cf", "v_id_target", String.class);
//		HBaseOptions hbaseOptions = new HBaseOptions.Builder().setZkQuorum("localhost:2181").setTableName("test_edges_cyto6").build();
//		HBaseWriteOptions hbaseWriteOptions = new HBaseWriteOptions.Builder().build();
//		HBaseUpsertTableSink hbaseUpsertTableSink = new HBaseUpsertTableSink(hbaseTableSchema, hbaseOptions, hbaseWriteOptions);	
//		hbaseUpsertTableSink.emitDataStream(ds_stream_nested_edges);
	}
}
