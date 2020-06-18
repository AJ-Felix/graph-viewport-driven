package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.AggregatedTable;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;



public class DegreeMatcher {
	
	//define Vertex Accumulator Object
	public static class VertexAccum {
		String bool;
		String v_id;
		String degree;
	}
	
	//define Vertex AggregateFunction Object to make downstream join on single rows possible
	public static class CurrentVertex extends AggregateFunction<Tuple2<String, String>, VertexAccum>{

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
	
	public static ArrayList<DataStream<Tuple2<Boolean, Row>>> match(
			StreamExecutionEnvironment fsEnv,
			StreamTableEnvironment fsTableEnv, 
			DataStream<Tuple2<Boolean, Row>> datastream_degree,
			LogicalGraph log) throws Exception {
		
		//create flink table from degree hbase table
		TupleTypeInfo<Tuple3<String, String,String>> datastream_degree_converted_info = 
			new TupleTypeInfo<Tuple3<String, String, String>>(new TypeInformation[]{Types.STRING,Types.STRING, Types.STRING});
		
			//convert DataStream to enable attribute comparison in table join
			DataStream<Tuple3<String,String,String>> datastream_degree_converted = datastream_degree.map(new MapFunction<Tuple2<Boolean,Row>, Tuple3<String,String,String>>() {
				private static final long serialVersionUID = -7026515741245426370L;
	
				@Override
				public Tuple3<String, String, String> map(Tuple2<Boolean, Row> value) throws Exception {
					String b = value.f0.toString();
					String i = value.f1.getField(0).toString();
					String d = ((Row) value.f1.getField(1)).getField(0).toString();
					return new Tuple3<String, String, String>(b, i, d);
				}
			}).returns(datastream_degree_converted_info);
			Table degree_table = fsTableEnv.fromDataStream(datastream_degree_converted).as("bool, v_id, degree");
			fsTableEnv.registerFunction("currentVertex", new CurrentVertex());
			AggregatedTable degree_table_aggregated = degree_table.groupBy("v_id").aggregate("currentVertex(bool, v_id, degree) as (bool, degree)");		
				//sammeln von bool verändert Verhalten kritisch!
			Table degree_table_filtered_aggregate = degree_table_aggregated.select("v_id, degree, bool").filter("bool = 'true'");
		
		//create flink vertices table from LogicalGraph
		DataSet<EPGMVertex> dataset_vertices = log.getVertices();
		List<EPGMVertex> list_vertices = dataset_vertices.collect();
		Map<String, String> map = new HashMap<String, String>();
		
			//convert EPGMVertex Object to Flink Tuple and build table from stream
			List<Tuple5<String, String, Integer, String, String>> list_converted_vertices = new ArrayList<Tuple5<String, String, Integer, String, String>>();
			for (int i = 0; i < list_vertices.size(); i++) {
				String vertex_id = list_vertices.get(i).getId().toString();
				map.put(vertex_id, ((Integer) i).toString());
				Integer x = list_vertices.get(i).getPropertyValue("X").getInt();
				String x_str = x.toString();
				Integer y = list_vertices.get(i).getPropertyValue("Y").getInt();
				String y_str = y.toString();
				String label = list_vertices.get(i).getLabel();
				list_converted_vertices.add(new Tuple5<String, String, Integer, String, String>(vertex_id, label, i, x_str, y_str));
			}	
			DataStreamSource<Tuple5<String, String, Integer, String, String>> datastream_converted_vertices = fsEnv.fromCollection(list_converted_vertices);
			Table vertex_table = fsTableEnv.fromDataStream(datastream_converted_vertices).as("v_id_2, v_label, v_id_layout, X, Y");
		
		//table joins for vertex table
		Table vertex_result_table = degree_table_filtered_aggregate.join(vertex_table).where("v_id = v_id_2").select("v_label, X, Y, v_id_layout, degree, v_id");
		
		//convert joined vertex table to data stream
		RowTypeInfo rowTypeInfo_vertices = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING}, 
				new String[] {"v_id", "X", "Y", "degree"});
		DataStream<Tuple2<Boolean, Row>> stream_vertices = fsTableEnv.toRetractStream(vertex_result_table, rowTypeInfo_vertices);
		
		//create flink edge table from LogicalGraph
		DataSet<EPGMEdge> datastream_edges = log.getEdges();
		List<EPGMEdge> list_edges = datastream_edges.collect();
		List<Tuple5<String, String, String, String, String>> list_converted_edges = new ArrayList<Tuple5<String, String, String, String, String>>();
		for (int i = 0; i < list_edges.size(); i++) {
			String id = list_edges.get(i).getId().toString();
			String node_1 = list_edges.get(i).getSourceId().toString();
			String i_1 = map.get(node_1);
			String node_2 = list_edges.get(i).getTargetId().toString();		
			String i_2 = map.get(node_2);
			Tuple5<String, String, String, String, String> tuple = Tuple5.of(id, node_1, node_2, i_1, i_2);
			list_converted_edges.add(tuple);
		}
		DataStreamSource<Tuple5<String, String, String, String, String>> datastream_converted_edges = fsEnv.fromCollection(list_converted_edges);
		Table edge_table = fsTableEnv.fromDataStream(datastream_converted_edges).as("edge_id, v_id_source, v_id_target, v_id_source_new, v_id_target_new");
		
		//table joins for edges table
		Table edge_table_first_join = degree_table_filtered_aggregate.join(edge_table).where("v_id = v_id_source")
				.select("edge_id, v_id_source, v_id_target, v_id_source_new, v_id_target_new");
		Table edge_result_table = degree_table_filtered_aggregate.join(edge_table_first_join).where("v_id = v_id_target")
				.select("edge_id, v_id_source_new, v_id_target_new");
		
		//convert joined edge table to data stream
		RowTypeInfo rowTypeInfo_edges = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING}, 
				new String[] {"edge_id", "v_id_source", "v_id_target"});
		DataStream<Tuple2<Boolean, Row>> stream_edges = fsTableEnv.toRetractStream(edge_result_table, rowTypeInfo_edges);
		
		//process vertex stream for future use
		datastream_converted_vertices.countWindowAll(50);
		
		
		ArrayList<DataStream<Tuple2<Boolean, Row>>> result_list = new ArrayList<DataStream<Tuple2<Boolean, Row>>>();
		result_list.add(stream_vertices);
		result_list.add(stream_edges);
		return result_list;
	}
}
