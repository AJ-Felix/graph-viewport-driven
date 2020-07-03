package Temporary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

public class Cyto_CSV_Builder {

	public static void build(StreamExecutionEnvironment fsEnv, LogicalGraph log) throws Exception {
		DataSet<EPGMVertex> ds_vertices = log.getVertices();
		List<EPGMVertex> list_vertices = ds_vertices.collect();
		Map<String, Integer> map = new HashMap<String, Integer>();
		List<Tuple5<Integer, String, Integer, String, String>> list_tuple_vertices = new ArrayList<Tuple5<Integer, String, Integer, String, String>>();
		for (int i = 0; i < list_vertices.size(); i++) {
			String vertex_id = list_vertices.get(i).getId().toString();
			map.put(vertex_id, i);
			Integer x = list_vertices.get(i).getPropertyValue("X").getInt();
			String x_str = x.toString();
			Integer y = list_vertices.get(i).getPropertyValue("Y").getInt();
			String y_str = y.toString();
			String label = list_vertices.get(i).getLabel();
			list_tuple_vertices.add(new Tuple5<Integer, String, Integer, String, String>(i, label, i, x_str, y_str));
		}
		DataStreamSource<Tuple5<Integer, String, Integer, String, String>> ds_tuple_vertices = fsEnv
				.fromCollection(list_tuple_vertices);
//		DataSet<Tuple5<Integer, String, Integer, String, String>> ds_tuple_vertices = 
//				ds_vertices.map(new MapFunction<EPGMVertex, Tuple5<Integer, String, Integer, String, String>>() {
//			private static final long serialVersionUID = -7026515741245426370L;
//	
//			@Override
//			public Tuple5<Integer, String, Integer, String, String> map(EPGMVertex vertex) throws Exception {
//				String vertex_id = vertex.getId().toString();
//				Integer i = new Integer(vertex_id, 16);
//				Integer x = vertex.getPropertyValue("X").getInt();
//				String x_str = x.toString();
//				Integer y = vertex.getPropertyValue("Y").getInt();
//				String y_str = y.toString();
//				String label = vertex.getLabel();
//				
//				Tuple5<Integer, String, Integer, String, String> tuple = Tuple5.of(i, label, i, x_str, y_str );
//				return tuple;
//			}
//		});
		ds_tuple_vertices.writeAsCsv("/home/aljoscha/debug/tmp/cyto_csv/vertices", FileSystem.WriteMode.OVERWRITE, "\n", " ").setParallelism(1);

		DataSet<EPGMEdge> ds_edges = log.getEdges();
		List<EPGMEdge> list_edges = ds_edges.collect();
		List<Tuple2<Integer, Integer>> list_tuple_edges = new ArrayList<Tuple2<Integer, Integer>>();
		for (int i = 0; i < list_edges.size(); i++) {
			String node_1 = list_edges.get(i).getSourceId().toString();
			Integer i_1 = map.get(node_1);
			String node_2 = list_edges.get(i).getTargetId().toString();		
			Integer i_2 = map.get(node_2);
			Tuple2<Integer, Integer> tuple = Tuple2.of(i_1, i_2);
				list_tuple_edges.add(tuple);
		}
		DataStreamSource<Tuple2<Integer, Integer>> ds_tuple_edges = fsEnv.fromCollection(list_tuple_edges);
//		DataSet<EPGMEdge> ds_edges = log.getEdges();
//		DataSet<Tuple2<Integer, Integer>> ds_tuple_edges = 
//				ds_edges.map(new MapFunction<EPGMEdge, Tuple2<Integer, Integer>>() {
//			private static final long serialVersionUID = -7026515741245426370L;
//	
//			@Override
//			public Tuple2<Integer, Integer> map(EPGMEdge edge) throws Exception {
//				String node_1 = edge.getSourceId().toString();
//				Integer i_1 = new Integer(node_1, 16);
//				String node_2 = edge.getTargetId().toString();		
//				Integer i_2 = new Integer(node_2, 16);
//				Tuple2<Integer, Integer> tuple = Tuple2.of(i_1, i_2);
//				return tuple;
//			}
//		});
		ds_tuple_edges.writeAsCsv("/home/aljoscha/debug/tmp/cyto_csv/edges", FileSystem.WriteMode.OVERWRITE, "\n", " ").setParallelism(1);
	}
}
