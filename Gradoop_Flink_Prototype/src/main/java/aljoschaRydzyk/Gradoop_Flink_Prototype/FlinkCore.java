package aljoschaRydzyk.Gradoop_Flink_Prototype; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;

public class FlinkCore {
	  private ExecutionEnvironment env;
	  private GradoopFlinkConfig graflink_cfg;
	  private GradoopHBaseConfig gra_hbase_cfg;
	  private Configuration hbase_cfg;
	  private EnvironmentSettings fsSettings;
	  private StreamExecutionEnvironment fsEnv;
	  private StreamTableEnvironment fsTableEnv;
	  
	  private LogicalGraph graph = null;

	public  FlinkCore () {
		this.env = ExecutionEnvironment.getExecutionEnvironment();
	    this.graflink_cfg = GradoopFlinkConfig.createConfig(env);
		this.gra_hbase_cfg = GradoopHBaseConfig.getDefaultConfig();
		this.hbase_cfg = HBaseConfiguration.create();
		this.fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		this.fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		this.fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
	}
	
	public StreamExecutionEnvironment getFsEnv() {
		return this.fsEnv;
	}
	
	public ArrayList<DataStream> buildTopView (){
		DataStream<Tuple2<Boolean, Row>> ds_degree = DegreeMatrix_Loader.load(this.fsTableEnv, "degree_vertices_10_third", 50);
		GraFlink_Graph_Loader loader = new GraFlink_Graph_Loader(this.graflink_cfg, this.gra_hbase_cfg, this.hbase_cfg);
		List<DataStream<Tuple2<Boolean, Row>>> graph_data_streams = null;
		ArrayList<DataStream> list_ds = null;
		try {
			graph = loader.getLogicalGraph("5ebe6813a7986cc7bd77f9c2");	//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
			DataStream<Tuple5<String, String, String, String, String>> dstream_vertices = this.getVertexStream();
			graph_data_streams = DegreeMatcher.match(this.fsEnv, this.fsTableEnv, ds_degree, graph);
			list_ds = new ArrayList();
			list_ds.addAll(graph_data_streams);
			list_ds.add(dstream_vertices);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return list_ds;
		
	}
	
	public DataStream<Tuple5<String, String, String, String, String>> getVertexStream() throws Exception {
		//create flink vertices table from LogicalGraph
		DataSet<EPGMVertex> dataset_vertices = graph.getVertices();
		List<EPGMVertex> list_vertices = dataset_vertices.collect();
		Map<String, String> map = new HashMap<String, String>();
	
		//convert EPGMVertex Object to Flink Tuple and build table from stream
		List<Tuple5<String, String, String, String, String>> list_converted_vertices = new ArrayList<Tuple5<String, String, String, String, String>>();
		for (int i = 0; i < list_vertices.size(); i++) {
			String vertex_id = list_vertices.get(i).getId().toString();
			map.put(vertex_id, ((Integer) i).toString());
			Integer x = list_vertices.get(i).getPropertyValue("X").getInt();
			String x_str = x.toString();
			Integer y = list_vertices.get(i).getPropertyValue("Y").getInt();
			String y_str = y.toString();
			String label = list_vertices.get(i).getLabel();
			list_converted_vertices.add(new Tuple5<String, String, String, String, String>(vertex_id, label, ((Integer) i).toString(), x_str, y_str));
		}	
		DataStreamSource<Tuple5<String, String, String, String, String>> datastream_converted_vertices = fsEnv.fromCollection(list_converted_vertices);
		return datastream_converted_vertices;
	}
	
	public DataStream<Tuple5<String, String, String, String, String>> zoomIn (DataStream<Tuple5<String, String, String, String, String>> dstream_vertices, Integer top, Integer right, Integer bottom, Integer left) throws Exception{
		System.out.println("in zoomIN");
//		DataStream<Tuple5<String, String, String, String, String>> dstream_vertices = this.getVertexStream();
//		dstream_vertices.print();
		DataStream<Tuple5<String, String, String, String, String>> dstream_vertices_filtered = dstream_vertices
				.filter(new FilterFunction<Tuple5<String, String, String, String, String>>(){
			@Override
			public boolean filter(Tuple5<String, String, String, String, String> value) throws Exception {
				Integer x = Integer.parseInt(value.f3);
				Integer y = Integer.parseInt(value.f4);
				return (left < x) &&  (x < right) && (top < y) && (y < bottom);
			}
			
		});
		System.out.println("Filtering done");
		return dstream_vertices_filtered;
	}
}
