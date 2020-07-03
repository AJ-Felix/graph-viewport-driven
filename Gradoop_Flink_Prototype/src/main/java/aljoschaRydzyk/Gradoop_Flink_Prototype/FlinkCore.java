package aljoschaRydzyk.Gradoop_Flink_Prototype; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.hbase.impl.io.HBaseDataSource;

public class FlinkCore {
	  private ExecutionEnvironment env;
	  private GradoopFlinkConfig graflink_cfg;
	  private GradoopHBaseConfig gra_hbase_cfg;
	  private Configuration hbase_cfg;
	  private EnvironmentSettings fsSettings;
	  private StreamExecutionEnvironment fsEnv;
	  private StreamTableEnvironment fsTableEnv;
	  
	  private GraphUtil graphUtil;
	  
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
	
	public LogicalGraph getLogicalGraph(String gradoopGraphID) throws IOException {
		DataSource hbaseDataSource = new HBaseDataSource(HBaseEPGMStoreFactory.createOrOpenEPGMStore(hbase_cfg, gra_hbase_cfg), graflink_cfg);
		LogicalGraph graph = hbaseDataSource.getGraphCollection().getGraph(GradoopId.fromString(gradoopGraphID));
		return graph;
	}
	
	public List<DataStream<Tuple2<Boolean, Row>>> buildTopView (){
		DataStream<Tuple2<Boolean, Row>> dataStreamDegree = DegreeMatrixLoader.load(fsTableEnv, "degree_vertices_10_third", 50);
		List<DataStream<Tuple2<Boolean, Row>>> datastreams = new ArrayList<DataStream<Tuple2<Boolean, Row>>>();
		try {
			LogicalGraph graph = this.getLogicalGraph("5ebe6813a7986cc7bd77f9c2");	//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
			this.graphUtil = new GraphUtil(graph, fsEnv, fsTableEnv);
			datastreams.addAll(graphUtil.getMaxDegreeSubset(dataStreamDegree));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return datastreams;
		
	}
	
	public DataStream<Tuple5<String, String, String, String, String>> zoomIn (Integer top, Integer right, Integer bottom, Integer left) throws Exception{
		DataStream<Tuple5<String, String, String, String, String>> dstream_vertices_filtered = this.graphUtil.getVertexStream()
			.filter(new FilterFunction<Tuple5<String, String, String, String, String>>(){
			@Override
			public boolean filter(Tuple5<String, String, String, String, String> value) throws Exception {
				Integer x = Integer.parseInt(value.f3);
				Integer y = Integer.parseInt(value.f4);
				return (left < x) &&  (x < right) && (top < y) && (y < bottom);
			}
		});
		return dstream_vertices_filtered;
	}
}
