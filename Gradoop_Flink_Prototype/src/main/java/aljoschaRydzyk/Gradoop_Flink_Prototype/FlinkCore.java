package aljoschaRydzyk.Gradoop_Flink_Prototype; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.hbase.impl.io.HBaseDataSource;

import Temporary.CSVGraphUtilMap;

public class FlinkCore {
	  private ExecutionEnvironment env;
	  private GradoopFlinkConfig graflink_cfg;
	  private GradoopHBaseConfig gra_hbase_cfg;
	  private org.apache.hadoop.conf.Configuration hbase_cfg;
	  private EnvironmentSettings fsSettings;
	  private StreamExecutionEnvironment fsEnv;
	  private StreamTableEnvironment fsTableEnv;
	  
	  private GraphUtil graphUtil;
	  private Float topModelPos;
	  private Float bottomModelPos;
	  private Float leftModelPos;
	  private Float rightModelPos;
	  
	public  FlinkCore () {
		this.env = ExecutionEnvironment.getExecutionEnvironment();
	    this.graflink_cfg = GradoopFlinkConfig.createConfig(env);
		this.gra_hbase_cfg = GradoopHBaseConfig.getDefaultConfig();
		this.hbase_cfg = HBaseConfiguration.create();
		this.fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		this.fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		org.apache.flink.configuration.Configuration conf = new Configuration();
		this.fsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		this.fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
//		TestThread thread = new TestThread("prototype", fsEnv, this);
//		thread.start();
		System.out.println("initiated Flink.");

	}
	
	public void setTopModelPos(Float topModelPos2) {
		this.topModelPos = topModelPos2;
	}
	
	public Float gettopModelPos() {
		return this.topModelPos;
	}
	
	public void setBottomModelPos(Float bottomModelPos) {
		this.bottomModelPos = bottomModelPos;
	}
	
	public Float getBottomModelPos() {
		return this.bottomModelPos;
	}
	
	public void setRightModelPos(Float rightModelPos) {
		this.rightModelPos = rightModelPos;
	}
	
	public Float getRightModelPos() {
		return this.rightModelPos;
	}
	
	public void setLeftModelPos(Float leftModelPos) {
		this.leftModelPos = leftModelPos;
	}
	
	public Float getLeftModelPos() {
		return this.leftModelPos;
	}
	
	public StreamExecutionEnvironment getFsEnv() {
		return this.fsEnv;
	}
	
	public LogicalGraph getLogicalGraph(String gradoopGraphID) throws IOException {
		DataSource hbaseDataSource = new HBaseDataSource(HBaseEPGMStoreFactory.createOrOpenEPGMStore(hbase_cfg, gra_hbase_cfg), graflink_cfg);
		LogicalGraph graph = hbaseDataSource.getGraphCollection().getGraph(GradoopId.fromString(gradoopGraphID));
		return graph;
	}
	
	public GraphUtil initializeGradoopGraphUtil() {
		LogicalGraph graph;
		try {
			graph = this.getLogicalGraph("5ebe6813a7986cc7bd77f9c2");	//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
			this.graphUtil = new GradoopGraphUtil(graph, this.fsEnv, this.fsTableEnv);
		} catch (IOException e) {
			e.printStackTrace();
		}	
		return this.graphUtil;
	}
	
	public GraphUtil initializeCSVGraphUtilJoin() {
		this.graphUtil = new CSVGraphUtilJoin(this.fsEnv, this.fsTableEnv, 
				"/home/aljoscha/graph-viewport-driven/csvGraphs/one10thousand_sample_2_third_degrees_layout");
		return this.graphUtil;
	}
	
	public GraphUtil initializeCSVGraphUtilMap() {
		this.graphUtil = new CSVGraphUtilMap(this.fsEnv, "/home/aljoscha/graph-viewport-driven/csvGraphs/one10thousand_sample_2_third_degrees_layout");
		return this.graphUtil;
	}
	
	public GraphUtil initializeAdjacencyGraphUtil() {
		this.graphUtil = new AdjacencyGraphUtil(this.fsEnv, this.fsTableEnv, 
				"/home/aljoscha/graph-viewport-driven/csvGraphs/adjacency/one10thousand_sample_2_third_degrees_layout");
		return this.graphUtil;
	}
	
	public GraphUtil getGraphUtil() {
		return this.graphUtil;
	}
	
	public DataStream<Tuple2<Boolean, Row>> buildTopViewRetract(){
		DataStream<Row> dataStreamDegree = FlinkGradoopVerticesLoader.load(fsTableEnv, 10);
		DataStream<Tuple2<Boolean, Row>> wrapperStream = null;
		try {
			GradoopGraphUtil graphUtil = ((GradoopGraphUtil) this.graphUtil);
			graphUtil.initializeStreams();
			wrapperStream = graphUtil.getMaxDegreeSubset(dataStreamDegree);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return wrapperStream;
	}
	
	public DataStream<Row> buildTopViewAppendJoin(Integer maxVertices){
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
		graphUtil.initializeStreams();
		return graphUtil.getMaxDegreeSubset(maxVertices);
	}
	
	public DataStream<Row> buildTopViewAppendMap(){
		CSVGraphUtilMap graphUtil = ((CSVGraphUtilMap) this.graphUtil);
		return graphUtil.initializeStreams();	
	}
	
	public DataStream<Row> buildTopViewAdjacency(Integer maxVertices) {
		AdjacencyGraphUtil graphUtil = (AdjacencyGraphUtil) this.graphUtil;
		graphUtil.initializeStreams();
		DataStream<Row> stream = null;
		try {
			stream = graphUtil.getMaxDegreeSubset(maxVertices);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stream;
	}
	
	public DataStream<Row> zoom(Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		DataStream<Row> stream = null;
		try {
			stream = this.graphUtil.zoom(topModel, rightModel, bottomModel, leftModel);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stream;
	}
	
	public DataStream<Row> pan(Float topOld, Float rightOld, Float bottomOld, Float leftOld, Float xModelDiff, Float yModelDiff){
		DataStream<Row> stream = null;
		try {
			stream =  this.graphUtil.pan(topOld, rightOld, bottomOld, leftOld, xModelDiff, yModelDiff);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stream;
	}
		
	public DataStream<Row> displayAll() {
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
		return graphUtil.initializeStreams();
	}
}
