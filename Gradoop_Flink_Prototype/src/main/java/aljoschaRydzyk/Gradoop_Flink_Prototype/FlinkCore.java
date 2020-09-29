package aljoschaRydzyk.Gradoop_Flink_Prototype; 

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
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
	  private org.apache.hadoop.conf.Configuration hbase_cfg;
	  private EnvironmentSettings fsSettings;
	  private StreamExecutionEnvironment fsEnv;
	  private StreamTableEnvironment fsTableEnv;
	  
	  private GraphUtil graphUtil;
	  private String graphOperationLogic;
	  private Float topModelPos;
	  private Float bottomModelPos;
	  private Float leftModelPos;
	  private Float rightModelPos;
	  private String vertexFields;
	  private String wrapperFields;
	  private String filePath;
	  
	  
	public  FlinkCore (String graphOperationLogic) {
		this.env = ExecutionEnvironment.getExecutionEnvironment();
	    this.graflink_cfg = GradoopFlinkConfig.createConfig(env);
		this.gra_hbase_cfg = GradoopHBaseConfig.getDefaultConfig();
		this.hbase_cfg = HBaseConfiguration.create();
		this.fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		this.fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		org.apache.flink.configuration.Configuration conf = new Configuration();
		this.fsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		this.fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
		this.vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree";
		this.wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
				+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
				+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
		this.filePath = "/home/aljoscha/graph-viewport-driven/csvGraphs/adjacency/one10thousand_sample_2_third_degrees_layout";
//		TestThread thread = new TestThread("prototype", fsEnv, this);
//		thread.start();
		this.graphOperationLogic = graphOperationLogic;
		System.out.println("initiated Flink.");

	}
	
	public void setTopModelPos(Float topModelPos) {
		this.topModelPos = topModelPos;
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
			this.graphUtil = new GradoopGraphUtil(graph, this.fsEnv, this.fsTableEnv, this.vertexFields, this.wrapperFields);
			if (graphOperationLogic.equals("serverSide")) {
				this.graphUtil.buildAdjacencyMatrix();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}	
		return this.graphUtil;
	}
	
	public GraphUtil initializeCSVGraphUtilJoin() {
		this.graphUtil = new CSVGraphUtilJoin(this.fsEnv, this.fsTableEnv, this.filePath, this.vertexFields, this.wrapperFields);
		if (graphOperationLogic.equals("serverSide")) {
			try {
				this.graphUtil.buildAdjacencyMatrix();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return this.graphUtil;
	}
	
	public GraphUtil initializeAdjacencyGraphUtil() {
		this.graphUtil =  new AdjacencyGraphUtil(this.fsEnv, this.filePath);
//		graphVis = new GraphVis();
//		GraphVis.setGraphVis(((AdjacencyGraphUtil) this.graphUtil).getAdjMatrix());
		return this.graphUtil;
	}
	
	public GraphUtil getGraphUtil() {
		return this.graphUtil;
	}
	
//	public GraphVis getGraphVis() {
//		return this.graphVis;
//	}
	
	public DataStream<Tuple2<Boolean, Row>> buildTopViewRetract(Integer maxVertices){
		DataStream<Row> dataStreamDegree = FlinkGradoopVerticesLoader.load(fsTableEnv, maxVertices);
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
	
	public DataStream<Row> zoomInLayout(Map<String, VertexCustom> innerVertices, Set<String> layoutedVertices){
		return ((CSVGraphUtilJoin) this.graphUtil).zoomInLayout(innerVertices, layoutedVertices);
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
		graphUtil.initializeStreams();
		return graphUtil.getWrapperStream();
	}
}
