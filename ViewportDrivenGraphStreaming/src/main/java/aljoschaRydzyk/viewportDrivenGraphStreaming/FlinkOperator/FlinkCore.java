package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator; 

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.hbase.impl.io.HBaseDataSource;

import aljoschaRydzyk.viewportDrivenGraphStreaming.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.AdjacencyGraphUtil;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.CSVGraphUtilJoin;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.GradoopGraphUtil;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.GraphUtil;

public class FlinkCore {
	  private ExecutionEnvironment env;
	  private GradoopFlinkConfig graflink_cfg;
	  private GradoopHBaseConfig gra_hbase_cfg;
	  private org.apache.hadoop.conf.Configuration hbase_cfg;
	  private EnvironmentSettings fsSettings;
	  private StreamExecutionEnvironment fsEnv;
	  private StreamTableEnvironment fsTableEnv;
	  
	  private String pathToGradoopCSV = "/home/aljoscha/graph-samples/one10thousand_sample_2_third_degrees_layout";
	  private Boolean gradoopWithHBase = false;
	  private String gradoopGraphID = "5ebe6813a7986cc7bd77f9c2";
	  
	  private GraphUtil graphUtil;
	  private Float topNew;
	  private Float bottomNew;
	  private Float leftNew;
	  private Float rightNew;
	  private Float topOld;
	  private Float rightOld;
	  private Float bottomOld;
	  private Float leftOld;
	  private String vertexFields;
	  private String wrapperFields;
	  private String filePath;
	  
	  
	public FlinkCore (String clusterEntryPointIp4, int clusterEntryPointPort) {
		this.env = ExecutionEnvironment.getExecutionEnvironment();
	    this.graflink_cfg = GradoopFlinkConfig.createConfig(env);
		this.gra_hbase_cfg = GradoopHBaseConfig.getDefaultConfig();
		this.hbase_cfg = HBaseConfiguration.create();
		this.fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		this.fsEnv = StreamExecutionEnvironment.createRemoteEnvironment(clusterEntryPointIp4, clusterEntryPointPort , "/home/aljoscha/remoteEnvJars/combined.jar"); 
//		this.fsEnv.setParallelism(4);
		this.fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
		this.vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree";
		this.wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
				+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
				+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
		this.filePath = "/home/aljoscha/graph-viewport-driven/csvGraphs/adjacency/one10thousand_sample_2_third_degrees_layout";
		System.out.println("initiated Flink.");
	}
	
	public void setTopModelPos(Float topModelPos) {
		this.topNew = topModelPos;
	}
	
	public Float getTopModel() {
		return this.topNew;
	}
	
	public void setBottomModelPos(Float bottomModelPos) {
		this.bottomNew = bottomModelPos;
	}
	
	public Float getBottomModel() {
		return this.bottomNew;
	}
	
	public void setRightModelPos(Float rightModelPos) {
		this.rightNew = rightModelPos;
	}
	
	public Float getRightModel() {
		return this.rightNew;
	}
	
	public void setLeftModelPos(Float leftModelPos) {
		this.leftNew = leftModelPos;
	}
	
	public Float getLeftModel() {
		return this.leftNew;
	}
	
	public StreamExecutionEnvironment getFsEnv() {
		return this.fsEnv;
	}
	
	public void setModelPositions(Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
		this.topNew = topModel;
		this.rightNew = rightModel;
		this.bottomNew = bottomModel;
		this.leftNew = leftModel;
	}
	
	public void setModelPositionsOld(Float topModelOld, Float rightModelOld, Float bottomModelOld, Float leftModelOld) {
		this.topOld = topModelOld;
		this.rightOld = rightModelOld;
		this.bottomOld = bottomModelOld;
		this.leftOld = leftModelOld;
	}
	
	public void setGradoopWithHBase(Boolean is) {
		this.gradoopWithHBase = is;
	}
	
	private LogicalGraph getLogicalGraph() throws IOException {
		LogicalGraph graph;
		if (gradoopWithHBase == false) {
			DataSource source = new CSVDataSource(pathToGradoopCSV, this.graflink_cfg);
			GradoopId id = GradoopId.fromString(gradoopGraphID);
			graph = source.getGraphCollection().getGraph(id);
		} else {
			DataSource hbaseDataSource = new HBaseDataSource(HBaseEPGMStoreFactory.createOrOpenEPGMStore(hbase_cfg, gra_hbase_cfg), graflink_cfg);
			graph = hbaseDataSource.getGraphCollection().getGraph(GradoopId.fromString(gradoopGraphID));
		}
		return graph;
	}
	
	public GraphUtil initializeGradoopGraphUtil() {
		LogicalGraph graph;
		try {
			graph = this.getLogicalGraph();	//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
			this.graphUtil = new GradoopGraphUtil(graph, this.fsEnv, this.fsTableEnv, this.vertexFields, this.wrapperFields);
			this.graphUtil.buildAdjacencyMatrix();
		} catch (Exception e) {
			e.printStackTrace();
		}	
		return this.graphUtil;
	}
	
	public GraphUtil initializeCSVGraphUtilJoin() {
		this.graphUtil = new CSVGraphUtilJoin(this.fsEnv, this.fsTableEnv, this.filePath, this.vertexFields, this.wrapperFields);
			try {
				this.graphUtil.buildAdjacencyMatrix();
			} catch (Exception e) {
				e.printStackTrace();
			}
		return this.graphUtil;
	}
	
	public GraphUtil initializeAdjacencyGraphUtil() {
		this.graphUtil =  new AdjacencyGraphUtil(this.fsEnv, this.filePath);
		return this.graphUtil;
	}
	
	public GraphUtil getGraphUtil() {
		return this.graphUtil;
	}
	
	public DataStream<Row> buildTopViewRetractCSV(Integer maxVertices){
		GradoopGraphUtil graphUtil = ((GradoopGraphUtil) this.graphUtil);
		try {
			graphUtil.initializeStreams();
		} catch (Exception e) {
			e.printStackTrace();
		}
		DataStream<Row> wrapperStream = graphUtil.getMaxDegreeSubsetCSV(maxVertices);
		return wrapperStream;
	}
	
	public DataStream<Tuple2<Boolean, Row>> buildTopViewRetractHBase(Integer maxVertices){
		DataStream<Row> dataStreamDegree = FlinkHBaseVerticesLoader.load(fsTableEnv, maxVertices);
		DataStream<Tuple2<Boolean, Row>> wrapperStream = null;
		try {
			GradoopGraphUtil graphUtil = ((GradoopGraphUtil) this.graphUtil);
			graphUtil.initializeStreams();
			wrapperStream = graphUtil.getMaxDegreeSubsetHBase(dataStreamDegree);
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
	
	public DataStream<Row> zoom(){
		DataStream<Row> stream = null;
		try {
			stream = this.graphUtil.zoom(topNew, rightNew, bottomNew, leftNew);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stream;
	}
	
	public DataStream<Row> zoomInLayoutFirstStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices){
		return this.graphUtil.panZoomInLayoutFirstStep(layoutedVertices, innerVertices, topNew, rightNew, 
				bottomNew, leftNew);
	}
	
	public DataStream<Row> zoomInLayoutSecondStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices,
			Map<String, VertexGVD> newVertices){
		Map<String,VertexGVD> unionMap = new HashMap<String,VertexGVD>(innerVertices);
		unionMap.putAll(newVertices);
		return this.graphUtil.panZoomInLayoutSecondStep(layoutedVertices, unionMap);
	}
	
	public DataStream<Row> zoomInLayoutThirdStep(Map<String, VertexGVD> layoutedVertices){
		return this.graphUtil.panZoomInLayoutThirdStep(layoutedVertices);
	}
	
	public DataStream<Row> zoomInLayoutFourthStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices,
			Map<String, VertexGVD> newVertices){
		return this.graphUtil.zoomInLayoutFourthStep(layoutedVertices, innerVertices, newVertices, 
				topNew, rightNew, bottomNew, leftNew);
	}
	
	public DataStream<Row> zoomOutLayoutFirstStep(Map<String, VertexGVD> layoutedVertices){
		return this.graphUtil.zoomOutLayoutFirstStep(layoutedVertices, topNew, rightNew, 
				bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld);
	}
	
	public DataStream<Row> zoomOutLayoutSecondStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices){
		return this.graphUtil.zoomOutLayoutSecondStep(layoutedVertices, newVertices, topNew, 
				rightNew, bottomNew, leftNew);
	}
	
	public DataStream<Row> panLayoutFirstStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices){
		return this.graphUtil.panZoomInLayoutFirstStep(layoutedVertices, newVertices, topNew, rightNew, bottomNew, 
				leftNew);
	}
	
	public DataStream<Row> panLayoutSecondStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices){
		return this.graphUtil.panZoomInLayoutSecondStep(layoutedVertices, newVertices);
	}
	
	public DataStream<Row> panLayoutThirdStep(Map<String, VertexGVD> layoutedVertices){
		return this.graphUtil.panZoomInLayoutThirdStep(layoutedVertices);
	}
	
	public DataStream<Row> panLayoutFourthStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices){
		return this.graphUtil.panLayoutFourthStep(layoutedVertices, newVertices, topNew, rightNew, bottomNew, 
				leftNew, topOld, rightOld, bottomOld, leftOld);
	}
	
	public DataStream<Row> pan(){
		DataStream<Row> stream = this.graphUtil.pan(topNew, rightNew, bottomNew, leftNew, 
				topOld, rightOld, bottomOld, leftOld);
		return stream;
	}
		
	public DataStream<Row> displayAll() {
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
		graphUtil.initializeStreams();
		return graphUtil.getWrapperStream();
	}
}
