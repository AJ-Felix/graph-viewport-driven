package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator; 

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtil.AdjacencyMatrixGraphUtil;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtil.TableStreamGraphUtil;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtil.GradoopGraphUtil;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtil.GraphUtil;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtil.GraphUtilSet;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtil.GraphUtilStream;

public class FlinkCore {
	  private ExecutionEnvironment env;
	  private GradoopFlinkConfig gradoopFlinkConfig;
	  private EnvironmentSettings fsSettings;
	  private StreamExecutionEnvironment fsEnv;
	  private StreamTableEnvironment fsTableEnv;
	  private String flinkJobJarPath;
	  private int clusterEntryPointPort = 8081;
	  private String hdfsFullPath;
	  private String gradoopGraphID;
	  private Boolean stream = true;
	  private GraphUtilStream graphUtilStream;
	  private GraphUtilSet graphUtilSet;
	  private Float topNew;
	  private Float bottomNew;
	  private Float leftNew;
	  private Float rightNew;
	  private Float topOld;
	  private Float rightOld;
	  private Float bottomOld;
	  private Float leftOld;
	  private String vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree, vertexZoomLevel";
	  private String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
				+ "sourceVertexY, sourceVertexDegree, sourceZoomLevel, targetVertexIdGradoop, targetVertexIdNumeric, "
				+ "targetVertexLabel, targetVertexX, targetVertexY, targetVertexDegree, targetZoomLevel, "
				+ "edgeIdGradoop, edgeLabel";	  
	  
	public FlinkCore(String flinkJobJarFilePath, String clusterEntryPointAddress, String hdfsFullPath, String gradoopGraphId, int parallelism) {
		this.flinkJobJarPath = flinkJobJarFilePath;
		this.hdfsFullPath = hdfsFullPath;
		this.gradoopGraphID = gradoopGraphId;
		this.env = ExecutionEnvironment.createRemoteEnvironment(clusterEntryPointAddress, clusterEntryPointPort, 
				flinkJobJarPath);
		this.env.setParallelism(parallelism);
	    this.gradoopFlinkConfig = GradoopFlinkConfig.createConfig(env);
		this.fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		this.fsEnv = StreamExecutionEnvironment.createRemoteEnvironment(clusterEntryPointAddress, clusterEntryPointPort,
				flinkJobJarPath); 
		this.fsEnv.setParallelism(parallelism);	
		this.fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
		System.out.println("initiated Flink.");
	}
	
	public StreamExecutionEnvironment getFsEnv() {
		return this.fsEnv;
	}
	
	public ExecutionEnvironment getEnv() {
		return this.env;
	}
	
	public void setStreamBool(Boolean stream) {
		this.stream = stream;
	}
	
	public void setModelPositions(Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
		this.topNew = topModel;
		this.rightNew = rightModel;
		this.bottomNew = bottomModel;
		this.leftNew = leftModel;
	}
	
	public float[] getModelPositions() {
		return new float[]{topNew, rightNew, bottomNew, leftNew};
	}
	
	public void setModelPositionsOld() {
		this.topOld = this.topNew;
		this.rightOld = this.rightNew;
		this.bottomOld = this.bottomNew;
		this.leftOld = this.leftNew;
	}
	
	private LogicalGraph getLogicalGraph() throws IOException {
		LogicalGraph graph;
		DataSource source = new CSVDataSource(this.hdfsFullPath, this.gradoopFlinkConfig);
		GradoopId id = GradoopId.fromString(gradoopGraphID);
		graph = source.getGraphCollection().getGraph(id);
		return graph;
	}
	
	public GraphUtilSet initializeGradoopGraphUtil() {
		LogicalGraph graph;
		try {
			graph = this.getLogicalGraph();	//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
			this.graphUtilSet = new GradoopGraphUtil(graph);
		} catch (Exception e) {
			e.printStackTrace();
		}	
		return this.graphUtilSet;
	}
	
	public GraphUtilStream initializeTableStreamGraphUtil() {
		this.graphUtilStream = new TableStreamGraphUtil(this.fsEnv, this.fsTableEnv, this.hdfsFullPath, this.vertexFields, this.wrapperFields);
		return this.graphUtilStream;
	}
	
	public GraphUtilStream initializeAdjacencyGraphUtil() {
		this.graphUtilStream =  new AdjacencyMatrixGraphUtil(this.fsEnv, this.env, this.hdfsFullPath);
		return this.graphUtilStream;
	}
	
	public GraphUtil getGraphUtil() {
		if (stream)	return this.graphUtilStream;
		else return this.graphUtilSet;
	}
	
	public DataSet<WrapperVDrive> buildTopViewGradoop(int maxVertices){
		GradoopGraphUtil graphUtil = (GradoopGraphUtil) this.graphUtilSet;
		try {
			graphUtil.initializeDataSets();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return graphUtil.getMaxDegreeSubset(maxVertices);
	}
	
	public DataStream<Row> buildTopViewTableStream(int maxVertices){
		TableStreamGraphUtil graphUtil = ((TableStreamGraphUtil) this.graphUtilStream);
		graphUtil.initializeDataSets();
		return graphUtil.getMaxDegreeSubset(maxVertices);
	}
	
	public DataStream<Row> buildTopViewAdjacency(int maxVertices) {
		AdjacencyMatrixGraphUtil graphUtil = (AdjacencyMatrixGraphUtil) this.graphUtilStream;
		graphUtil.initializeDataSets();
		DataStream<Row> stream = null;
		try {
			stream = graphUtil.getMaxDegreeSubset(maxVertices);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stream;
	}
	
	public DataSet<WrapperVDrive> zoomSet(){
		return this.graphUtilSet.zoom(topNew, rightNew, bottomNew, leftNew);
	}
	
	public DataStream<Row> zoomStream(){
		DataStream<Row> stream = null;
		try {
			stream = this.graphUtilStream.zoom(topNew, rightNew, bottomNew, leftNew);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stream;
	}
	
	public DataSet<WrapperVDrive> zoomInLayoutStep1Set(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> innerVertices){
		return this.graphUtilSet.panZoomInLayoutStep1(layoutedVertices, innerVertices, topNew, rightNew, 
				bottomNew, leftNew);
	}
	
	public DataStream<Row> zoomInLayoutStep1Stream(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> innerVertices){
		return this.graphUtilStream.panZoomInLayoutStep1(layoutedVertices, innerVertices, topNew, rightNew, 
				bottomNew, leftNew);
	}
	
	public DataSet<WrapperVDrive> zoomInLayoutStep2Set(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> innerVertices,
			Map<String, VertexVDrive> newVertices){
		Map<String,VertexVDrive> unionMap = new HashMap<String,VertexVDrive>(innerVertices);
		unionMap.putAll(newVertices);
		return this.graphUtilSet.panZoomInLayoutStep2(layoutedVertices, unionMap);
	}
	
	public DataStream<Row> zoomInLayoutStep2Stream(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> innerVertices,
			Map<String, VertexVDrive> newVertices){
		Map<String,VertexVDrive> unionMap = new HashMap<String,VertexVDrive>(innerVertices);
		unionMap.putAll(newVertices);
		return this.graphUtilStream.panZoomInLayoutStep2(layoutedVertices, unionMap);
	}
	
	public DataSet<WrapperVDrive> zoomInLayoutStep3Set(Map<String, VertexVDrive> layoutedVertices){
		return this.graphUtilSet.panZoomInLayoutStep3(layoutedVertices);
	}
	
	public DataStream<Row> zoomInLayoutThirdStep(Map<String, VertexVDrive> layoutedVertices){
		return this.graphUtilStream.panZoomInLayoutStep3(layoutedVertices);
	}

	public DataSet<WrapperVDrive> zoomInLayoutStep4Set(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> innerVertices,
			Map<String, VertexVDrive> newVertices){
		return this.graphUtilSet.zoomInLayoutStep4(layoutedVertices, innerVertices, newVertices, 
				topNew, rightNew, bottomNew, leftNew);
	}
	
	public DataStream<Row> zoomInLayoutStep4Stream(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> innerVertices,
			Map<String, VertexVDrive> newVertices){
		return this.graphUtilStream.zoomInLayoutStep4(layoutedVertices, innerVertices, newVertices, 
				topNew, rightNew, bottomNew, leftNew);
	}
	
	public DataSet<WrapperVDrive> zoomOutLayoutStep1Set(Map<String, VertexVDrive> layoutedVertices){
		return this.graphUtilSet.zoomOutLayoutStep1(layoutedVertices, topNew, rightNew, 
				bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld);
	}
	
	public DataStream<Row> zoomOutLayoutStep1Stream(Map<String, VertexVDrive> layoutedVertices){
		return this.graphUtilStream.zoomOutLayoutStep1(layoutedVertices, topNew, rightNew, 
				bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld);
	}
	
	public DataSet<WrapperVDrive> zoomOutLayoutStep2Set(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices){
		return this.graphUtilSet.zoomOutLayoutStep2(layoutedVertices, newVertices, topNew, 
				rightNew, bottomNew, leftNew);
	}
	
	public DataStream<Row> zoomOutLayoutStep2Stream(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices){
		return this.graphUtilStream.zoomOutLayoutStep2(layoutedVertices, newVertices, topNew, 
				rightNew, bottomNew, leftNew);
	}
	
	public DataSet<WrapperVDrive> panLayoutStep1Set(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices){
		return this.graphUtilSet.panZoomInLayoutStep1(layoutedVertices, newVertices, topNew, rightNew, bottomNew, 
				leftNew);
	}
	
	public DataStream<Row> panLayoutStep1Stream(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices){
		return this.graphUtilStream.panZoomInLayoutStep1(layoutedVertices, newVertices, topNew, rightNew, bottomNew, 
				leftNew);
	}
	
	public DataSet<WrapperVDrive> panLayoutStep2Set(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices){
		return this.graphUtilSet.panZoomInLayoutStep2(layoutedVertices, newVertices);
	}
	
	public DataStream<Row> panLayoutStep2Stream(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices){
		return this.graphUtilStream.panZoomInLayoutStep2(layoutedVertices, newVertices);
	}
	
	public DataSet<WrapperVDrive> panLayoutStep3Set(Map<String, VertexVDrive> layoutedVertices){
		return this.graphUtilSet.panZoomInLayoutStep3(layoutedVertices);
	}
	
	public DataStream<Row> panLayoutStep3Stream(Map<String, VertexVDrive> layoutedVertices){
		return this.graphUtilStream.panZoomInLayoutStep3(layoutedVertices);
	}
	
	public DataSet<WrapperVDrive> panLayoutStep4Set(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices){
		return this.graphUtilSet.panLayoutStep4(layoutedVertices, newVertices, topNew, rightNew, bottomNew, 
				leftNew, topOld, rightOld, bottomOld, leftOld);
	}
	
	public DataStream<Row> panLayoutStep4Stream(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices){
		return this.graphUtilStream.panLayoutStep4(layoutedVertices, newVertices, topNew, rightNew, bottomNew, 
				leftNew, topOld, rightOld, bottomOld, leftOld);
	}
	
	public DataStream<Row> pan(){
		DataStream<Row> stream = this.graphUtilStream.pan(topNew, rightNew, bottomNew, leftNew, 
				topOld, rightOld, bottomOld, leftOld);
		return stream;
	}
	
	public DataSet<WrapperVDrive> panSet(){
		DataSet<WrapperVDrive> set = this.graphUtilSet.pan(topNew, rightNew, bottomNew, leftNew, 
				topOld, rightOld, bottomOld, leftOld);
		return set;
	}
	
	public void setVertexZoomLevel(int zoomLevel) {
		this.getGraphUtil().setVertexZoomLevel(zoomLevel);
	}
}
