package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator; 

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.hbase.impl.io.HBaseDataSource;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.AdjacencyGraphUtil;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.CSVGraphUtilJoin;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.GradoopGraphUtil;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.GraphUtil;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.GraphUtilSet;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils.GraphUtilStream;

public class FlinkCore {
	  private ExecutionEnvironment env;
	  private GradoopFlinkConfig graflink_cfg;
	  private GradoopHBaseConfig gra_hbase_cfg;
	  private org.apache.hadoop.conf.Configuration hbase_cfg;
	  private EnvironmentSettings fsSettings;
	  private StreamExecutionEnvironment fsEnv;
	  private StreamTableEnvironment fsTableEnv;
	  private String flinkJobJarPath = "/home/aljoscha/remoteEnvJars/combined.jar";
	  
//	  private String clusterEntryPointAddress = "localhost";
	  private int clusterEntryPointPort = 8081;
//	  private String hdfsEntryPointAddress = "localhost";
//	  private int hdfsEntryPointPort = 9000;
//	  private String hdfsGraphFilesDirectory;
	  private String hdfsFullPath;
	  private Boolean degreesCalculated = false;
	  
	  
	  private Boolean gradoopWithHBase;
	  private String gradoopGraphID = "5ebe6813a7986cc7bd77f9c2";
	  
	  private Boolean stream = true;;
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
	  private String vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree";
	  private String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
				+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
				+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
	  
	  
	public FlinkCore(String clusterEntryPointAddress, String hdfsFullPath, String gradoopGraphId,
			Boolean degreesCalculated) {
//		String clusterEntryPointIp4 = flinkCoreParameters.get(0);
//		this.gradoopCSVPath = flinkCoreParameters.get(1);
//		this.gradoopGraphID = flinkCoreParameters.get(2);
//		this.viDGraSCSVPath = flinkCoreParameters.get(
//		this.env = ExecutionEnvironment.getExecutionEnvironment();
		this.degreesCalculated = degreesCalculated;
		this.hdfsFullPath = hdfsFullPath;
		this.env = ExecutionEnvironment.createRemoteEnvironment(clusterEntryPointAddress, clusterEntryPointPort, 
				flinkJobJarPath);
		
		this.env.setParallelism(1);
		
	    this.graflink_cfg = GradoopFlinkConfig.createConfig(env);
		this.gra_hbase_cfg = GradoopHBaseConfig.getDefaultConfig();
		this.hbase_cfg = HBaseConfiguration.create();
		this.fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		this.fsEnv = StreamExecutionEnvironment.createRemoteEnvironment(clusterEntryPointAddress, clusterEntryPointPort,
				flinkJobJarPath); 
		
		this.fsEnv.setParallelism(1);
		
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
	
//	public void setClusterEntryPointAdress(String address) {
//		this.clusterEntryPointAddress = address;
//	}
//	
//	public void setHDFSEntryPointAdress(String address) {
//		this.hdfsEntryPointAddress = address;
//		this.setHDFSFullPath();
//	}
//	
//	public void setHDFSEntryPointPort(int port) {
//		this.hdfsEntryPointPort = port;
//		this.setHDFSFullPath();
//	}
	
//	public void setGraphId(String Id) {
//		this.gradoopGraphID = Id;
//	}
	
//	public void setHDFSGraphFilesDirectory(String directory) {
//		this.hdfsGraphFilesDirectory = directory;
//		this.setHDFSFullPath();
//	}
	
//	public void setDegreesCalculated(Boolean calculated) {
//		this.degreesCalculated = calculated;
//	}
	
//	private void setHDFSFullPath() {
//		this.hdfsFullPath = "hdfs://" + this.hdfsEntryPointAddress + ":" + String.valueOf(this.hdfsEntryPointPort)
//				+ this.hdfsGraphFilesDirectory;
//		System.out.println(this.hdfsFullPath);
//	}
	
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
		this.rightOld = this.topNew;
		this.bottomOld = this.bottomNew;
		this.leftOld = this.leftNew;
	}
	
	public void setGradoopWithHBase(Boolean is) {
		this.gradoopWithHBase = is;
	}
	
	private LogicalGraph getLogicalGraph() throws IOException {
		LogicalGraph graph;
		if (gradoopWithHBase == false) {
			DataSource source = new CSVDataSource(this.hdfsFullPath, this.graflink_cfg);
			GradoopId id = GradoopId.fromString(gradoopGraphID);
			graph = source.getGraphCollection().getGraph(id);
		} else {
			DataSource hbaseDataSource = new HBaseDataSource(HBaseEPGMStoreFactory.createOrOpenEPGMStore(hbase_cfg, gra_hbase_cfg), graflink_cfg);
			graph = hbaseDataSource.getGraphCollection().getGraph(GradoopId.fromString(gradoopGraphID));
		}
		if (!degreesCalculated) {
			graph = graph.callForGraph(new DistinctVertexDegrees("degree", "inDegree", "outDegree", true));
		}
		return graph;
	}
	
	public GraphUtilSet initializeGradoopGraphUtil() {
		LogicalGraph graph;
		try {
			graph = this.getLogicalGraph();	//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
			this.graphUtilSet = new GradoopGraphUtil(graph, this.fsEnv, this.fsTableEnv, this.vertexFields, this.wrapperFields);
//			this.graphUtilSet.buildAdjacencyMatrix();
		} catch (Exception e) {
			e.printStackTrace();
		}	
		return this.graphUtilSet;
	}
	
	public GraphUtilStream initializeCSVGraphUtilJoin() {
		this.graphUtilStream = new CSVGraphUtilJoin(this.fsEnv, this.fsTableEnv, this.hdfsFullPath, this.vertexFields, this.wrapperFields);
			try {
				this.graphUtilStream.buildAdjacencyMatrix();
			} catch (Exception e) {
				e.printStackTrace();
			}
		return this.graphUtilStream;
	}
	
	public GraphUtilStream initializeAdjacencyGraphUtil() {
		this.graphUtilStream =  new AdjacencyGraphUtil(this.fsEnv, this.hdfsFullPath);
		return this.graphUtilStream;
	}
	
	public GraphUtil getGraphUtil() {
		if (stream)	return this.graphUtilStream;
		else return this.graphUtilSet;
	}
	
	public DataSet<WrapperGVD> buildTopViewGradoop(Integer maxVertices){
		GradoopGraphUtil graphUtil = (GradoopGraphUtil) this.graphUtilSet;
		try {
			graphUtil.initializeDataSets();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return graphUtil.getMaxDegreeSubsetGradoop(maxVertices);
	}
	
	public DataStream<Tuple2<Boolean, Row>> buildTopViewHBase(Integer maxVertices){
		DataStream<Row> dataStreamDegree = FlinkHBaseVerticesLoader.load(fsTableEnv, maxVertices);
		DataStream<Tuple2<Boolean, Row>> wrapperStream = null;
		try {
			GradoopGraphUtil graphUtil = ((GradoopGraphUtil) this.graphUtilStream);
			graphUtil.initializeDataSets();
			wrapperStream = graphUtil.getMaxDegreeSubsetHBase(dataStreamDegree);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return wrapperStream;
	}
	
	public DataStream<Row> buildTopViewCSV(Integer maxVertices){
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtilStream);
		graphUtil.initializeDataSets();
		return graphUtil.getMaxDegreeSubset(maxVertices);
	}
	
	public DataStream<Row> buildTopViewAdjacency(Integer maxVertices) {
		AdjacencyGraphUtil graphUtil = (AdjacencyGraphUtil) this.graphUtilStream;
		graphUtil.initializeDataSets();
		DataStream<Row> stream = null;
		try {
			stream = graphUtil.getMaxDegreeSubset(maxVertices);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return stream;
	}
	
	public DataSet<WrapperGVD> zoomSet(){
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
	
	public DataStream<Row> zoomInLayoutFirstStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices){
		return this.graphUtilStream.panZoomInLayoutFirstStep(layoutedVertices, innerVertices, topNew, rightNew, 
				bottomNew, leftNew);
	}
	
	public DataStream<Row> zoomInLayoutSecondStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices,
			Map<String, VertexGVD> newVertices){
		Map<String,VertexGVD> unionMap = new HashMap<String,VertexGVD>(innerVertices);
		unionMap.putAll(newVertices);
		return this.graphUtilStream.panZoomInLayoutSecondStep(layoutedVertices, unionMap);
	}
	
	public DataStream<Row> zoomInLayoutThirdStep(Map<String, VertexGVD> layoutedVertices){
		return this.graphUtilStream.panZoomInLayoutThirdStep(layoutedVertices);
	}
	
	public DataStream<Row> zoomInLayoutFourthStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices,
			Map<String, VertexGVD> newVertices){
		return this.graphUtilStream.zoomInLayoutFourthStep(layoutedVertices, innerVertices, newVertices, 
				topNew, rightNew, bottomNew, leftNew);
	}
	
	public DataStream<Row> zoomOutLayoutFirstStep(Map<String, VertexGVD> layoutedVertices){
		return this.graphUtilStream.zoomOutLayoutFirstStep(layoutedVertices, topNew, rightNew, 
				bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld);
	}
	
	public DataStream<Row> zoomOutLayoutSecondStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices){
		return this.graphUtilStream.zoomOutLayoutSecondStep(layoutedVertices, newVertices, topNew, 
				rightNew, bottomNew, leftNew);
	}
	
	public DataStream<Row> panLayoutFirstStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices){
		return this.graphUtilStream.panZoomInLayoutFirstStep(layoutedVertices, newVertices, topNew, rightNew, bottomNew, 
				leftNew);
	}
	
	public DataStream<Row> panLayoutSecondStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices){
		return this.graphUtilStream.panZoomInLayoutSecondStep(layoutedVertices, newVertices);
	}
	
	public DataStream<Row> panLayoutThirdStep(Map<String, VertexGVD> layoutedVertices){
		return this.graphUtilStream.panZoomInLayoutThirdStep(layoutedVertices);
	}
	
	public DataStream<Row> panLayoutFourthStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices){
		return this.graphUtilStream.panLayoutFourthStep(layoutedVertices, newVertices, topNew, rightNew, bottomNew, 
				leftNew, topOld, rightOld, bottomOld, leftOld);
	}
	
	public DataStream<Row> pan(){
		DataStream<Row> stream = this.graphUtilStream.pan(topNew, rightNew, bottomNew, leftNew, 
				topOld, rightOld, bottomOld, leftOld);
		return stream;
	}
	
	public DataSet<WrapperGVD> panSet(){
		DataSet<WrapperGVD> set = this.graphUtilSet.pan(topNew, rightNew, bottomNew, leftNew, 
				topOld, rightOld, bottomOld, leftOld);
		return set;
	}
		
	public DataStream<Row> displayAll() {
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtilStream);
		graphUtil.initializeDataSets();
		return graphUtil.getWrapperStream();
	}
}
