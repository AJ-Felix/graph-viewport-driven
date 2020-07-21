package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

public class GradoopGraphUtil implements GraphUtil{
	private DataStreamSource<VertexCustom> vertexStream;
	private DataStreamSource<EdgeCustom> edgeStream;
	private DataStreamSource<VVEdgeWrapper> wrapperStream = null;
	private Map<String, Integer> vertexIdMap = null;
	private LogicalGraph graph;
	private StreamExecutionEnvironment fsEnv;
	private StreamTableEnvironment fsTableEnv;
	
	public GradoopGraphUtil (LogicalGraph graph, StreamExecutionEnvironment fsEnv, StreamTableEnvironment fsTableEnv) {
		this.graph = graph;
		this.fsEnv = fsEnv;
		this.fsTableEnv = fsTableEnv;
	}
	
	@Override
	public DataStreamSource<VVEdgeWrapper> produceWrapperStream() throws Exception{
		List<EPGMVertex> vertices = this.graph.getVertices().collect();
		this.vertexIdMap = new HashMap<String, Integer>();
		List<VertexCustom> customVertices = new ArrayList<VertexCustom>();
		for (int i = 0; i < vertices.size(); i++) {
			String vertexIdGradoop = vertices.get(i).getId().toString();
			Integer vertexIdNumeric = (Integer) i;
			this.vertexIdMap.put(vertexIdGradoop, vertexIdNumeric);
			Integer x = ((Integer) vertices.get(i).getPropertyValue("X").getInt());
			Integer y = ((Integer) vertices.get(i).getPropertyValue("Y").getInt());
			Long degree = ((Long) vertices.get(i).getPropertyValue("degree").getLong());
			String vertexLabel = vertices.get(i).getLabel();
			customVertices.add(new VertexCustom(vertexIdGradoop, vertexLabel, vertexIdNumeric, x, y, degree));
		}	
		List<EPGMEdge> edges = this.graph.getEdges().collect();
		List<EdgeCustom> customEdges = new ArrayList<EdgeCustom>();
		List<VVEdgeWrapper> wrappers = new ArrayList<VVEdgeWrapper>();
		for (int i = 0; i < edges.size(); i++) {
			String edgeIdGradoop = edges.get(i).getId().toString();
			String edgeLabel = edges.get(i).getLabel();
			String sourceVertexIdGradoop = edges.get(i).getSourceId().toString();
			String targetVertexIdGradoop = edges.get(i).getTargetId().toString();		
			EdgeCustom edgeCustom = new EdgeCustom(edgeIdGradoop, edgeLabel, sourceVertexIdGradoop, targetVertexIdGradoop);
			customEdges.add(edgeCustom);
			for (VertexCustom sourceVertex: customVertices) {
				wrappers.add(new VVEdgeWrapper(sourceVertex, sourceVertex, new EdgeCustom("identityEdge", "identityEdge", sourceVertex.getIdGradoop(),
						sourceVertex.getIdGradoop())));
				if (sourceVertex.getIdGradoop().equals(sourceVertexIdGradoop)) {
					for (VertexCustom targetVertex: customVertices) {
						if (targetVertex.getIdGradoop().equals(targetVertexIdGradoop)) wrappers.add(new VVEdgeWrapper(sourceVertex, targetVertex, edgeCustom));
					}
				}
			}
		}
		this.vertexStream = fsEnv.fromCollection(customVertices);
		this.edgeStream = fsEnv.fromCollection(customEdges);
		this.wrapperStream = fsEnv.fromCollection(wrappers);
		return this.wrapperStream;
	}
	
	@Override
	public DataStream<VVEdgeWrapper> getWrapperStream() throws Exception {
		if (this.wrapperStream == null) throw new Exception("This function can only be used posterior to 'produceWrapperStream' invocation!");
		return this.wrapperStream;
	}
	
	public DataStream<Tuple2<Boolean, Row>> getMaxDegreeSubset(DataStream<Row> dataStreamDegree) throws Exception {
		if (this.wrapperStream == null) throw new Exception("This function can only be used posterior to 'produceWrapperStream' invocation!");
		DataStream<Tuple2<Boolean, Row>> wrapperStream = MaxDegreeSubset.getWrapperStream(this.fsEnv, this.fsTableEnv, this.vertexStream , this.edgeStream, this.wrapperStream, 
				dataStreamDegree, this.graph);
		return wrapperStream;
	}

	public DataStreamSource<VertexCustom> getVertexStream() throws Exception{
		if (this.wrapperStream == null) throw new Exception("This function can only be used posterior to 'produceWrapperStream' invocation!");
		return this.vertexStream;
	}
	
	public DataStreamSource<EdgeCustom> getEdgeStream() throws Exception{
		if (this.wrapperStream == null) throw new Exception("This function can only be used posterior to 'produceWrapperStream' invocation!");
		return this.edgeStream;
	}
}
