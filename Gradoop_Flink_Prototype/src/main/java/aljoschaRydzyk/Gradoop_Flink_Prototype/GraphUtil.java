package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

public class GraphUtil {
	private DataStreamSource<Tuple5<String, String, String, String, String>> vertexStream;
	private DataStreamSource<Tuple5<String, String, String, String, String>> edgeStream;
	private Map<String, String> vertexIdMap = null;
	private LogicalGraph graph;
	private StreamExecutionEnvironment fsEnv;
	private StreamTableEnvironment fsTableEnv;
	
	public GraphUtil (LogicalGraph graph, StreamExecutionEnvironment fsEnv, StreamTableEnvironment fsTableEnv) {
		this.graph = graph;
		this.fsEnv = fsEnv;
		this.fsTableEnv = fsTableEnv;
	}
	
	public DataStreamSource<Tuple5<String, String, String, String, String>> produceVertexStream() throws Exception {
		List<EPGMVertex> vertices = this.graph.getVertices().collect();
		this.vertexIdMap = new HashMap<String, String>();
		List<Tuple5<String, String, String, String, String>> tupledVertices = new ArrayList<Tuple5<String, String, String, String, String>>();
		for (int i = 0; i < vertices.size(); i++) {
			String vertexId = vertices.get(i).getId().toString();
			this.vertexIdMap.put(vertexId, ((Integer) i).toString());
			String x = ((Integer) vertices.get(i).getPropertyValue("X").getInt()).toString();
			String y = ((Integer) vertices.get(i).getPropertyValue("Y").getInt()).toString();
			String label = vertices.get(i).getLabel();
			tupledVertices.add(new Tuple5<String, String, String, String, String>(vertexId, label, ((Integer) i).toString(), x, y));
		}	
		this.vertexStream = fsEnv.fromCollection(tupledVertices);
		return this.vertexStream;
	}
	
	public DataStreamSource<Tuple5<String, String, String, String, String>> produceEdgeStream() throws Exception {
		if (this.vertexIdMap == null) throw new Exception("This method is only applicable when vertex ID map is available (invoke getVertexStream()!");
		List<EPGMEdge> edges = this.graph.getEdges().collect();
		List<Tuple5<String, String, String, String, String>> tupledEdges = new ArrayList<Tuple5<String, String, String, String, String>>();
		for (int i = 0; i < edges.size(); i++) {
			String edgeId = edges.get(i).getId().toString();
			String oldIdVertex1 = edges.get(i).getSourceId().toString();
			String newIdVertex1 = this.vertexIdMap.get(oldIdVertex1);
			String oldIdVertex2 = edges.get(i).getTargetId().toString();		
			String newIdVertex2 = this.vertexIdMap.get(oldIdVertex2);
			tupledEdges.add(new Tuple5<String, String, String, String, String>(edgeId, oldIdVertex1, oldIdVertex2, newIdVertex1, newIdVertex2));
		}
		this.edgeStream = fsEnv.fromCollection(tupledEdges);
		return this.edgeStream;
	}
	
	public List<DataStream<Tuple2<Boolean, Row>>> getMaxDegreeSubset(DataStream<Tuple2<Boolean, Row>> datastreamDegree) throws Exception {
		DataStreamSource<Tuple5<String, String, String, String, String>> vertexStream = this.produceVertexStream();
		DataStreamSource<Tuple5<String, String, String, String, String>> edgeStream = this.produceEdgeStream();
		return MaxDegreeSubset.getStreams(this.fsEnv, this.fsTableEnv, vertexStream , edgeStream, datastreamDegree, this.graph);
	}
	
	public DataStreamSource<Tuple5<String, String, String, String, String>> getVertexStream(){
		return this.vertexStream;
	}
}
