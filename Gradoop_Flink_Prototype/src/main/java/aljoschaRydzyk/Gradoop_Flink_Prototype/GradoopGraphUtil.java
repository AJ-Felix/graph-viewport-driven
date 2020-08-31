package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import Temporary.MaxDegreeSubset.CurrentVertex;
import Temporary.MaxDegreeSubset.VertexAccum;

//graphIdGradoop ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; edgeIdGradoop ; edgeLabel

public class GradoopGraphUtil implements GraphUtil{
	
	private DataStreamSource<Row> vertexStream;
	private DataStreamSource<EdgeCustom> edgeStream;
	private DataStreamSource<Row> wrapperStream = null;
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
	public DataStreamSource<Row> produceWrapperStream() throws Exception{
		String graphId = this.graph.getGraphHead().collect().get(0).getId().toString();
		List<EPGMVertex> vertices = this.graph.getVertices().collect();
		this.vertexIdMap = new HashMap<String, Integer>();
		List<Row> customVertices = new ArrayList<Row>();
		List<Row> edgeRow = new ArrayList<Row>();
		for (int i = 0; i < vertices.size(); i++) {
			String vertexIdGradoop = vertices.get(i).getId().toString();
			Integer vertexIdNumeric = (Integer) i;
			this.vertexIdMap.put(vertexIdGradoop, vertexIdNumeric);
			Integer x = ((Integer) vertices.get(i).getPropertyValue("X").getInt());
			Integer y = ((Integer) vertices.get(i).getPropertyValue("Y").getInt());
			Long degree = ((Long) vertices.get(i).getPropertyValue("degree").getLong());
			String vertexLabel = vertices.get(i).getLabel();
			customVertices.add(Row.of(graphId, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, degree));
			edgeRow.add(Row.of(graphId, vertexIdGradoop, vertexIdNumeric, vertexLabel,
					x, y, degree, vertexIdGradoop, vertexIdNumeric, vertexLabel,
					x, y, degree, "identityEdge", "identityEdge"));
		}	
		List<EPGMEdge> edges = this.graph.getEdges().collect();
		List<EdgeCustom> customEdges = new ArrayList<EdgeCustom>();
		for (int i = 0; i < edges.size(); i++) {
			String edgeIdGradoop = edges.get(i).getId().toString();
			String edgeLabel = edges.get(i).getLabel();
			String sourceVertexIdGradoop = edges.get(i).getSourceId().toString();
			String targetVertexIdGradoop = edges.get(i).getTargetId().toString();		
			EdgeCustom edgeCustom = new EdgeCustom(edgeIdGradoop, edgeLabel, sourceVertexIdGradoop, targetVertexIdGradoop);
			customEdges.add(edgeCustom);
			for (Row sourceVertex: customVertices) {
				if (sourceVertex.getField(1).equals(sourceVertexIdGradoop)) {
					for (Row targetVertex: customVertices) {
						if (targetVertex.getField(1).equals(targetVertexIdGradoop)) {
							edgeRow.add(Row.of(graphId, sourceVertexIdGradoop, sourceVertex.getField(2), 
									sourceVertex.getField(3), sourceVertex.getField(4), sourceVertex.getField(5), sourceVertex.getField(6), 
									targetVertexIdGradoop, targetVertex.getField(2), targetVertex.getField(3), targetVertex.getField(4), targetVertex.getField(5), 
									targetVertex.getField(6), edgeIdGradoop, edgeLabel));
						}
					}
				}
			}
		}
		this.vertexStream = fsEnv.fromCollection(customVertices);
		this.edgeStream = fsEnv.fromCollection(customEdges);
		this.wrapperStream = fsEnv.fromCollection(edgeRow);
		return this.wrapperStream;
	}
	
	@Override
	public DataStream<Row> getWrapperStream() {
		return this.wrapperStream;
	}
	
	public DataStream<Tuple2<Boolean, Row>> getMaxDegreeSubset(DataStream<Row> vertexStreamDegree) {
		Table degreeTable = fsTableEnv.fromDataStream(vertexStreamDegree).as("bool, vertexId, degree");
		fsTableEnv.registerFunction("currentVertex", new CurrentVertex());
		degreeTable = degreeTable
				.groupBy("vertexId")
				.aggregate("currentVertex(bool, vertexId, degree) as (bool, degree)")
				.select("bool, vertexId, degree")
				.filter("bool = 'true'")
				.select("vertexId, degree");		
			//sammeln von bool verändert Verhalten kritisch!
		String fieldNames = "graphId, sourceIdGradoop, sourceIdNumeric, sourceLabel, sourceX, sourceY, sourceDegree, targetIdGradoop,"
				+ "targetIdNumeric, targetLabel, targetX, targetY, targetDegree, edgeIdGradoop, edgeLabel";
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(fieldNames);
		wrapperTable = wrapperTable.join(degreeTable).where("sourceIdGradoop = vertexId").select(fieldNames);
		wrapperTable = wrapperTable.join(degreeTable).where("targetIdGradoop = vertexId").select(fieldNames);
		RowTypeInfo rowTypeInfoWrappers = new RowTypeInfo(new TypeInformation[] {
				Types.STRING, Types.STRING, Types.INT, Types.STRING, 
				Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, 
				Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING
				}, new String[] {"graphId", "sourceIdGradoop", "sourceIdNumeric", "sourceLabel", "sourceX", "sourceY", "sourceDegree", 
						"targetIdGradoop", "targetIdNumeric", "targetLabel", "targetX", "targetY", "targetDegree", "edgeIdGradoop", "edgeLabel"});
		DataStream<Tuple2<Boolean, Row>> wrapperStream = fsTableEnv.toRetractStream(wrapperTable, rowTypeInfoWrappers);	
		return wrapperStream;
	}

	public DataStreamSource<Row> getVertexStream(){
		return this.vertexStream;
	}
	
	public DataStreamSource<EdgeCustom> getEdgeStream() throws Exception{
		if (this.wrapperStream == null) throw new Exception("This function can only be used posterior to 'produceWrapperStream' invocation!");
		return this.edgeStream;
	}
	
	public static class VertexAccum {
		String bool;
		String v_id;
		Long degree;
	}
	
	public static class CurrentVertex extends AggregateFunction<Tuple2<String, Long>, VertexAccum>{

		@Override
		public VertexAccum createAccumulator() {
			return new VertexAccum();
		}

		@Override
		public Tuple2<String, Long> getValue(VertexAccum accumulator) {
			return new Tuple2<String, Long>(accumulator.bool, accumulator.degree);
		}
		
		public void accumulate(VertexAccum accumulator, String bool,  String v_id, Long degree) {
			accumulator.bool = bool;
			accumulator.v_id = v_id;
			accumulator.degree = degree;
		}
	}


}
