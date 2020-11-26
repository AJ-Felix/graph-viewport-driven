package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import aljoschaRydzyk.viewportDrivenGraphStreaming.VertexDegreeComparator;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperSourceIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperTargetIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperTupleMapWrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperTupleMapWrapperRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.EdgeSourceIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.EdgeTargetIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.VertexIDRowKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.VertexMapIdentityWrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperRowMapWrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInner;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInnerOldNotNew;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInnerNewNotOld;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInside;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedOutside;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsVisualized;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotInsideBefore;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotLayouted;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotVisualized;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterOuter;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterOuterBoth;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexMapIdentityWrapperRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterVisualizedWrappers;

//graphIdGradoop ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; edgeIdGradoop ; edgeLabel

public class GradoopGraphUtil implements GraphUtilSet{
	
	private DataStreamSource<Row> vertexStream;
	private DataStreamSource<Row> wrapperStream = null;
	private Map<String, Integer> vertexIdMap = null;
	private LogicalGraph graph;
	private StreamExecutionEnvironment fsEnv;
	private StreamTableEnvironment fsTableEnv;
	private Set<String> visualizedWrappers;
	private Set<String> visualizedVertices;
	private String vertexFields;
	private String wrapperFields;
	@SuppressWarnings("rawtypes")
	private TypeInformation[] wrapperFormatTypeInfo;
	private RowTypeInfo wrapperRowTypeInfo; 
	private Table wrapperTable;
	private Map<String,Map<String,String>> adjMatrix;
	private FilterFunction<Row> zoomOutVertexFilter;
	private List<EPGMVertex> vertexCollection;
	private List<EPGMEdge> edgeCollection;
	
	//batch
	private DataSet<Row> verticesIndexed;
	private DataSet<EPGMEdge> edges;
	private DataSet<Row> wrapper;
	
	
	//Area Definition
			//A	: Inside viewport after operation
			//B : Outside viewport before and after operation
			//C : Inside viewport before and after operation
			//D : Outside viewport after operation
	
	public GradoopGraphUtil (LogicalGraph graph, StreamExecutionEnvironment fsEnv, StreamTableEnvironment fsTableEnv, String vertexFields, 
			String wrapperFields) {
		this.graph = graph;
		this.fsEnv = fsEnv;
		this.fsTableEnv = fsTableEnv;
		this.vertexFields = vertexFields;
		this.wrapperFields = wrapperFields;
		this.visualizedWrappers = new HashSet<String>();
		this.visualizedVertices = new HashSet<String>();
		this.wrapperFormatTypeInfo = new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING};
		this.wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
	}
	
//	public void loadDataSets() {
//		try {
//			vertexCollection = this.graph.getVertices().collect();
//			edgeCollection = this.graph.getEdges().collect();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
	
	@Override
	public void initializeDataSets() throws Exception{
		String graphId = this.graph.getGraphHead().collect().get(0).getId().toString();
//		String[] somestring = {"hello"};
		verticesIndexed = DataSetUtils.zipWithIndex((this.graph.getVertices()
					.map(new MapFunction<EPGMVertex, Tuple2<EPGMVertex,Long>>() {
						@Override
						public Tuple2<EPGMVertex, Long> map(EPGMVertex vertex) throws Exception {
							return Tuple2.of(vertex, vertex.getPropertyValue("degree").getLong());
						}
					})
					.sortPartition(1, Order.DESCENDING).setParallelism(1)
				))
				.map(new MapFunction<Tuple2<Long,Tuple2<EPGMVertex,Long>>,Row>() {
					@Override
					public Row map(Tuple2<Long,Tuple2<EPGMVertex,Long>> tuple) throws Exception {
						EPGMVertex vertex = tuple.f1.f0;
						return Row.of(graphId, vertex.getId(), tuple.f0, vertex.getLabel(),
								vertex.getPropertyValue("X").getInt(), vertex.getPropertyValue("Y").getInt(),
								vertex.getPropertyValue("degree").getLong());
					}
				});
//				.returns(new TypeHint<Row>(){});
		edges = this.graph.getEdges();
		DataSet<Tuple2<Tuple2<Row, EPGMEdge>, Row>> wrapperTuple = 
				verticesIndexed.join(edges).where(new VertexIDRowKeySelector())
			.equalTo(new EdgeSourceIDKeySelector())
			.join(verticesIndexed).where(new EdgeTargetIDKeySelector())
			.equalTo(new VertexIDRowKeySelector());
		
		DataSet<Row> nonIdentityWrapper = wrapperTuple.map(new MapFunction<Tuple2<Tuple2<Row, EPGMEdge>,Row>,Row>(){
			@Override
			public Row map(Tuple2<Tuple2<Row, EPGMEdge>, Row> tuple) throws Exception {
				EPGMEdge edge = tuple.f0.f1;
				Row sourceVertex = tuple.f0.f0;
				Row targetVertex = tuple.f1;
				Row vertices = Row.join(sourceVertex, Row.project(targetVertex, new int[] {1, 2, 3, 4, 5, 6}));
				System.out.println(vertices.getArity());
				return Row.join(vertices, Row.of(edge.getId().toString(), edge.getLabel()));
			}
		});
		
		DataSet<Row> identityWrapper = verticesIndexed.map(new MapFunction<Row,Row>(){
			@Override
			public Row map(Row row) throws Exception {
				return Row.of(Row.join(row, Row.project(row, new int[] {1, 2, 3, 4, 5, 6})), "identityEdge",
						"identityEdge");
			}
		});
		wrapper = identityWrapper
				.union(nonIdentityWrapper)
				;
	}
	
	@Override
	public void setVisualizedWrappers(Set<String> visualizedWrappers) {
		this.visualizedWrappers = visualizedWrappers;
	}
	
	@Override
	public void setVisualizedVertices(Set<String> visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	public DataStream<Row> getWrapperStream() {
		return this.wrapperStream;
	}
	
	public DataSet<WrapperGVD> getMaxDegreeSubsetGradoop(Integer numberVertices){
		//filter for vertices with degree above cut off
		DataSet<Row> vertices = verticesIndexed.filter(row -> (long) row.getField(2) < numberVertices);
		
		//produce non-identity wrapper
		DataSet<WrapperGVD> wrapperSet = 
				vertices.join(wrapper).where(new VertexIDRowKeySelector())
			.equalTo(new WrapperSourceIDKeySelector())
			.join(vertices).where(new WrapperTargetIDKeySelector())
			.equalTo(new VertexIDRowKeySelector())
			.map(new WrapperTupleMapWrapperGVD());
		
		//produce identity wrapper
		wrapperSet = wrapperSet.union(vertices.map(new VertexMapIdentityWrapperGVD()));
		
		return wrapperSet;
	}
	
	public DataStream<Tuple2<Boolean, Row>> getMaxDegreeSubsetHBase(DataStream<Row> vertexStreamDegree) {
		//aggregate on vertexId to identify vertices that are part of the subset
		Table degreeTable = fsTableEnv.fromDataStream(vertexStreamDegree).as("bool, vertexId, degree");
		fsTableEnv.registerFunction("currentVertex", new CurrentVertex());
		degreeTable = degreeTable
				.groupBy("vertexId")
				.aggregate("currentVertex(bool, vertexId, degree) as (bool, degree)")
				.select("bool, vertexId, degree")
				.filter("bool = 'true'")
				.select("vertexId, degree");		
			//NOTE: Collecting 'bool' changes behaviour critically!
		
		//produce wrappers containing only subset vertices
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		wrapperTable = wrapperTable.join(degreeTable).where("sourceVertexIdGradoop = vertexId").select(this.wrapperFields);
		wrapperTable = wrapperTable.join(degreeTable).where("targetVertexIdGradoop = vertexId").select(this.wrapperFields);
		RowTypeInfo rowTypeInfoWrappers = new RowTypeInfo(this.wrapperFormatTypeInfo, this.wrapperFields.split(","));
		DataStream<Tuple2<Boolean, Row>> wrapperStream = fsTableEnv.toRetractStream(wrapperTable, rowTypeInfoWrappers);	
		return wrapperStream;
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
	
	public DataSet<WrapperGVD> zoom(Float top, Float right, Float bottom, Float left){
		
		//vertex set filter for in-view and out-view area
		DataSet<Row> verticesInner = 
				verticesIndexed.filter(new VertexFilterInner(top, right, bottom, left));
		DataSet<Row> verticesOuter =
				verticesIndexed.filter(new VertexFilterOuter(top, right, bottom, left));

		
		//produce wrapper set from in-view area to in-view area
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> inIn = verticesInner
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesInner).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		
		//wrapper set from in-view area to out-view area and vice versa
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> inOut = verticesInner
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesOuter).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> outIn = verticesOuter
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesInner).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		
		//union
		inIn = inIn.union(inOut).union(outIn);
		
		//map to wrapper row 
		DataSet<Row> wrapperRow = inIn.map(new WrapperTupleMapWrapperRow());
		
		//filter out already visualized edges in wrapper set
		wrapperRow = wrapperRow.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		
		//filter out already visualized vertices in wrapper set (identity wrappers)
		Set<String> visualizedVertices = this.visualizedVertices;
		wrapperRow = wrapperRow.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !(visualizedVertices.contains(value.getField(2).toString()) && value.getField(14).equals("identityEdge"));
			}
		});		
		
		//map to wrapper GVD
		DataSet<WrapperGVD> wrapperGVD = wrapperRow.map(new WrapperRowMapWrapperGVD());
		
		try {
			inIn.print();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return wrapperGVD;
	}
	
	@Override
	public DataSet<WrapperGVD> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, 
			Float rightOld,
			Float bottomOld, Float leftOld){
		
		//vertex set filter for areas A, B and C
		DataSet<Row> verticesInner = verticesIndexed.filter(new VertexFilterInner(topNew, rightNew, bottomNew, 
				leftNew));
		DataSet<Row> verticesInnerNewNotOld = verticesIndexed.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row row) throws Exception {
				int x = (int) row.getField(4);
				int y = (int) row.getField(5);
				return (leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld);
			}
		});
		DataSet<Row> verticesOuterBoth = 
				verticesIndexed.filter(new VertexFilterOuterBoth(leftNew, rightNew, topNew, bottomNew, leftOld, 
						rightOld, topOld, bottomOld));
		DataSet<Row> verticesOldInnerNotNewInner = 
				verticesIndexed.filter(new VertexFilterInnerOldNotNew(leftNew, rightNew, topNew, bottomNew, leftOld, 
						rightOld, topOld, bottomOld));
		
		//produce wrapper set from A to B and vice versa
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperBToA = verticesInnerNewNotOld
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesOuterBoth).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperAToB = verticesOuterBoth
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesInnerNewNotOld).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		
		//produce wrapper set from A to A
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperAToA = verticesInnerNewNotOld
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesInnerNewNotOld).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		
		//produce wrapper set from A+C to D and vice versa
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperAPlusCToD = verticesInner
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesOldInnerNotNewInner).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperDToAPlusC = verticesOldInnerNotNewInner
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesInner).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
			//filter out already visualized edges
			DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperAPlusCD = wrapperAPlusCToD.union(wrapperDToAPlusC);
			DataSet<Row> wrapperRow = wrapperAPlusCD.map(new WrapperTupleMapWrapperRow());
			wrapperRow = wrapperRow.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
			DataSet<WrapperGVD> wrapperGVD = wrapperRow.map(new WrapperRowMapWrapperGVD());
		
		//dataset union
		wrapperGVD = wrapperAToA.union(wrapperAToB).union(wrapperBToA).map(new WrapperTupleMapWrapperGVD())
				.union(wrapperGVD);
		return wrapperGVD;	
	}

	
//	@Override
//	public DataStream<Row> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld,
//			Float bottomOld, Float leftOld){
//		/*
//		 * Pan function for graphs with layout
//		 */
//		
//		//vertex stream filter and conversion to Flink Tables for areas A, B and C
//		DataStream<Row> vertexStreamInner = this.vertexStream.filter(new VertexFilterInner(topNew, rightNew, bottomNew, leftNew));
//		DataStream<Row> vertexStreamInnerNewNotOld = vertexStreamInner.filter(new FilterFunction<Row>() {
//				@Override
//				public boolean filter(Row value) throws Exception {
//					Integer x = (Integer) value.getField(4);
//					Integer y = (Integer) value.getField(5);
//					return (leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld);
//				}
//			});
//		DataStream<Row> vertexStreamOldOuterBoth = this.vertexStream.filter(new VertexFilterOuterBoth(leftNew, rightNew, topNew, bottomNew, leftOld, rightOld, topOld, bottomOld));
//		DataStream<Row> vertexStreamOldInnerNotNewInner = this.vertexStream.filter(new VertexFilterInnerOldNotNew(leftNew, rightNew, topNew, bottomNew, leftOld, rightOld, topOld, bottomOld));
//		Table vertexTableInnerNew = fsTableEnv.fromDataStream(vertexStreamInnerNewNotOld).as(this.vertexFields);
//		Table vertexTableOldOuterExtend = fsTableEnv.fromDataStream(vertexStreamOldOuterBoth).as(this.vertexFields);
//		Table vertexTableOldInNotNewIn = fsTableEnv.fromDataStream(vertexStreamOldInnerNotNewInner).as(this.vertexFields);
//		Table vertexTableInner = fsTableEnv.fromDataStream(vertexStreamInner).as(this.vertexFields);
//		
//		//produce wrapperStream from A to B and vice versa
//		Table wrapperTableInOut = wrapperTable
//				.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
//				.join(vertexTableOldOuterExtend).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		Table wrapperTableOutIn = wrapperTable
//				.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
//				.join(vertexTableOldOuterExtend).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		
//		//produce wrapperStream from A to A
//		Table wrapperTableInIn = wrapperTable
//				.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
//				.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//			//filter out redundant identity edges
//			DataStream<Row> wrapperStreamInIn = fsTableEnv.toAppendStream(wrapperTableInIn, wrapperRowTypeInfo)
////					.filter(new WrapperFilterIdentity())
//					;
//		
//		//produce wrapperStream from A+C to D and vice versa
//		Table wrapperTableOldInNewInInOut = wrapperTable
//				.join(vertexTableInner).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
//				.join(vertexTableOldInNotNewIn).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		Table wrapperTableOldInNewInOutIn = wrapperTable
//				.join(vertexTableOldInNotNewIn).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
//				.join(vertexTableInner).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//			//filter out already visualized edges
//			DataStream<Row> wrapperStreamOldInNewIn = fsTableEnv.toAppendStream(wrapperTableOldInNewInInOut, wrapperRowTypeInfo)
//					.union(fsTableEnv.toAppendStream(wrapperTableOldInNewInOutIn, wrapperRowTypeInfo));	
//			wrapperStreamOldInNewIn = wrapperStreamOldInNewIn.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
//			
//		//stream union
//		DataStream<Row> wrapperStream = wrapperStreamInIn
//				.union(fsTableEnv.toAppendStream(wrapperTableOutIn, wrapperRowTypeInfo))
//				.union(wrapperStreamOldInNewIn)
//				.union(fsTableEnv.toAppendStream(wrapperTableInOut, wrapperRowTypeInfo));
//		return wrapperStream;
//	}

	@Override
	public Map<String, Map<String, String>> buildAdjacencyMatrix() throws Exception {
		this.adjMatrix = new HashMap<String,Map<String,String>>();
//		for (EPGMVertex vertex : vertexCollection) this.adjMatrix.put(vertex.getId().toString(), new HashMap<String,String>());
//		for (EPGMEdge edge : edgeCollection) {
//			String sourceId = edge.getSourceId().toString();
//			String targetId = edge.getTargetId().toString();
//			String edgeId = edge.getId().toString();
//			this.adjMatrix.get(sourceId).put(targetId, edgeId);
//			this.adjMatrix.get(targetId).put(sourceId, edgeId);
//		}
//		System.out.println("adjMatrix built");
		return this.adjMatrix;
	}
	
	@Override
	public Map<String, Map<String, String>> getAdjMatrix() {
		return this.adjMatrix;
	}

}
