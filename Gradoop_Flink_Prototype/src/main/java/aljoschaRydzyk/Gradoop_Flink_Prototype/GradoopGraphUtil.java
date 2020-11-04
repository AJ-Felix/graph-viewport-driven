package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
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

//graphIdGradoop ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; edgeIdGradoop ; edgeLabel

//Viewport Zones:
//		A : Area that remains outside of the viewport before and after operation
//		B : Area that is newly inside the viewport after operation
//		C : Area that reamins inside of the viewport before and after operation
//		D : Area that is newly outside the viewport after operation

public class GradoopGraphUtil implements GraphUtil{
	
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
	
	@Override
	public void initializeStreams() throws Exception{
		String graphId = this.graph.getGraphHead().collect().get(0).getId().toString();
		List<EPGMVertex> vertices = this.graph.getVertices().collect();
		vertices.sort(new VertexDegreeComparator());
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
		for (int i = 0; i < edges.size(); i++) {
			String edgeIdGradoop = edges.get(i).getId().toString();
			String edgeLabel = edges.get(i).getLabel();
			String sourceVertexIdGradoop = edges.get(i).getSourceId().toString();
			String targetVertexIdGradoop = edges.get(i).getTargetId().toString();		
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
		this.wrapperStream = fsEnv.fromCollection(edgeRow);
		this.wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
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
	
	public DataStream<Tuple2<Boolean, Row>> getMaxDegreeSubset(DataStream<Row> vertexStreamDegree) {
		
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
	
	@Override
	public DataStreamSource<Row> getVertexStream(){
		return this.vertexStream;
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

	@Override
	public DataStream<Row> zoom(Float topModel, Float rightModel, Float bottomModel, Float leftModel)
			throws IOException {

		//vertex stream filter for in-view and out-view area and conversion to Flink Tables
		DataStream<Row> vertexStreamInner = this.vertexStream.filter(new VertexFilterInner(topModel, rightModel, bottomModel, leftModel));
		DataStream<Row> vertexStreamOuter = this.vertexStream.filter(new VertexFilterOuter(topModel, rightModel, bottomModel, leftModel));
		Table vertexTable = fsTableEnv.fromDataStream(vertexStreamInner).as(this.vertexFields);		
		Table vertexTableOuter = fsTableEnv.fromDataStream(vertexStreamOuter).as(this.vertexFields);

		//filter out already visualized edges in wrapper stream
		DataStream<Row> wrapperStream = this.wrapperStream;
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		
		//filter out already visualized vertices in wrapper stream (identity wrappers)
		Set<String> visualizedVertices = this.visualizedVertices;
		wrapperStream = wrapperStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !(visualizedVertices.contains(value.getField(2).toString()) && value.getField(14).equals("identityEdge"));
			}
		});
		
		//produce wrapper stream from in-view area to in-view area
		Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream).as(this.wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		//produce wrapper stream from in-view area to out-view area and vice versa
		Table wrapperTableInOut = fsTableEnv.fromDataStream(wrapperStream).as(this.wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTableOuter).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOutIn = fsTableEnv.fromDataStream(wrapperStream).as(this.wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTableOuter).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		//stream union
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		wrapperStream = fsTableEnv.toAppendStream(wrapperTable, wrapperRowTypeInfo).union(fsTableEnv.toAppendStream(wrapperTableInOut, wrapperRowTypeInfo))
				.union(fsTableEnv.toAppendStream(wrapperTableOutIn, wrapperRowTypeInfo));
		return wrapperStream;
	}
	
	@Override
	public DataStream<Row> pan(Float top, Float right, Float bottom, Float left, Float xModelDiff, Float yModelDiff){
		Float topOld = top - yModelDiff;
		Float rightOld = right - xModelDiff;
		Float bottomOld = bottom - yModelDiff;
		Float leftOld = left - xModelDiff;
		
		//vertex stream filter and conversion to Flink Tables for areas A, B and C
		DataStream<Row> vertexStreamInner = this.vertexStream.filter(new VertexFilterInner(top, right, bottom, left));
		DataStream<Row> vertexStreamInnerNewNotOld = vertexStreamInner.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld);
				}
			});
		DataStream<Row> vertexStreamOldOuterBoth = this.vertexStream.filter(new VertexFilterOuterBoth(left, right, top, bottom, leftOld, rightOld, topOld, bottomOld));
		DataStream<Row> vertexStreamOldInnerNotNewInner = this.vertexStream.filter(new VertexFilterInnerOldNotNew(left, right, top, bottom, leftOld, rightOld, topOld, bottomOld));
		Table vertexTableInnerNew = fsTableEnv.fromDataStream(vertexStreamInnerNewNotOld).as(this.vertexFields);
		Table vertexTableOldOuterExtend = fsTableEnv.fromDataStream(vertexStreamOldOuterBoth).as(this.vertexFields);
		Table vertexTableOldInNotNewIn = fsTableEnv.fromDataStream(vertexStreamOldInnerNotNewInner).as(this.vertexFields);
		Table vertexTableInner = fsTableEnv.fromDataStream(vertexStreamInner).as(this.vertexFields);
		
		//wrapper stream initialization
		DataStream<Row> wrapperStream = this.wrapperStream;
		Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream).as(this.wrapperFields);
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		
		//produce wrapperStream from A to B and vice versa
		Table wrapperTableInOut = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTableOldOuterExtend).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOutIn = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTableOldOuterExtend).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		
		//produce wrapperStream from A to A
		Table wrapperTableInIn = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableInIn = wrapperTableInIn.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
			//filter out redundant identity edges
			DataStream<Row> wrapperStreamInIn = fsTableEnv.toAppendStream(wrapperTableInIn, wrapperRowTypeInfo).filter(new WrapperFilterIdentity());
		
		//produce wrapperStream from A+C to D and vice versa
		Table wrapperTableOldInNewInInOut = wrapperTable.join(vertexTableInner)
				.where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOldInNewInInOut = wrapperTableOldInNewInInOut.join(vertexTableOldInNotNewIn)
				.where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOldInNewInOutIn = wrapperTable.join(vertexTableOldInNotNewIn)
				.where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOldInNewInOutIn = wrapperTableOldInNewInOutIn.join(vertexTableInner)
				.where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
			//filter out already visualized edges
			DataStream<Row> wrapperStreamOldInNewIn = fsTableEnv.toAppendStream(wrapperTableOldInNewInInOut, wrapperRowTypeInfo)
					.union(fsTableEnv.toAppendStream(wrapperTableOldInNewInOutIn, wrapperRowTypeInfo));	
			wrapperStreamOldInNewIn = wrapperStreamOldInNewIn.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
			
		//stream union
		wrapperStream = wrapperStreamInIn
				.union(fsTableEnv.toAppendStream(wrapperTableOutIn, wrapperRowTypeInfo))
				.union(wrapperStreamOldInNewIn)
				.union(fsTableEnv.toAppendStream(wrapperTableInOut, wrapperRowTypeInfo));
		return wrapperStream;
	}

	@Override
	public Map<String, Map<String, String>> buildAdjacencyMatrix() throws Exception {
		this.adjMatrix = new HashMap<String,Map<String,String>>();
		List<EPGMVertex> vertices = this.graph.getVertices().collect();
		for (EPGMVertex vertex : vertices) this.adjMatrix.put(vertex.getId().toString(), new HashMap<String,String>());
		List<EPGMEdge> edges = this.graph.getEdges().collect();
		for (EPGMEdge edge : edges) {
			String sourceId = edge.getSourceId().toString();
			String targetId = edge.getTargetId().toString();
			String edgeId = edge.getId().toString();
			this.adjMatrix.get(sourceId).put(targetId, edgeId);
			this.adjMatrix.get(targetId).put(sourceId, edgeId);
		}
		System.out.println("adjMatrix built");
		for (Map.Entry<String, Map<String, String>> entry : this.adjMatrix.entrySet()) System.out.println(entry);
		return this.adjMatrix;
	}
	
	@Override
	public Map<String, Map<String, String>> getAdjMatrix() {
		return this.adjMatrix;
	}
	
	public DataStream<Row> panZoomInLayoutFirstStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> innerVertices, 
			Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		/*
		 * First substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that were
		 * layouted before and have their coordinates in the current model window but are not visualized yet.
		 */
		
		DataStream<Row> vertices = this.vertexStream.filter(new VertexFilterIsLayoutedInside(layoutedVertices, topModel, rightModel, bottomModel, leftModel))
			.filter(new VertexFilterNotVisualized(innerVertices));
		Table verticesTable = fsTableEnv.fromDataStream(vertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(verticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(verticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields),
			wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	public DataStream<Row> panZoomInLayoutSecondStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> unionMap){
		/*
		 * Second substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are not yet layouted on the
		 * other hand.
		 */
		
		DataStream<Row> visualizedVertices = this.vertexStream.filter(new VertexFilterIsVisualized(unionMap));
		DataStream<Row> neighbours = this.vertexStream
				.filter(new VertexFilterNotLayouted(layoutedVertices));
		Table visualizedVerticesTable = fsTableEnv.fromDataStream(visualizedVertices).as(this.vertexFields);
		Table neighboursTable = fsTableEnv.fromDataStream(neighbours).as(this.vertexFields);
		DataStream<Row> wrapperStream = 
			fsTableEnv.toAppendStream(wrapperTable
					.join(visualizedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
					.join(neighboursTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
				, wrapperRowTypeInfo)
			.union(fsTableEnv.toAppendStream(wrapperTable
					.join(neighboursTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
					.join(visualizedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
				, wrapperRowTypeInfo));
		return wrapperStream;
	}
	
	public DataStream<Row> panZoomInLayoutThirdStep(Map<String, VertexCustom> layoutedVertices){		
		/*
		 * Third substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * not yet layouted starting with highest degree.
		 */
		DataStream<Row> notLayoutedVertices = this.vertexStream.filter(new VertexFilterNotLayouted(layoutedVertices));
		Table notLayoutedVerticesTable = fsTableEnv.fromDataStream(notLayoutedVertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(notLayoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(notLayoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
			, wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	public DataStream<Row> zoomInLayoutFourthStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> innerVertices, 
			Map<String, VertexCustom> newVertices, Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		/*
		 * Fourth substep for zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		
		//unite maps of already visualized vertices before this zoom-in operation and vertices added in this zoom-in operation
		Map<String,VertexCustom> unionMap = new HashMap<String,VertexCustom>(innerVertices);
		unionMap.putAll(newVertices);
		
		DataStream<Row> visualizedVerticesStream = this.vertexStream.filter(new VertexFilterIsVisualized(unionMap));
		DataStream<Row> layoutedVerticesStream = this.vertexStream.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, 
			topModel, rightModel, bottomModel, leftModel));
		Table visualizedVerticesTable = this.fsTableEnv.fromDataStream(visualizedVerticesStream).as(this.vertexFields);
		Table layoutedVerticesTable = this.fsTableEnv.fromDataStream(layoutedVerticesStream).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(layoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(visualizedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
				, wrapperRowTypeInfo)
			.union(fsTableEnv.toAppendStream(wrapperTable
				.join(visualizedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(layoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
				, wrapperRowTypeInfo));
		
		//filter out already visualized edges in wrapper stream
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return wrapperStream;
	}
	
	public DataStream<Row> panLayoutFourthStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
			Float top, Float right, Float bottom, Float left, Float xModelDiff, Float yModelDiff){
		/*
		 * Fourth substep for pan operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * newly visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		
		//calculate previous model coordinate borders
		Float topOld = top - yModelDiff;
		Float rightOld = right - xModelDiff;
		Float bottomOld = bottom - yModelDiff;
		Float leftOld = left - xModelDiff;
		
		DataStream<Row> newlyAddedInsideVertices = this.vertexStream.filter(new VertexFilterIsVisualized(newVertices))
				.filter(new VertexFilterNotInsideBefore(layoutedVertices, topOld, rightOld, bottomOld, leftOld));
		DataStream<Row> layoutedOutsideVertices = this.vertexStream.filter(new VertexFilterIsLayoutedOutside(layoutedVertices,
				top, right, bottom, left));
		Table newlyAddedInsideVerticesTable = this.fsTableEnv.fromDataStream(newlyAddedInsideVertices).as(this.vertexFields);
		Table layoutedOutsideVerticesTable = this.fsTableEnv.fromDataStream(layoutedOutsideVertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = this.fsTableEnv.toAppendStream(wrapperTable
				.join(newlyAddedInsideVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(layoutedOutsideVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), wrapperRowTypeInfo)
			.union(this.fsTableEnv.toAppendStream(wrapperTable
				.join(layoutedOutsideVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(newlyAddedInsideVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), wrapperRowTypeInfo));
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return wrapperStream;
	}
	
	public DataStream<Row> zoomOutLayoutFirstStep(Map<String, VertexCustom> layoutedVertices, 
			Float topModelNew, Float rightModelNew, Float bottomModelNew, Float leftModelNew, 
			Float topModelOld, Float rightModelOld, Float bottomModelOld, Float leftModelOld){
		/*
		 * First substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * layouted inside the model space which was added by operation.
		 */
		
		zoomOutVertexFilter = new VertexFilterIsLayoutedInnerNewNotOld(layoutedVertices, leftModelNew, rightModelNew, topModelNew, 
				bottomModelNew, leftModelOld, rightModelOld, topModelOld, bottomModelOld);
		DataStream<Row> vertices = this.vertexStream.filter(zoomOutVertexFilter);
		Table verticesTable = fsTableEnv.fromDataStream(vertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(verticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(verticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	public DataStream<Row> zoomOutLayoutSecondStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
			Float topModelNew, Float rightModelNew, Float bottomModelNew, Float leftModelNew){
		/*
		 * Second substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the model space which was added by operation on the one hand, neighbour vertices that are layouted with 
		 * coordinates outside the current model window on the other hand.
		 */
		
		DataStream<Row> newlyVisualizedVertices = this.vertexStream
				.filter(new VertexFilterIsVisualized(newVertices))
				.filter(zoomOutVertexFilter);
		DataStream<Row> layoutedOutsideVertices = this.vertexStream
				.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, topModelNew, rightModelNew, bottomModelNew, leftModelNew));
		Table newlyVisualizedVerticesTable = this.fsTableEnv.fromDataStream(newlyVisualizedVertices).as(this.vertexFields);
		Table layoutedOutsideVerticesTable = this.fsTableEnv.fromDataStream(layoutedOutsideVertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = this.fsTableEnv.toAppendStream(wrapperTable
				.join(newlyVisualizedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(layoutedOutsideVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), 
				wrapperRowTypeInfo)
			.union(this.fsTableEnv.toAppendStream(wrapperTable
				.join(layoutedOutsideVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(newlyVisualizedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), 
				wrapperRowTypeInfo));
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return wrapperStream;
	}
}
