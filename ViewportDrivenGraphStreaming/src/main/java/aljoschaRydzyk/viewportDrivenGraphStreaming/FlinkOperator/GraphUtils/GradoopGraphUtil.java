package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.EdgeSourceIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.EdgeTargetIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.VertexIDRowKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.VertexMapIdentityWrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperFilterVisualizedVertices;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperRowMapWrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperSourceIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperTargetIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperTupleMapWrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperTupleMapWrapperRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInner;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInnerOldNotNew;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInnerNewNotOld;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInnerOldNotNew;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInside;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedOutside;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsVisualized;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotLayouted;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotVisualized;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterOuter;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterOuterBoth;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexMapIdentityWrapperRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterVisualizedWrappers;

public class GradoopGraphUtil implements GraphUtilSet{
	
	private DataStreamSource<Row> wrapperStream = null;
	private LogicalGraph graph;
	private StreamTableEnvironment fsTableEnv;
	private Set<String> visualizedWrappers;
	private Set<String> visualizedVertices;
	private String wrapperFields;
	@SuppressWarnings("rawtypes")
	private TypeInformation[] wrapperFormatTypeInfo;
	private FilterFunction<Row> zoomOutVertexFilter;
	private int zoomLevelCoefficient = 1000;

	
	//Batch
	private DataSet<Row> verticesIndexed;
	private DataSet<Row> wrapper;
	
	
	//Area Definition
			//A	: Inside viewport after operation
			//B : Outside viewport before and after operation
			//C : Inside viewport before and after operation
			//D : Outside viewport after operation
	
	public GradoopGraphUtil (LogicalGraph graph, StreamTableEnvironment fsTableEnv, String wrapperFields) {
		this.graph = graph;
		this.fsTableEnv = fsTableEnv;
		this.wrapperFields = wrapperFields;
		this.visualizedWrappers = new HashSet<String>();
		this.visualizedVertices = new HashSet<String>();
		this.wrapperFormatTypeInfo = new TypeInformation[] {
				Types.STRING, 
				Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.INT,
				Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.INT,
				Types.STRING, Types.STRING};
	}
	
	
	@Override
	public void initializeDataSets() throws Exception{
		System.out.println(this.graph);
		System.out.println("graphHeadSize: " + this.graph.getGraphHead().collect().size());
		String graphId = this.graph.getGraphHead().collect().get(0).getId().toString();
		int numberVertices = Integer.parseInt(String.valueOf(this.graph.getVertices().count()));
		int numberZoomLevels = (numberVertices + zoomLevelCoefficient - 1) / zoomLevelCoefficient;
		int zoomLevelSetSize = (numberVertices + numberZoomLevels - 1) / numberZoomLevels;
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
								vertex.getPropertyValue("degree").getLong(), 
								Integer.parseInt(String.valueOf(tuple.f0)) / zoomLevelSetSize);
					}
				});
		DataSet<EPGMEdge> edges = this.graph.getEdges();
		DataSet<Tuple2<Tuple2<Row, EPGMEdge>, Row>> wrapperTuple = 
				verticesIndexed.join(edges).where(new VertexIDRowKeySelector())
			.equalTo(new EdgeSourceIDKeySelector())
			.join(verticesIndexed).where(new EdgeTargetIDKeySelector())
			.equalTo(new VertexIDRowKeySelector());
		
		wrapper = wrapperTuple.map(new MapFunction<Tuple2<Tuple2<Row, EPGMEdge>,Row>,Row>(){
			@Override
			public Row map(Tuple2<Tuple2<Row, EPGMEdge>, Row> tuple) throws Exception {
				EPGMEdge edge = tuple.f0.f1;
				Row sourceVertex = tuple.f0.f0;
				Row targetVertex = tuple.f1;
				Row vertices = Row.join(sourceVertex, Row.project(targetVertex, new int[] {1, 2, 3, 4, 5, 6, 7}));
				System.out.println(vertices.getArity());
				return Row.join(vertices, Row.of(edge.getId().toString(), edge.getLabel()));
			}
		});
	}
	
	@Override
	public void setVisualizedWrappers(Set<String> visualizedWrappers) {
		this.visualizedWrappers = visualizedWrappers;
	}
	
	@Override
	public void setVisualizedVertices(Set<String> visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	public DataSet<WrapperGVD> getMaxDegreeSubsetGradoop(int numberVertices){
		//filter for vertices with degree above cut off
		DataSet<Row> vertices = verticesIndexed.filter(row -> (long) row.getField(2) < numberVertices);
		
		//produce non-identity wrapper
		DataSet<WrapperGVD> wrapperSet = 
				vertices.join(wrapper).where(new VertexIDRowKeySelector())
			.equalTo(new WrapperSourceIDKeySelector())
			.join(vertices).where(new WrapperTargetIDKeySelector())
			.equalTo(new VertexIDRowKeySelector())
			.map(new WrapperTupleMapWrapperGVD());
		try {
			wrapperSet.print();
			System.out.println("wrapperSetSize: " + wrapperSet.count());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
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
	
	@Override
	public DataSet<WrapperGVD> zoom(Float top, Float right, Float bottom, Float left){
		
		//vertex set filter for in-view and out-view area
		DataSet<Row> verticesInner = 
				verticesIndexed.filter(new VertexFilterInner(top, right, bottom, left));
		DataSet<Row> verticesOuter =
				verticesIndexed.filter(new VertexFilterOuter(top, right, bottom, left));
		
		//produce identity wrapper set for in-view area
		DataSet<Row> identityWrapper = verticesInner.map(new VertexMapIdentityWrapperRow())
				.filter(new WrapperFilterVisualizedVertices(visualizedVertices));
		
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
		DataSet<Row> wrapperRow = inIn.map(new WrapperTupleMapWrapperRow()).union(identityWrapper);
		
		//filter out already visualized edges in wrapper set
		wrapperRow = wrapperRow.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));

		//map to wrapper GVD
		DataSet<WrapperGVD> wrapperGVD = wrapperRow.map(new WrapperRowMapWrapperGVD());

		return wrapperGVD;
	}
	
	@Override
	public DataSet<WrapperGVD> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, 
			Float rightOld,
			Float bottomOld, Float leftOld){
		
		//vertex set filter for areas A, B and C
		DataSet<Row> verticesInner = verticesIndexed.filter(new VertexFilterInner(topNew, rightNew, bottomNew, 
				leftNew));
		DataSet<Row> verticesInnerNewNotOld = verticesInner
				.filter(new VertexFilterOuter(topOld, rightOld, bottomOld, leftOld));
		DataSet<Row> verticesOuterBoth = 
				verticesIndexed.filter(new VertexFilterOuterBoth(leftNew, rightNew, topNew, bottomNew, leftOld, 
						rightOld, topOld, bottomOld));
		DataSet<Row> verticesOldInnerNotNewInner = 
				verticesIndexed.filter(new VertexFilterInnerOldNotNew(leftNew, rightNew, topNew, bottomNew, leftOld, 
						rightOld, topOld, bottomOld));
		
		//produce identity wrapper for A to A
		DataSet<Row> identityWrapper = verticesInnerNewNotOld.map(new VertexMapIdentityWrapperRow())
				.filter(new WrapperFilterVisualizedVertices(visualizedVertices));
		
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
			DataSet<WrapperGVD> wrapperGVD = wrapperRow.union(identityWrapper)
					.map(new WrapperRowMapWrapperGVD());
		
		//dataset union
		wrapperGVD = wrapperAToA.union(wrapperAToB).union(wrapperBToA).map(new WrapperTupleMapWrapperGVD())
				.union(wrapperGVD);
		
		return wrapperGVD;	
	}
	
	@Override
	public DataSet<WrapperGVD> panZoomInLayoutStep1(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices, 
			Float top, Float right, Float bottom, Float left){
		/*
		 * First substep for pan/zoom-in operation on graphs without layout. Returns a DataSet of wrappers including vertices that were
		 * layouted before and have their coordinates in the current model window but are not visualized yet.
		 */
		Set<String> innerVerticeskeySet = new HashSet<String>(innerVertices.keySet());
		DataSet<Row> vertices = verticesIndexed.filter(new VertexFilterIsLayoutedInside(layoutedVertices, top, right, bottom, left))
			.filter(new VertexFilterNotVisualized(innerVerticeskeySet));
		DataSet<WrapperGVD> wrapperGVDIdentity = vertices.map(new VertexMapIdentityWrapperGVD());
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTupleNonIdentity = vertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(vertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		return wrapperGVDIdentity.union(wrapperTupleNonIdentity.map(new WrapperTupleMapWrapperGVD()));
	}
	
	@Override
	public DataSet<WrapperGVD> panZoomInLayoutStep2(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> unionMap){
		/*
		 * Second substep for pan/zoom-in operation on graphs without layout. Returns a DataSet of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are not yet layouted on the
		 * other hand.
		 */
		Set<String> layoutedVerticesKeySet = new HashSet<String>(layoutedVertices.keySet());
		Set<String> unionKeySet = new HashSet<String>(unionMap.keySet());
		DataSet<Row> visualizedVertices = verticesIndexed.filter(new VertexFilterIsVisualized(unionKeySet));
		DataSet<Row> neighbours = verticesIndexed.filter(new VertexFilterNotLayouted(layoutedVerticesKeySet));
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTuple = visualizedVertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(neighbours).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector())
			.union(neighbours
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(visualizedVertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector()));
		DataSet<WrapperGVD> wrapperGVD = wrapperTuple.map(new WrapperTupleMapWrapperGVD());
		return wrapperGVD;
	}
	
	@Override
	public DataSet<WrapperGVD> panZoomInLayoutStep3(Map<String, VertexGVD> layoutedVertices){		
		/*
		 * Third substep for pan/zoom-in operation on graphs without layout. Returns a DataSet of wrappers including vertices that are 
		 * not yet layouted starting with highest degree.
		 */
		System.out.println("layoutedVertices size" + layoutedVertices.size());
		Set<String> layoutedVerticesKeySet = new HashSet<String>(layoutedVertices.keySet());
		DataSet<Row> notLayoutedVertices = verticesIndexed.filter(new VertexFilterNotLayouted(layoutedVerticesKeySet));
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTuple = notLayoutedVertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(notLayoutedVertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		DataSet<WrapperGVD> wrapperGVDIdentity = notLayoutedVertices.map(new VertexMapIdentityWrapperGVD());
		DataSet<WrapperGVD> wrapperGVD = wrapperTuple.map(new WrapperTupleMapWrapperGVD()).union(wrapperGVDIdentity);
		return wrapperGVD;
	}
	
	@Override
	public DataSet<WrapperGVD> zoomInLayoutStep4(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices, 
			Map<String, VertexGVD> newVertices, Float top, Float right, Float bottom, Float left){
		/*
		 * Fourth substep for zoom-in operation on graphs without layout. Returns a DataSet of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		
		//unite maps of already visualized vertices before this zoom-in operation and vertices added in this zoom-in operation
		Map<String,VertexGVD> unionMap = new HashMap<String,VertexGVD>(innerVertices);
		unionMap.putAll(newVertices);
		
		Set<String> unionKeySet = new HashSet<String>(unionMap.keySet());
		DataSet<Row> visualizedVerticesSet = verticesIndexed.filter(new VertexFilterIsVisualized(unionKeySet));
		DataSet<Row> layoutedVerticesSet = verticesIndexed.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, 
			top, right, bottom, left));
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTuple = visualizedVerticesSet
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(layoutedVerticesSet).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector())
			.union(layoutedVerticesSet
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(visualizedVerticesSet).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector()));

		//filter out already visualized edges in wrapper set
		DataSet<WrapperGVD> wrapperGVD = wrapperTuple.map(new WrapperTupleMapWrapperRow())
				.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers))
				.map(new WrapperRowMapWrapperGVD());
		return wrapperGVD;
	}
	
	@Override
	public DataSet<WrapperGVD> panLayoutStep4(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices, 
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld,
			Float leftOld){
		/*
		 * Fourth substep for pan operation on graphs without layout. Returns a DataSet of wrappers including vertices that are 
		 * newly visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		
		//produce wrapper set from C To D and vice versa
		Set<String> newVerticesKeySet = new HashSet<String>(newVertices.keySet());
		DataSet<Row> cVertices = verticesIndexed.filter(new VertexFilterIsVisualized(newVerticesKeySet))
				.filter(new VertexFilterIsLayoutedInside(layoutedVertices, topOld, rightOld, bottomOld, leftOld));
		DataSet<Row> dVertices = verticesIndexed
				.filter(new VertexFilterIsLayoutedInnerOldNotNew(layoutedVertices,
				topNew, rightNew, bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld));
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTuple = cVertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(dVertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector())
			.union(dVertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(cVertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector()));

		//produce wrapper set from A to B+D and vice versa
		DataSet<Row> aVertices = verticesIndexed.filter(new VertexFilterIsVisualized(newVerticesKeySet))
				.filter(new VertexFilterIsLayoutedInnerNewNotOld(layoutedVertices,
						topNew, rightNew, bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld));
		DataSet<Row> bdVertices = verticesIndexed.filter(new VertexFilterIsLayoutedOutside(
				layoutedVertices, topNew, rightNew, bottomNew, leftNew));
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTuple2 = aVertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(bdVertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector())
			.union(bdVertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(aVertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector()));
		
		//filter out already visualized edges in wrapper set
		DataSet<WrapperGVD> wrapperGVD = wrapperTuple.union(wrapperTuple2).map(new WrapperTupleMapWrapperRow())
				.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers))
				.map(new WrapperRowMapWrapperGVD());
		return wrapperGVD;
	}
	
	@Override
	public DataSet<WrapperGVD> zoomOutLayoutStep1(Map<String, VertexGVD> layoutedVertices, 
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, 
			Float topOld, Float rightOld, Float bottomOld, Float leftOld){
		/*
		 * First substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * layouted inside the model space which was added by operation.
		 */
		
		zoomOutVertexFilter = new VertexFilterIsLayoutedInnerNewNotOld(layoutedVertices, topNew, rightNew, bottomNew, 
				leftNew, topOld, rightOld, bottomOld, leftOld);
		DataSet<Row> vertices = verticesIndexed.filter(zoomOutVertexFilter);
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTuple = vertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(vertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		DataSet<WrapperGVD> wrapperGVDIdentity = vertices.map(new VertexMapIdentityWrapperGVD());
		DataSet<WrapperGVD> wrapperGVD = wrapperTuple.map(new WrapperTupleMapWrapperGVD()).union(wrapperGVDIdentity);
		return wrapperGVD;
	}
	
	@Override
	public DataSet<WrapperGVD> zoomOutLayoutStep2(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices, 
			Float top, Float right, Float bottom, Float left){
		/*
		 * Second substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the model space which was added by operation on the one hand, neighbour vertices that are layouted with 
		 * coordinates outside the current model window on the other hand.
		 */
		
		Set<String> newVerticesKeySet = new HashSet<String>(newVertices.keySet());
		DataSet<Row> newlyVisualizedVertices = verticesIndexed
				.filter(new VertexFilterIsVisualized(newVerticesKeySet))
				.filter(zoomOutVertexFilter);
		DataSet<Row> layoutedOutsideVertices = verticesIndexed
				.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, top, right, bottom, left));
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTuple = newlyVisualizedVertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(layoutedOutsideVertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector())
			.union(layoutedOutsideVertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(newlyVisualizedVertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector()));
		
		//filter out already visualized edges in wrapper set
		DataSet<WrapperGVD> wrapperGVD = wrapperTuple.map(new WrapperTupleMapWrapperRow())
				.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers))
				.map(new WrapperRowMapWrapperGVD());
		return wrapperGVD;
	}
}
