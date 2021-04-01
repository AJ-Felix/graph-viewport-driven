package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.EdgeSourceIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.EdgeTargetIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.VertexIDRowKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.VertexMapIdentityWrapperVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.VertexMapRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.WrapperFilterVisualizedVertices;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.WrapperRowMapWrapperVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.WrapperSourceIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.WrapperTargetIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.WrapperTupleComplexMapRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.WrapperTupleRowMapWrapperVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.WrapperTupleRowMapWrapperRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperVDrive;
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
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterZoomLevel;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexMapIdentityWrapperRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterVisualizedWrappers;

public class GradoopGraphUtil implements GraphUtilSet{
	
	private LogicalGraph graph;
	private Set<String> visualizedWrappers;
	private Set<String> visualizedVertices;
	private FilterFunction<Row> zoomOutVertexFilter;
	private int zoomLevel;
	private DataSet<Row> vertices;
	private DataSet<Row> wrapper;
	
	//Area Definition
			//A	: Inside viewport after operation
			//B : Outside viewport before and after operation
			//C : Inside viewport before and after operation
			//D : Outside viewport after operation
	
	public GradoopGraphUtil (LogicalGraph graph) {
		this.graph = graph;
		this.visualizedWrappers = new HashSet<String>();
		this.visualizedVertices = new HashSet<String>();
	}
	
	
	@Override
	public void initializeDataSets() throws Exception{
		vertices = this.graph.getVertices().map(new VertexMapRow());
		DataSet<EPGMEdge> edges = this.graph.getEdges();
		DataSet<Tuple2<Tuple2<Row, EPGMEdge>, Row>> wrapperTuple = 
				vertices.join(edges).where(new VertexIDRowKeySelector())
			.equalTo(new EdgeSourceIDKeySelector())
			.join(vertices).where(new EdgeTargetIDKeySelector())
			.equalTo(new VertexIDRowKeySelector());
		wrapper = wrapperTuple.map(new WrapperTupleComplexMapRow());
	}
	
	@Override
	public void setVisualizedWrappers(Set<String> visualizedWrappers) {
		this.visualizedWrappers = visualizedWrappers;
	}
	
	@Override
	public void setVisualizedVertices(Set<String> visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	@Override
	public DataSet<WrapperVDrive> getMaxDegreeSubset(int numberVertices){
		
		DataSet<Row> verticesMaxDegree = vertices
				.filter(new VertexFilterZoomLevel(zoomLevel))
				.filter(row -> (long) row.getField(2) < numberVertices);
		
		//produce non-identity wrapper
		DataSet<WrapperVDrive> wrapperSet = 
				verticesMaxDegree.join(wrapper).where(new VertexIDRowKeySelector())
			.equalTo(new WrapperSourceIDKeySelector())
			.join(verticesMaxDegree).where(new WrapperTargetIDKeySelector())
			.equalTo(new VertexIDRowKeySelector())
			.map(new WrapperTupleRowMapWrapperVDrive());
		
		//produce identity wrapper
		wrapperSet = wrapperSet.union(verticesMaxDegree.map(new VertexMapIdentityWrapperVDrive()));
		
		return wrapperSet;
	}
	
	@Override
	public DataSet<WrapperVDrive> zoom(Float top, Float right, Float bottom, Float left){
		
		//zoomLevel
		DataSet<Row> verticesZoomLevel = vertices.filter(new VertexFilterZoomLevel(zoomLevel));
		
		//vertex set filter for in-view and out-view area
		DataSet<Row> verticesInner = 
				verticesZoomLevel.filter(new VertexFilterInner(top, right, bottom, left));
		DataSet<Row> verticesOuter =
				verticesZoomLevel.filter(new VertexFilterOuter(top, right, bottom, left));
		
		//produce identity wrapper set for in-view area
		DataSet<Row> identityWrapper = verticesInner.map(new VertexMapIdentityWrapperRow())
				.filter(new WrapperFilterVisualizedVertices(visualizedVertices));
		
		//produce wrapper set from in-view area to in-view area
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> inIn = verticesInner
				.join(wrapper, JoinHint.REPARTITION_SORT_MERGE).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesInner, JoinHint.REPARTITION_SORT_MERGE).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		
		
		//wrapper set from in-view area to out-view area and vice versa
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> inOut = verticesInner
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesOuter).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> outIn = verticesOuter
				.join(wrapper, JoinHint.REPARTITION_SORT_MERGE).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesInner, JoinHint.REPARTITION_SORT_MERGE).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		
		//union
		inIn = inIn.union(inOut).union(outIn);
		
		//map to wrapper row 
		DataSet<Row> wrapperRow = inIn.map(new WrapperTupleRowMapWrapperRow()).union(identityWrapper);
		
		//filter out already visualized edges in wrapper set
		wrapperRow = wrapperRow.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));

		//map to vdrive wrapper
		DataSet<WrapperVDrive> wrapperVDrive = wrapperRow.map(new WrapperRowMapWrapperVDrive());

		return wrapperVDrive;
	}
	
	@Override
	public DataSet<WrapperVDrive> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, 
			Float rightOld,
			Float bottomOld, Float leftOld){
		
		//zoomLevel 
		DataSet<Row> verticesZoomLevel = vertices.filter(new VertexFilterZoomLevel(zoomLevel));
		
		//vertex set filter for areas A, B and C
		DataSet<Row> verticesInner = verticesZoomLevel.filter(new VertexFilterInner(topNew, rightNew, bottomNew, 
				leftNew));
		DataSet<Row> verticesInnerNewNotOld = verticesInner
				.filter(new VertexFilterOuter(topOld, rightOld, bottomOld, leftOld));
		DataSet<Row> verticesOuterBoth = 
				verticesZoomLevel.filter(new VertexFilterOuterBoth(leftNew, rightNew, topNew, bottomNew, leftOld, 
						rightOld, topOld, bottomOld));
		DataSet<Row> verticesOldInnerNotNewInner = 
				verticesZoomLevel.filter(new VertexFilterInnerOldNotNew(leftNew, rightNew, topNew, bottomNew, leftOld, 
						rightOld, topOld, bottomOld));
		
		//produce identity wrapper for A to A
		DataSet<Row> identityWrapper = verticesInnerNewNotOld.map(new VertexMapIdentityWrapperRow())
				.filter(new WrapperFilterVisualizedVertices(visualizedVertices));
		
		//produce wrapper set from A to B and vice versa
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperBToA = verticesInnerNewNotOld
				.join(wrapper, JoinHint.REPARTITION_SORT_MERGE).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesOuterBoth, JoinHint.REPARTITION_SORT_MERGE).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperAToB = verticesOuterBoth
				.join(wrapper, JoinHint.REPARTITION_SORT_MERGE).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesInnerNewNotOld, JoinHint.REPARTITION_SORT_MERGE).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		
		//produce wrapper set from A to A
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperAToA = verticesInnerNewNotOld
				.join(wrapper, JoinHint.REPARTITION_SORT_MERGE).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesInnerNewNotOld, JoinHint.REPARTITION_SORT_MERGE).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		
		//produce wrapper set from A+C to D and vice versa
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperAPlusCToD = verticesInner
				.join(wrapper, JoinHint.REPARTITION_SORT_MERGE).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesOldInnerNotNewInner, JoinHint.REPARTITION_SORT_MERGE).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperDToAPlusC = verticesOldInnerNotNewInner
				.join(wrapper, JoinHint.REPARTITION_SORT_MERGE).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesInner, JoinHint.REPARTITION_SORT_MERGE).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
			//filter out already visualized edges
			DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperAPlusCD = wrapperAPlusCToD.union(wrapperDToAPlusC);
			DataSet<Row> wrapperRow = wrapperAPlusCD.map(new WrapperTupleRowMapWrapperRow());
			wrapperRow = wrapperRow.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
			DataSet<WrapperVDrive> wrapperVDrive = wrapperRow.union(identityWrapper)
					.map(new WrapperRowMapWrapperVDrive());
		
		//dataset union
		wrapperVDrive = wrapperAToA.union(wrapperAToB).union(wrapperBToA).map(new WrapperTupleRowMapWrapperVDrive())
				.union(wrapperVDrive);
		
		return wrapperVDrive;	
	}
	
	@Override
	public DataSet<WrapperVDrive> panZoomInLayoutStep1(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> innerVertices, 
			Float top, Float right, Float bottom, Float left){
		/*
		 * First substep for pan/zoom-in operation on graphs without layout. Returns a DataSet of wrappers including vertices that were
		 * layouted before and have their coordinates in the current model window but are not visualized yet.
		 */
		Set<String> innerVerticeskeySet = new HashSet<String>(innerVertices.keySet());
		DataSet<Row> verticesLayoutedInsideNotVisualized = vertices
				.filter(new VertexFilterIsLayoutedInside(layoutedVertices, top, right, bottom, left))
				.filter(new VertexFilterNotVisualized(innerVerticeskeySet))
				.filter(new VertexFilterZoomLevel(zoomLevel));
		DataSet<WrapperVDrive> wrapperVDriveIdentity = verticesLayoutedInsideNotVisualized.map(new VertexMapIdentityWrapperVDrive());
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTupleNonIdentity = verticesLayoutedInsideNotVisualized
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesLayoutedInsideNotVisualized).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		return wrapperVDriveIdentity.union(wrapperTupleNonIdentity.map(new WrapperTupleRowMapWrapperVDrive()));
	}
	
	@Override
	public DataSet<WrapperVDrive> panZoomInLayoutStep2(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> unionMap){
		/*
		 * Second substep for pan/zoom-in operation on graphs without layout. Returns a DataSet of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are not yet layouted on the
		 * other hand.
		 */
		
		Set<String> layoutedVerticesKeySet = new HashSet<String>(layoutedVertices.keySet());
		Set<String> unionKeySet = new HashSet<String>(unionMap.keySet());
		DataSet<Row> visualizedVertices = vertices.filter(new VertexFilterIsVisualized(unionKeySet));
		DataSet<Row> neighbours = vertices
				.filter(new VertexFilterNotLayouted(layoutedVerticesKeySet))
				.filter(new VertexFilterZoomLevel(zoomLevel));
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
		DataSet<WrapperVDrive> wrapperVDrive = wrapperTuple.map(new WrapperTupleRowMapWrapperVDrive());
		return wrapperVDrive;
	}
	
	@Override
	public DataSet<WrapperVDrive> panZoomInLayoutStep3(Map<String, VertexVDrive> layoutedVertices){		
		/*
		 * Third substep for pan/zoom-in operation on graphs without layout. Returns a DataSet of wrappers including vertices that are 
		 * not yet layouted starting with highest degree.
		 */
		Set<String> layoutedVerticesKeySet = new HashSet<String>(layoutedVertices.keySet());
		DataSet<Row> notLayoutedVertices = vertices
				.filter(new VertexFilterNotLayouted(layoutedVerticesKeySet))
				.filter(new VertexFilterZoomLevel(zoomLevel));
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTuple = notLayoutedVertices
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(notLayoutedVertices).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		DataSet<WrapperVDrive> wrapperVDriveIdentity = notLayoutedVertices.map(new VertexMapIdentityWrapperVDrive());
		DataSet<WrapperVDrive> wrapperVDrive = wrapperTuple.map(new WrapperTupleRowMapWrapperVDrive()).union(wrapperVDriveIdentity);
		return wrapperVDrive;
	}
	
	@Override
	public DataSet<WrapperVDrive> zoomInLayoutStep4(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> innerVertices, 
			Map<String, VertexVDrive> newVertices, Float top, Float right, Float bottom, Float left){
		/*
		 * Fourth substep for zoom-in operation on graphs without layout. Returns a DataSet of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		
		//unite maps of already visualized vertices before this zoom-in operation and vertices added in this zoom-in operation
		Map<String,VertexVDrive> unionMap = new HashMap<String,VertexVDrive>(innerVertices);
		unionMap.putAll(newVertices);
		
		Set<String> unionKeySet = new HashSet<String>(unionMap.keySet());
		DataSet<Row> visualizedVerticesSet = vertices.filter(new VertexFilterIsVisualized(unionKeySet));
		DataSet<Row> layoutedVerticesSet = vertices
				.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, top, right, bottom, left))
				.filter(new VertexFilterZoomLevel(zoomLevel));
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
		DataSet<WrapperVDrive> wrapperVDrive = wrapperTuple.map(new WrapperTupleRowMapWrapperRow())
				.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers))
				.map(new WrapperRowMapWrapperVDrive());
		return wrapperVDrive;
	}
	
	@Override
	public DataSet<WrapperVDrive> panLayoutStep4(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices, 
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld,
			Float leftOld){
		/*
		 * Fourth substep for pan operation on graphs without layout. Returns a DataSet of wrappers including vertices that are 
		 * newly visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		
		//produce wrapper set from C To D and vice versa
		Set<String> newVerticesKeySet = new HashSet<String>(newVertices.keySet());
		DataSet<Row> cVertices = vertices.filter(new VertexFilterIsVisualized(newVerticesKeySet))
				.filter(new VertexFilterIsLayoutedInside(layoutedVertices, topOld, rightOld, bottomOld, leftOld));
		DataSet<Row> dVertices = vertices
				.filter(new VertexFilterIsLayoutedInnerOldNotNew(layoutedVertices,
				topNew, rightNew, bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld))
				.filter(new VertexFilterZoomLevel(zoomLevel));
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
		DataSet<Row> aVertices = vertices.filter(new VertexFilterIsVisualized(newVerticesKeySet))
				.filter(new VertexFilterIsLayoutedInnerNewNotOld(layoutedVertices,
						topNew, rightNew, bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld));
		DataSet<Row> bdVertices = vertices
				.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, topNew, rightNew, bottomNew, leftNew))
				.filter(new VertexFilterZoomLevel(zoomLevel));
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
		DataSet<WrapperVDrive> wrapperVDrive = wrapperTuple.union(wrapperTuple2).map(new WrapperTupleRowMapWrapperRow())
				.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers))
				.map(new WrapperRowMapWrapperVDrive());
		return wrapperVDrive;
	}
	
	@Override
	public DataSet<WrapperVDrive> zoomOutLayoutStep1(Map<String, VertexVDrive> layoutedVertices, 
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, 
			Float topOld, Float rightOld, Float bottomOld, Float leftOld){
		/*
		 * First substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * layouted inside the model space which was added by operation.
		 */
		
		zoomOutVertexFilter = new VertexFilterIsLayoutedInnerNewNotOld(layoutedVertices, topNew, rightNew, bottomNew, 
				leftNew, topOld, rightOld, bottomOld, leftOld);
		DataSet<Row> verticesLayoutedInnerNewNotOld = vertices
				.filter(zoomOutVertexFilter)
				.filter(new VertexFilterZoomLevel(zoomLevel));
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperTuple = verticesLayoutedInnerNewNotOld
				.join(wrapper).where(new VertexIDRowKeySelector())
				.equalTo(new WrapperSourceIDKeySelector())
				.join(verticesLayoutedInnerNewNotOld).where(new WrapperTargetIDKeySelector())
				.equalTo(new VertexIDRowKeySelector());
		DataSet<WrapperVDrive> wrapperVDriveIdentity = verticesLayoutedInnerNewNotOld.map(new VertexMapIdentityWrapperVDrive());
		DataSet<WrapperVDrive> wrapperVDrive = wrapperTuple.map(new WrapperTupleRowMapWrapperVDrive()).union(wrapperVDriveIdentity);
		return wrapperVDrive;
	}
	
	@Override
	public DataSet<WrapperVDrive> zoomOutLayoutStep2(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices, 
			Float top, Float right, Float bottom, Float left){
		/*
		 * Second substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the model space which was added by operation on the one hand, neighbour vertices that are layouted with 
		 * coordinates outside the current model window on the other hand.
		 */
		
		Set<String> newVerticesKeySet = new HashSet<String>(newVertices.keySet());
		DataSet<Row> newlyVisualizedVertices = vertices
				.filter(new VertexFilterIsVisualized(newVerticesKeySet))
				.filter(zoomOutVertexFilter);
		DataSet<Row> layoutedOutsideVertices = vertices
				.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, top, right, bottom, left))
				.filter(new VertexFilterZoomLevel(zoomLevel));
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
		DataSet<WrapperVDrive> wrapperVDrive = wrapperTuple.map(new WrapperTupleRowMapWrapperRow())
				.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers))
				.map(new WrapperRowMapWrapperVDrive());
		return wrapperVDrive;
	}

	@Override
	public void setVertexZoomLevel(int zoomLevel) {
		this.zoomLevel = zoomLevel;
	}
}
