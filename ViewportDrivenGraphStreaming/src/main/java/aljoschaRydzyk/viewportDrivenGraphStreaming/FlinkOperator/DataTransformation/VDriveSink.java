package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.EdgeSourceIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.EdgeTargetIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.VertexEPGMMapTupleDegreeComplex;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.VertexIDRowKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.VertexTupleComplexMapRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.WrapperTupleComplexMapRow;



public class VDriveSink {
	private int zoomLevelCoefficient;
	
	public VDriveSink(int zoomLevelCoefficient) {
		this.zoomLevelCoefficient = zoomLevelCoefficient;
	}
	
	
	public void parseGradoopToVDrive(LogicalGraph graph, String outPath, String graphId) throws Exception {
		/*
		 * Builds all VDrive format files into 'outPath' directory from given logical graph.
		 */
		int numberVertices = Integer.parseInt(String.valueOf(graph.getVertices().count()));
		int numberZoomLevels = (numberVertices + zoomLevelCoefficient - 1) / zoomLevelCoefficient;
		int zoomLevelSetSize = (numberVertices + numberZoomLevels - 1) / numberZoomLevels;
		
		//build vertices file
		DataSet<Row> vertices = DataSetUtils.zipWithIndex((graph.getVertices()
				.map(new VertexEPGMMapTupleDegreeComplex())
				.sortPartition(1, Order.DESCENDING)
				.setParallelism(1)
			))
			.map(new VertexTupleComplexMapRow(graphId, zoomLevelSetSize));
		
		MapOperator<Row, Tuple8<String, String, Long, String, Integer, Integer, Long, Integer>> verticesTupled = 
				vertices.map(new VerticesMapTuple());
		
		verticesTupled.writeAsCsv(outPath + "/vertices", "\n", ";", WriteMode.OVERWRITE).setParallelism(1);
		
		//build wrappers file
		DataSet<EPGMEdge> edges = graph.getEdges();
		DataSet<Row> wrapper = 
				vertices.join(edges, JoinHint.REPARTITION_SORT_MERGE).where(new VertexIDRowKeySelector())
			.equalTo(new EdgeSourceIDKeySelector())
			.join(vertices, JoinHint.REPARTITION_SORT_MERGE).where(new EdgeTargetIDKeySelector())
			.equalTo(new VertexIDRowKeySelector())
			.map(new WrapperTupleComplexMapRow());
		MapOperator<Row, Tuple17<
			String, 
			String, Long, String, Integer, Integer, Long, Integer, 
			String, Long, String, Integer, Integer, Long, Integer, 
			String, String>> wrapperTupled = wrapper
				.map(new WrapperMapTuple());
		wrapperTupled.writeAsCsv(outPath + "/wrappers", "\n", ";", WriteMode.OVERWRITE).setParallelism(1);
		
		//build adjacency file
		MapOperator<Row, Tuple3<String, String, String>> idsSourceTargetWrapper = 
				wrapper.map(new WrapperMapIDsSourceTarget());
		MapOperator<Row, Tuple3<String, String, String>> idsTargetSourceWrapper = 
				wrapper.map(new WrapperMapIDsTargetSource());
		UnionOperator<Tuple3<String, String, String>> idsUnited = idsSourceTargetWrapper
				.union(idsTargetSourceWrapper);
		MapOperator<Tuple3<String, String, String>, AdjacencyRow> 
		adjaRow = idsUnited.map(new IDsMapAdjacencyRow());	
		UnsortedGrouping<AdjacencyRow> adjaRowGrouped = adjaRow.groupBy(new AdjacencyRowGroup());	
		ReduceOperator<AdjacencyRow> adjaRowReduced = adjaRowGrouped.reduce(new AdjacencyRowReduce());
		MapOperator<AdjacencyRow, String> idsStringified = adjaRowReduced.map(new AdjacencyRowMapString());
		idsStringified.writeAsText(outPath + "/adjacency", WriteMode.OVERWRITE).setParallelism(1);
	}
}
