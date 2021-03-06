package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.common.operators.Order;
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

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.EdgeSourceIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.EdgeTargetIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.VertexEPGMMapTupleDegreeComplex;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.VertexIDRowKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.VertexTupleComplexMapRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperTupleComplexMapRow;



public class GradoopToGVD {
	private int zoomLevelCoefficient;
	
	public GradoopToGVD(int zoomLevelCoefficient) {
		this.zoomLevelCoefficient = zoomLevelCoefficient;
	}
	
	
	public void parseGradoopToCSV(LogicalGraph graph, String outPath, String graphId) throws Exception {
		int numberVertices = Integer.parseInt(String.valueOf(graph.getVertices().count()));
		int numberZoomLevels = (numberVertices + zoomLevelCoefficient - 1) / zoomLevelCoefficient;
		int zoomLevelSetSize = (numberVertices + numberZoomLevels - 1) / numberZoomLevels;
		
		//vertices
		DataSet<Row> vertices = DataSetUtils.zipWithIndex((graph.getVertices()
				.map(new VertexEPGMMapTupleDegreeComplex())
				.sortPartition(1, Order.DESCENDING)
				.setParallelism(1)
			))
			.map(new VertexTupleComplexMapRow(graphId, zoomLevelSetSize));
		
		MapOperator<Row, Tuple8<String, String, Long, String, Integer, Integer, Long, Integer>> verticesTupled = 
				vertices.map(new VerticesMapTuple());
		
		verticesTupled.writeAsCsv(outPath + "/vertices", "\n", ";", WriteMode.OVERWRITE).setParallelism(1);
		
		//wrappers
		DataSet<EPGMEdge> edges = graph.getEdges();
		DataSet<Row> wrapper = 
				vertices.join(edges).where(new VertexIDRowKeySelector())
			.equalTo(new EdgeSourceIDKeySelector())
			.join(vertices).where(new EdgeTargetIDKeySelector())
			.equalTo(new VertexIDRowKeySelector())
			.map(new WrapperTupleComplexMapRow());
		
		MapOperator<Row, Tuple17<
			String, 
			String, Long, String, Integer, Integer, Long, Integer, 
			String, Long, String, Integer, Integer, Long, Integer, 
			String, String>> wrapperTupled = wrapper
				.map(new WrapperMapTuple());
		wrapperTupled.writeAsCsv(outPath + "/wrappers", "\n", ";", WriteMode.OVERWRITE).setParallelism(1);
		
		//adjacency
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
		 
//		UnsortedGrouping<Tuple3<String, String, String>> idsGrouped = idsUnited
//				.groupBy(new UnitedIDsGroup());
		
//		GroupReduceOperator<Tuple3<String, String, String>, Tuple2<String, List<Tuple2<String, String>>>> 
//		idsReduced = idsGrouped.reduceGroup(new IDsGroupedReduce());
		
//		MapOperator<Tuple2<String, List<Tuple2<String, String>>>, String> idsStringified = idsReduced
//				.map(new ReducedIDsMapString());	
		
		idsStringified.writeAsText(outPath + "/adjacency", WriteMode.OVERWRITE).setParallelism(1);
	}
}