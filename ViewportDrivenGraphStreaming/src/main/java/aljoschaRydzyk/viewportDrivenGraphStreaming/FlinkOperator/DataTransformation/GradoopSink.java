package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import java.util.List;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly.VertexEPGMMapTupleDegreeComplex;

public class GradoopSink {
	private int zoomLevelCoefficient;
	private List<String> operations;
	
	public GradoopSink(List<String> operations, int zoomLevelCoefficient) {
		this.zoomLevelCoefficient = zoomLevelCoefficient;
		this.operations = operations;
	}
	
	public void addAttributes(LogicalGraph log, String sourcePath, String writePath, String gradoopGraphId, 
			ExecutionEnvironment env) 
			throws NumberFormatException, Exception {
		DataSet<String> metadata = env.readTextFile(sourcePath + "/metadata.csv");
		int numberVertices = Integer.parseInt(String.valueOf(log.getVertices().count()));
		int numberZoomLevels = (numberVertices + zoomLevelCoefficient - 1) / zoomLevelCoefficient;
		int zoomLevelSetSize = (numberVertices + numberZoomLevels - 1) / numberZoomLevels;
		DataSet<Tuple2<Long, Tuple2<EPGMVertex, Long>>> vertices = 
				DataSetUtils.zipWithIndex((log.getVertices()
				.map(new VertexEPGMMapTupleDegreeComplex())
				.sortPartition(1, Order.DESCENDING)
				.setParallelism(1)
			));
		vertices = vertices.rebalance();
		DataSet<Tuple2<String, Tuple2<Long, Tuple2<EPGMVertex, Long>>>> joined = 
				metadata.join(vertices).where(new MetadataKeyselectorVertexLabel())
			.equalTo(new VertexKeyselectorVertexLabel());
		DataSet<String> verticesString = 
				joined.map(new VertexTupleComplexMapString(operations, gradoopGraphId, zoomLevelSetSize));
		verticesString.rebalance();
		verticesString.writeAsText(writePath + "/vertices.csv");
		
	}	
	
	public void editMetadata(String sourcePath, String writePath, ExecutionEnvironment env) {
		DataSet<String> metadata = env.readTextFile(sourcePath + "/metadata.csv");
		metadata = metadata.map(new MetadataMapNewProperties(this.operations));
		metadata.writeAsText(writePath + "/metadata.csv", WriteMode.OVERWRITE).setParallelism(1);
	}
}
