package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator.DefaultJoin;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.VertexEPGMMapTupleDegreeComplex;

public class GradoopToGradoop {
	private int zoomLevelCoefficient;
	
	public GradoopToGradoop(int zoomLevelCoefficient) {
		this.zoomLevelCoefficient = zoomLevelCoefficient;
	}
	
	public void transform(LogicalGraph log, String writePath, String gradoopGraphId, 
			ExecutionEnvironment env) 
			throws NumberFormatException, Exception {
		DataSet<String> metadata = env.readTextFile(writePath + "/metadata.csv");
		
		
		int numberVertices = Integer.parseInt(String.valueOf(log.getVertices().count()));
		int numberZoomLevels = (numberVertices + zoomLevelCoefficient - 1) / zoomLevelCoefficient;
		int zoomLevelSetSize = (numberVertices + numberZoomLevels - 1) / numberZoomLevels;
		DataSet<Tuple2<Long, Tuple2<EPGMVertex, Long>>> vertices = 
				DataSetUtils.zipWithIndex((log.getVertices()
				.map(new VertexEPGMMapTupleDegreeComplex())
				.sortPartition(1, Order.DESCENDING)
				.setParallelism(1)
			));
		DataSet<Tuple2<String, Tuple2<Long, Tuple2<EPGMVertex, Long>>>> joined = 
				metadata.join(vertices).where(new MetadataKeyselectorVertexLabel())
			.equalTo(new VertexKeyselectorVertexLabel());
		DataSet<String> verticesString = 
				joined.map(new VertexTupleComplexMapString(gradoopGraphId, zoomLevelSetSize))
				.setParallelism(1);
		verticesString.writeAsText(writePath + "/vertices.csv");
		
		metadata = metadata.map(new MetadataMapNewProperties());
		metadata.writeAsText(writePath + "/metadata.csv", WriteMode.OVERWRITE).setParallelism(1);
	}	
	
//	public void editMetadata(String writePath, ExecutionEnvironment env) {
//		DataSet<String> metadata = env.readTextFile(writePath + "/metadata.csv");
//		
////		File file = new File(writePath + "/metadata.csv");
////		System.out.println(new File(".").getAbsolutePath());
////		System.out.println(file.exists());
////		System.out.println(file.canRead());
////		System.out.println(file.isDirectory());
////		FileReader reader = new FileReader(file);
////		BufferedReader buffReader = new BufferedReader(reader);
////		List<String> list = new ArrayList<String>();
////		String line;
////		while ((line = buffReader.readLine()) != null) {
////			if (line.startsWith("v")) {
////				line += ",numericId:long,zoomLevel:int"; 
////			}
////			list.add(line);
////		}
////		buffReader.close();
////		
////		FileWriter writer = new FileWriter(file);
////		BufferedWriter buffWriter = new BufferedWriter(writer);
////		Iterator<String> iter = list.iterator();
////		buffWriter.write( iter.next());
////		while (iter.hasNext()) {
////			String s = iter.next();
////			buffWriter.newLine();
////			buffWriter.write(s);
////		}
////		writer.close();
//	}
}
