package aljoschaRydzyk.viewportDrivenGraphStreaming.DataTransformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.CentroidFRLayouter;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexNeighborhoodSampling;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;
import org.gradoop.flink.util.GradoopFlinkConfig;

/*
 * To be executed using local flink cluster on port 8081
 * execute with at least 5 arguments in this order:
			1	sourcePath of gradoop graph data
			2	writePath of result graph data
			3 gradoop graphID
			4 one of 'gradoop' or 'gvd' to determine result graph format
			5 jar files necessary for flink job execution (ExecutionEnvironment.createRemoteEnvironment())
			optional:
				one of 'sample', 'degree', 'layout' or a combination (up to 8 arguments then)
 * 
 */

public class BuildCSVFromGradoop {
	
	private static int clusterEntryPointPort = 8081;
	private static String clusterEntryPointAddress = "localhost";
	
	public static void main(String[] args) throws Exception {
		
		
		String sourcePath = args[0];
		String writePath = args[1];
		String gradoopGraphId = args[2];
		String formatType = args[3];
		String flinkJobJarPath = args[4];
		List<String> operations = new ArrayList<String>();
		if (args.length >= 6) operations.add(args[5]);
		if (args.length >= 7) operations.add(args[6]);
		if (args.length == 8) operations.add(args[7]);
		
		
		
		//create gradoop Flink configuration
		ExecutionEnvironment env = ExecutionEnvironment
				.createRemoteEnvironment(clusterEntryPointAddress, clusterEntryPointPort, flinkJobJarPath);
		env.setParallelism(4);
		GradoopFlinkConfig gra_flink_cfg = GradoopFlinkConfig.createConfig(env);
		
		//create Flink Stream Configuration
		StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		fsEnv.setParallelism(4);
		
		//load graph
		DataSource source = new CSVDataSource(sourcePath, gra_flink_cfg);
		GradoopId id = GradoopId.fromString(gradoopGraphId);
		LogicalGraph log = source.getGraphCollection().getGraph(id);
		
		//sample graph
		if (operations.contains("sample")) log = new RandomVertexNeighborhoodSampling((float) 0.001, 3, 
				Neighborhood.BOTH).sample(log);
		
		//calculate degrees
		if (operations.contains("degree")) {
			String propertyKeyDegree = "degree";
			String propertyKeyInDegree = "inDegree";
			String propertyKeyOutDegree = "outDegree";
			boolean includeZeroDegreeVertices = true;
			log = log.callForGraph(new DistinctVertexDegrees(propertyKeyDegree, propertyKeyInDegree, 
					propertyKeyOutDegree, includeZeroDegreeVertices));
		}

		//layout graph
//		if (operations.contains("layout")) log = new CentroidFRLayouter(5, 2000).execute(log);
		if (operations.contains("layout")) log = new CentroidFRLayouter(20, 1000).execute(log);


		//sink to gradoop format or GVD format
		if (formatType.equals("gradoop")) {
			DataSink csvDataSink = new CSVDataSink(writePath, gra_flink_cfg);
			csvDataSink.write(log, true);
		} else if (formatType.equals("gvd")) {
			GradoopToCSV.parseGradoopToCSV(log, writePath, gradoopGraphId);
		}
		env.execute();
	}
}
