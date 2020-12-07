package aljoschaRydzyk.viewportDrivenGraphStreaming.DataTransformation;

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

public class BuildCSVFromGradoop {
	public static void main(String[] args) throws Exception {
		String sourcePath = args[0];
		String writePath = args[1];
		String gradoopGraphId = args[2];
		
		//create gradoop Flink configuration
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig gra_flink_cfg = GradoopFlinkConfig.createConfig(env);
		
		//create Flink Stream Configuration
		StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		
		//load graph
		DataSource source = new CSVDataSource(sourcePath, gra_flink_cfg);
		GradoopId id = GradoopId.fromString(gradoopGraphId);
		LogicalGraph log = source.getGraphCollection().getGraph(id);
		
		//sample graph
		log = new RandomVertexNeighborhoodSampling((float) 0.0001, 3, Neighborhood.BOTH).sample(log);
		
		//calculate degrees
		String propertyKeyDegree = "degree";
		String propertyKeyInDegree = "inDegree";
		String propertyKeyOutDegree = "outDegree";
		boolean includeZeroDegreeVertices = true;
		log = log.callForGraph(new DistinctVertexDegrees(propertyKeyDegree, propertyKeyInDegree, 
				propertyKeyOutDegree, includeZeroDegreeVertices));

		//layout graph
		log = new CentroidFRLayouter(5, 2000).execute(log);

		//sink to gradoop format or GVD format
		if (args[3].equals("gradoop")) {
			DataSink csvDataSink = new CSVDataSink(writePath, gra_flink_cfg);
			csvDataSink.write(log, true);
		} else if (args[3].equals("gvd")) {
			GradoopToCSV.parseGradoopToCSV(log, writePath);
			fsEnv.execute();
		}
		env.execute();
	}
}
