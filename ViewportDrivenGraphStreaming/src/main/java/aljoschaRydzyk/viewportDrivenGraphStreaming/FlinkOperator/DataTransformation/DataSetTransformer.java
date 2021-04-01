package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
//import org.gradoop.flink.model.impl.operators.layouting.FRLayouter;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexNeighborhoodSampling;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;
import org.gradoop.flink.util.GradoopFlinkConfig;

/*
 * To be executed using local flink cluster on port 8081
 * execute with the following arguments:
			sourcePath of gradoop graph data
			writePath of result graph data
			gradoop graphID
			output format, i.e. 'gradoop' or 'vdrive'
			jar files necessary for flink job execution
			cluster entry point address 
			optional:
				one of 'sample', 'degree', 'layout' or a combination 
				'noAttributes' - this invokes the CSVGradoopSink instead, a Gradoop functionality that does not include layout level and numeric indices
				
	For GradoopSink:
	Graph has to be copied manually into 'writePath' directory, there 'vertices.csv' directory 
	has to be deleted and 'metadata.csv' has to be edited manually according to the new vertex 
	properties given!!!
	
	Sampling: 
	Input has to be gradoop data that include vertex degrees, layout coordinates and layout level to yield a visualizable graph
 * 
 */

public class DataSetTransformer {
	
	private static int clusterEntryPointPort = 8081;
	private static String clusterEntryPointAddress = "localhost";
	private static int zoomLevelCoefficient = 250;
	
	
	public static void main(String[] args) {
		
		//define option
		Options options = new Options();
		Option sourcePathOption = new Option("i", "input", true, "path to data source folder");
		options.addOption(sourcePathOption);
		Option sinkPathOption = new Option("o", "output", true, "path to data sink folder");
		options.addOption(sinkPathOption);
		Option gradoopGraphIdOption = new Option("id", "gradoopGraphId", true, "gradoop graph ID");
		options.addOption(gradoopGraphIdOption);
		Option outputFormatOption = new Option("of", "outputFormat", true, "gradoop or vdrive");
		options.addOption(outputFormatOption);
		Option flinkJarOption = new Option("j", "jar", true, "path to flink job jar");
		options.addOption(flinkJarOption);
		Option clusterEntryPointAddressOption = new Option("c", "clusterEntryPoint", true, "clustern entry point address");
		options.addOption(clusterEntryPointAddressOption);
		
		//parse command line
		CommandLineParser parser = new DefaultParser();
		CommandLine cmdLine = null;
		try {
			cmdLine = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String sourcePath = cmdLine.getOptionValue("i");
		String writePath = cmdLine.getOptionValue("o");
		String gradoopGraphId = null;
		if (cmdLine.hasOption("id")) gradoopGraphId = cmdLine.getOptionValue("id");
		String outputFormatType = cmdLine.getOptionValue("of");
		String flinkJobJarPath = cmdLine.getOptionValue("j");
		clusterEntryPointAddress = cmdLine.getOptionValue("c");
		List<String> remainingArgs = cmdLine.getArgList();
		
		//create gradoop Flink configuration
		ExecutionEnvironment env = ExecutionEnvironment
				.createRemoteEnvironment(clusterEntryPointAddress, clusterEntryPointPort, flinkJobJarPath);
		env.setParallelism(24);
		GradoopFlinkConfig gra_flink_cfg = GradoopFlinkConfig.createConfig(env);
		
		//load graph
		LogicalGraph log = null;
		DataSource source = new CSVDataSource(sourcePath, gra_flink_cfg);
		GradoopId id = GradoopId.fromString(gradoopGraphId);
		try {
			log = source.getGraphCollection().getGraph(id);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		
		//sample graph
		if (remainingArgs.contains("sample")) log = new RandomVertexNeighborhoodSampling((float) 0.01, 3, 
				Neighborhood.BOTH).sample(log);
		
		//calculate degrees
		if (remainingArgs.contains("degree")) {
			String propertyKeyDegree = "degree";
			String propertyKeyInDegree = "inDegree";
			String propertyKeyOutDegree = "outDegree";
			boolean includeZeroDegreeVertices = true;
			log = log.callForGraph(new DistinctVertexDegrees(propertyKeyDegree, propertyKeyInDegree, 
					propertyKeyOutDegree, includeZeroDegreeVertices));
		}
		
		//FRlayouter, only applicable for small enough graphs
//		if (remainingArgs.contains("layout")) {
//			int numberVertices = 0;
//			try {
//				numberVertices = Integer.parseInt(String.valueOf(log.getVertices().count()));
//			} catch (NumberFormatException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//			log = new FRLayouter(1000, numberVertices).execute(log);
//		}
		
		//random layouter
		if (remainingArgs.contains("layout")) {
			log = log.transformVertices(new RandomLayouter());
		}

		//sink to Gradoop format or VDrive format
		if (outputFormatType.equals("gradoop")) {
			if (remainingArgs.contains("noAttributes")) {
				DataSink csvDataSink = new CSVDataSink(writePath, gra_flink_cfg);
				try {
					csvDataSink.write(log, true);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				GradoopSink gradoopToGradoop = new GradoopSink(remainingArgs, zoomLevelCoefficient);
				try {
					gradoopToGradoop.addAttributes(log, sourcePath, writePath, gradoopGraphId, env);
				} catch (Exception e) {
					e.printStackTrace();
				}
				gradoopToGradoop.editMetadata(sourcePath, writePath, env);
			}	
		} else if (outputFormatType.equals("vdrive")) {
			if (!remainingArgs.contains("degree")) 
				System.out.println("Warning: VDrive Format assumes that vertex degrees are already calculated!");
			if (!remainingArgs.contains("layout"))
				System.out.println("Warning: VDrive Format requires layout coordinates for all vertices!");
			VDriveSink vDriveSink = new VDriveSink(zoomLevelCoefficient);
			try {
				vDriveSink.parseGradoopToVDrive(log, writePath, gradoopGraphId);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		try {
			env.execute("Parse Graph");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
