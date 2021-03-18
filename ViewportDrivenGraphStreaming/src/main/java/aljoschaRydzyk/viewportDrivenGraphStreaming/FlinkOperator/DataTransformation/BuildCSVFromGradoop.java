package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import java.io.IOException;
import java.util.ArrayList;
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
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexNeighborhoodSampling;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;
import org.gradoop.flink.util.GradoopFlinkConfig;

import aljoschaRydzyk.viewportDrivenGraphStreaming.LdbcImporter;

/*
 * To be executed using local flink cluster on port 8081
 * execute with the following arguments:
			sourcePath of gradoop graph data
			input format
			writePath of result graph data
			gradoop graphID
			one of 'gradoop' or 'gvd' to determine result graph format
			jar files necessary for flink job execution (ExecutionEnvironment.createRemoteEnvironment())
			cluster entry point address (default: 'loclahost')
			optional:
				one of 'sample', 'degree', 'layout' or a combination (up to 8 arguments then)
				
	For GradoopToGradoop:
	Graph has to be copied manually into 'writePath' directory, there 'vertices.csv' directory 
	has to be deleted and 'metadata.csv' has to be edited manually according to the new vertex 
	properties given!!!
	
	Sampling: 
	Input has to be modified gradoop data (including vertex degrees, layout coordinates and layout level)
 * 
 */

public class BuildCSVFromGradoop {
	
	private static int clusterEntryPointPort = 8081;
	private static String clusterEntryPointAddress = "localhost";
	private static int zoomLevelCoefficient = 250;
	
	
	public static void main(String[] args) {
		
		Options options = new Options();
		Option inputFormatOption = new Option("if", "inputFormat", true, "gradoop or generator");
		options.addOption(inputFormatOption);
		Option sourcePathOption = new Option("i", "input", true, "path to data source folder");
		options.addOption(sourcePathOption);
		Option sinkPathOption = new Option("o", "output", true, "path to data sink folder");
		options.addOption(sinkPathOption);
		Option gradoopGraphIdOption = new Option("id", "gradoopGraphId", true, "gradoop graph ID");
		options.addOption(gradoopGraphIdOption);
		Option outputFormatOption = new Option("of", "outputFormat", true, "gradoop or gvd");
		options.addOption(outputFormatOption);
		Option flinkJarOption = new Option("j", "jar", true, "path to flink job jar");
		options.addOption(flinkJarOption);
		Option clusterEntryPointAddressOption = new Option("c", "clusterEntryPoint", true, "clustern entry point address");
		options.addOption(clusterEntryPointAddressOption);
		
		CommandLineParser parser = new DefaultParser();
		CommandLine cmdLine = null;
		
		try {
			cmdLine = parser.parse(options, args);
		} catch (ParseException e2) {
			e2.printStackTrace();
		}
		
		String sourcePath = cmdLine.getOptionValue("i");
		String writePath = cmdLine.getOptionValue("o");
		String gradoopGraphId = null;
		if (cmdLine.hasOption("id")) gradoopGraphId = cmdLine.getOptionValue("id");
		String outputFormatType = cmdLine.getOptionValue("of");
		String inputFormatType = cmdLine.getOptionValue("if");
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
		if (inputFormatType.equals("gradoop")) {
			DataSource source = new CSVDataSource(sourcePath, gra_flink_cfg);
			GradoopId id = GradoopId.fromString(gradoopGraphId);
			try {
				log = source.getGraphCollection().getGraph(id);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		} else if (inputFormatType.equals("generator")){
			LdbcImporter ldbcImporter = new LdbcImporter(sourcePath, env);
			ldbcImporter.parseToLogicalGraph();
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
		
		//layout graph
//		if (operations.contains("layout")) {
//			int numberVertices = Integer.parseInt(String.valueOf(log.getVertices().count()));
//			log = new FRLayouter(1000, numberVertices).execute(log);
//		}
		
		//random layouter
		if (remainingArgs.contains("layout")) {
			log = log.transformVertices(new RandomLayouter());
		}

		//sink to gradoop format or GVD format
		if (outputFormatType.equals("gradoop")) {
			if (remainingArgs.contains("sample")) {
				System.out.println("sampled!");
				DataSink csvDataSink = new CSVDataSink(writePath, gra_flink_cfg);
				try {
					csvDataSink.write(log, true);
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				GradoopToGradoop gradoopToGradoop = new GradoopToGradoop(remainingArgs, zoomLevelCoefficient);
				try {
					gradoopToGradoop.transform(log, sourcePath, writePath, gradoopGraphId, env);
				} catch (Exception e) {
					e.printStackTrace();
				}
				gradoopToGradoop.editMetadata(sourcePath, writePath, env);
			}	
		} else if (outputFormatType.equals("gvd")) {
			if (!remainingArgs.contains("degree")) 
				System.out.println("Warning: GVD Format assumes that vertex degrees are already calculated!");
			if (!remainingArgs.contains("layout"))
				System.out.println("Warning: GVD Format requires layout coordinates for all vertices!");
			GradoopToGVD gradoopToCSV = new GradoopToGVD(zoomLevelCoefficient);
			try {
				gradoopToCSV.parseGradoopToCSV(log, writePath, gradoopGraphId);
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
