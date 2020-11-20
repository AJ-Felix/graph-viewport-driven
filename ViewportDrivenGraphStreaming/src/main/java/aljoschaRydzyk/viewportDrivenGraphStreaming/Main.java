package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.net.SocketException;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.BasicConfigurator;

import com.sun.tools.javac.util.List;

public class Main {
	
    private static Server server;
	
    public static void main(final String[] args) {
    	
//    	BasicConfigurator.configure();
    	PrintStream fileOut = null;
		try {
			fileOut = new PrintStream("/home/aljoscha/out.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		System.setOut(fileOut);
		
		//initialize Server
		server = Server.getInstance();
		try {
			server.setPublicIp4Adress();
		} catch (SocketException e) {
			System.out.println("Could not set local machine's public Ip4 adress!");
			e.printStackTrace();
		}
		server.initializeServerFunctionality();
		server.initializeHandlers();
		
		//parse main input arguments
		parseCommandLineArguments(args);
		
		System.out.println("exiting main thread");
    }
    
    private static void parseCommandLineArguments(String[] args) {
    	Options options = new Options();
    	
        Option clusterEntryPointIp4Opt = new Option("s", "clusterEntryPointIp4", true, 
        		"Public Ip4 adress of cluster entry point.");
        clusterEntryPointIp4Opt.setRequired(false);
        options.addOption(clusterEntryPointIp4Opt);

        Option gradoopCSVPathOpt = new Option("pg", "pathGradoop", true, 
        		"Full Path to gradoop csv graph folder.");
        gradoopCSVPathOpt.setRequired(false);
        options.addOption(gradoopCSVPathOpt);
        
        Option gradoopGraphIdOpt = new Option("id", "gradoopGraphId", true, "Gradoop Graph ID.");
        gradoopGraphIdOpt.setRequired(false);
        options.addOption(gradoopGraphIdOpt);
        
        Option viDGraSCSVPathOpt= new Option("pv", "pathViDGraS", true, 
        		"Full Path to ViDGraS csv graph folder");
        viDGraSCSVPathOpt.setRequired(false);
        options.addOption(viDGraSCSVPathOpt);
        
        Option vertexDegreesOpt = new Option("vd", "vertexDegrees", false, 
        		"Select if vertices carry degree attributes.");
        vertexDegreesOpt.setRequired(false);
        options.addOption(vertexDegreesOpt);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        
        String clusterEntryPointIp4 = "localhost";
        String gradoopCSVPath = "/home/aljoscha/graph-samples/one10thousand_sample_2_third_degrees_layout";
        String gradoopGraphId = "5ebe6813a7986cc7bd77f9c2";
        String viDGraSCSVPath = "/home/aljoscha/graph-viewport-driven/csvGraphs/adjacency/one10thousand_sample_2_third_degrees_layout";
        String vertexDegrees = "false";
        
        try {
            cmd = parser.parse(options, args);
            if (!cmd.hasOption("pv") && !cmd.hasOption("pg")) {
            	throw new ParseException("Please provide path to graph sources!");
            }
            if (cmd.hasOption("pg") && !cmd.hasOption("id")) {
            	throw new ParseException("Please provide gradoop graph ID!");
            }
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("ViDGraS", options);
            System.exit(1);
        }
        
        if (cmd.hasOption("s")) clusterEntryPointIp4 = cmd.getOptionValue("s");
        if (cmd.hasOption("pg")) gradoopCSVPath = cmd.getOptionValue("pg");
        if (cmd.hasOption("id")) gradoopGraphId = cmd.getOptionValue("id");
        if (cmd.hasOption("pv")) viDGraSCSVPath = cmd.getOptionValue("pv");
        if (cmd.hasOption("vd")) vertexDegrees = "true";
        ArrayList<String> flinkCoreParameters = new ArrayList<String>(List.of(clusterEntryPointIp4, gradoopCSVPath, gradoopGraphId, 
        		viDGraSCSVPath, vertexDegrees));
        server.setParameters(flinkCoreParameters);
    }
}
