package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.net.SocketException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;


public class Main {
	
    private static Server server;
	
    public static void main(final String[] args) {
    	
    	PrintStream fileOut = null;
		try {
			fileOut = new PrintStream("/home/aljoscha/out.txt");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		System.setOut(fileOut);
		
		//parse command line
		Options options = new Options();
		Option evalOption = new Option("e", "evaluation", true, "toggle performance evaluation");
		options.addOption(evalOption);
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		
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
		if (cmd.hasOption("e"))	{
			boolean automated;
			if (cmd.getOptionValue("e").equals("automated")) automated = true;
			else automated = false;
			server.setEvaluation(automated);
		}
    }
}
