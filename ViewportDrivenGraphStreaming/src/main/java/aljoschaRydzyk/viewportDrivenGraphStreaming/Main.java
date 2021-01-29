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
import org.apache.log4j.BasicConfigurator;


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
		
		//parse command line
		Options options = new Options();
		Option evalOption = new Option("e", "evaluation", false, "toggle performance evaluation");
		options.addOption(evalOption);
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e1) {
			e1.printStackTrace();
		}
		boolean eval = cmd.hasOption("e");
		
		//initialize Server
		server = new Server(eval);
		try {
			server.setPublicIp4Adress();
		} catch (SocketException e) {
			System.out.println("Could not set local machine's public Ip4 adress!");
			e.printStackTrace();
		}
		server.initializeServerFunctionality();
		server.initializeHandlers();
		System.out.println("exiting main thread");
    }
}
