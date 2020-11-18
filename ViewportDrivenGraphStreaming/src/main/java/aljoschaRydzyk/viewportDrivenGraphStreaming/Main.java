package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.net.SocketException;

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
		
		//parse main input arguments
		String clusterEntryPointIP4 = args[0];
		System.out.println(clusterEntryPointIP4);
		
		//initialize Server
		server = Server.getInstance();
		try {
			server.setPublicIp4Adresses(clusterEntryPointIP4);
		} catch (SocketException e) {
			System.out.println("Could not set local machine's public Ip4 adress!");
			e.printStackTrace();
		}
		server.initializeServerFunctionality();
		server.initializeHandlers();
		System.out.println("exiting main thread");
    }
}
