package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Iterator;
import java.util.Queue;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;


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
		server = Server.getInstance();
		try {
			server.setPublicIp4Adress();
		} catch (SocketException e) {
			System.out.println("Could not set local machine's public Ip4 adress!");
			e.printStackTrace();
		}
		server.initializeServerFunctionality();
		server.initializeHandlers();
		if (eval) server.initializeEvaluator(eval);
		while(true) {
//			System.out.println(server.getMessageQueue().size());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    	server.sendToAll("test String4");
			Queue<WrapperGVD> queue;
//			synchronized (server) {	
//				 queue = server.getMessageQueue();
//				 server.sendToAll("Test String 5");
//				 System.out.println("Mains turn!");
//			}
//				if (!queue.isEmpty()) {
//					Iterator<WrapperGVD> iter = queue.iterator();
//					while (iter.hasNext()) {
//						System.out.println("Giving to wrapperHandler");
//						server.sendToAll("Test message to client!!!!!");
////						iter.remove();
//					}
//				} 
		}
		
		//new attempt
//		ServerSocket serverSocket = null;
//		try {
//			serverSocket = new ServerSocket(8897);
//			serverSocket.setReuseAddress(true);
//			while(true) {
//				Socket client = serverSocket.accept();
//				ClientHandler clientSock = new ClientHandler(client);
//				new Thread(clientSock).start();
//			}
//		} catch (IOException e) {
//			e.printStackTrace();
//		} finally {
//			if (serverSocket != null) {
//				try {
//					serverSocket.close();
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		
//		while (true)
//		
//		System.out.println("exiting main thread");
    }
}
