package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class FlinkResponseThread extends Thread{
	private Thread t;
	private String threadName;
	
	private String operation;
	private boolean verticesHaveCoordinates;
	private WrapperHandler wrapperHandler;
	private int port = 8898;
    private ServerSocket serverSocket = null;
    private String line;
    private Socket echoSocket;
    private PrintWriter out;
	private BufferedReader in;
	   
	public FlinkResponseThread(String name) {
		threadName = name;
	    System.out.println("Creating " +  threadName );
	}
	
	@Override
	public void start() {
		System.out.println("Starting " +  threadName );
	    if (t == null) {
	         t = new Thread (this, threadName);
	         t.start ();
	    }
	}
	
	@Override
	public void run() {
		 try {
		    	System.out.println("executing listen on flinkResponseHandler");
		        serverSocket = new ServerSocket(port);
		        echoSocket = serverSocket.accept();
		        out = new PrintWriter(echoSocket.getOutputStream(), true);
		        in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
	            System.out.println("connected to socket!!!");
	            line = "empty";
	            if (!(operation.startsWith("initial"))) {
	            	if (verticesHaveCoordinates) {
	            		while((line = in.readLine()) != null)  {
	    	            	System.out.println(line);
	    	            	wrapperHandler.addWrapper(parseWrapperString(line));
	    	            }
	            	} else {
	            		while((line = in.readLine()) != null)  {
	    	            	System.out.println(line);
	    	            	wrapperHandler.addWrapperLayout(parseWrapperStringNoCoordinates(line));
	    	            }
	            	}	
	            } else {
	            	if (verticesHaveCoordinates) {
		            	if (operation.contains("Append")) {
		            		while((line = in.readLine()) != null)  {
		    	            	System.out.println(line);
		    	            	wrapperHandler.addWrapperInitial(parseWrapperString(line));
		    	            }
		            	} else if (operation.contains("Retract")) {
		            		while((line = in.readLine()) != null)  {
		    	            	System.out.println(line);
		    	            	if (line.endsWith("true")) {
		    		            	wrapperHandler.addWrapperInitial(parseWrapperString(line));
		    	            	} else if (line.endsWith("false")) {
		    	            		wrapperHandler.removeWrapper(parseWrapperString(line));
		    	            	}
		    	            }
		            	}
	            	} else  {
	            		if (operation.contains("Append")) {
		            		while((line = in.readLine()) != null)  {
		    	            	System.out.println(line);
		    	            	wrapperHandler.addWrapperInitial(parseWrapperStringNoCoordinates(line));
		    	            }
		            	} else if (operation.contains("Retract")) {
		            		while((line = in.readLine()) != null)  {
		    	            	System.out.println(line);
		    	            	if (line.endsWith("true")) {
		    		            	wrapperHandler.addWrapperInitial(parseWrapperStringNoCoordinates(line));
		    	            	} else if (line.endsWith("false")) {
		    	            		wrapperHandler.removeWrapper(parseWrapperStringNoCoordinates(line));
		    	            	}
		    	            }
		            	}
	            	}
	            }
	            in.close();
	    	    out.close();
	    	    echoSocket.close();   
		    }
		    catch (IOException e) {
		        e.printStackTrace();
		    }
		    finally {
		        try {
		        	System.out.println("closing serverSocket!");
		            serverSocket.close();
		        }
		        catch (IOException e) {
		            e.printStackTrace();
		        }
		    }
		    this.listen();
	    }
	}

}
