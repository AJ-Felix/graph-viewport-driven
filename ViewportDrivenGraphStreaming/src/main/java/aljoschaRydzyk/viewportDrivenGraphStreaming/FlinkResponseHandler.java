package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.EdgeGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

public class FlinkResponseHandler extends Thread{
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
	
	@Override
	public void start() {
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}
	
	@Override
	public void run() {
		System.out.println("thread running");
		this.listen();
	}
    
	public FlinkResponseHandler(WrapperHandler wrapperHandler) {
		this.wrapperHandler = wrapperHandler;
		this.threadName = "flinkClusterListener";
        try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    
    public void listen() {
	    try {
	    	System.out.println("executing listen on flinkResponseHandler");
			echoSocket = serverSocket.accept();
	        out = new PrintWriter(echoSocket.getOutputStream(), true);
	        in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
            System.out.println("connected to socket!!!");
            line = "empty";
            if (!(operation.startsWith("initial"))) {
            	if (verticesHaveCoordinates) {
            		while((line = in.readLine()) != null)  {
    	            	System.out.println("flinkResponseHandler: " + line);
    	            	wrapperHandler.addWrapper(parseWrapperString(line));
    	            }
	                wrapperHandler.clearOperation();
            	} else {
            		while((line = in.readLine()) != null)  {
    	            	System.out.println("flinkResponseHandler: " + line);
    	            	wrapperHandler.addWrapperLayout(parseWrapperStringNoCoordinates(line));
    	            }
            	}	
            } else {
            	if (verticesHaveCoordinates) {
	            	if (operation.contains("Append")) {
	            		while((line = in.readLine()) != null)  {
	    	            	System.out.println("flinkResponseHandler: " + line);
	    	            	wrapperHandler.addWrapperInitial(parseWrapperString(line));
	    	            }
	            	} else if (operation.contains("Retract")) {
	            		while((line = in.readLine()) != null)  {
	    	            	System.out.println("flinkResponseHandler: " + line);
	    	            	if (line.endsWith("true")) {
	    		            	wrapperHandler.addWrapperInitial(parseWrapperString(line));
	    	            	} else if (line.endsWith("false")) {
	    	            		wrapperHandler.removeWrapper(parseWrapperString(line));
	    	            	}
	    	            }
	            	}
	            	Server.getInstance().sendToAll("fit");
	                wrapperHandler.clearOperation();
            	} else  {
            		if (operation.contains("Append")) {
	            		while((line = in.readLine()) != null)  {
	    	            	System.out.println("flinkResponseHandler: " + line);
	    	            	wrapperHandler.addWrapperInitial(parseWrapperStringNoCoordinates(line));
	    	            }
	            	} else if (operation.contains("Retract")) {
	            		while((line = in.readLine()) != null)  {
	    	            	System.out.println("flinkResponseHandler: " + line);
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
	    this.listen();
    }
    
    public void closeAndReopen() {
		try {
			in.close();
			out.close();
			echoSocket.close();
			serverSocket.close();
			this.listen();
		} catch (IOException e) {
			e.printStackTrace();
		}  
    }
	
	private WrapperGVD parseWrapperStringNoCoordinates(String line) {
		String[] array = line.split(",");
		VertexGVD sourceVertex = new VertexGVD(array[1], array[3], Integer.parseInt(array[2]), Long.parseLong(array[4]));
		VertexGVD targetVertex = new VertexGVD(array[5], array[7], Integer.parseInt(array[6]), Long.parseLong(array[8]));
		EdgeGVD edge = new EdgeGVD(array[9], array[10], array[1], array[5]);
		return new WrapperGVD(sourceVertex, targetVertex, edge);
	}

	public void setOperation(String wrapperHandling) {
		this.operation = wrapperHandling;
	}
	
	public void setVerticesHaveCoordinates(Boolean have) {
		this.verticesHaveCoordinates = have;
	}
	
	private WrapperGVD parseWrapperString(String line) {
		String[] array = line.split(",");
		VertexGVD sourceVertex = new VertexGVD(array[1], array[3], 
				Integer.parseInt(array[2]), Integer.parseInt(array[4]), Integer.parseInt(array[5]), Long.parseLong(array[6]));
		VertexGVD targetVertex = new VertexGVD(array[7], array[9], 
				Integer.parseInt(array[8]), Integer.parseInt(array[10]), Integer.parseInt(array[11]), Long.parseLong(array[12]));
		EdgeGVD edge = new EdgeGVD(array[13], array[14], array[1], array[7]);
		return new WrapperGVD(sourceVertex, targetVertex, edge);
	}
	
	public String getLine() {
		return this.line;
	}
}
