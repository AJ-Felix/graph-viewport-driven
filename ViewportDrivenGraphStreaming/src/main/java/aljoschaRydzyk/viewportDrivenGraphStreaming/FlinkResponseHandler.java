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
	private boolean layout;
	private WrapperHandler wrapperHandler;
	private int port = 8898;
    private ServerSocket serverSocket = null;
    private String line;
    private Socket echoSocket;
    private PrintWriter out;
	private BufferedReader in;
	private Server server;
	
	
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
		try {
			this.listen();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    
	public FlinkResponseHandler(Server server, WrapperHandler wrapperHandler) {
		this.server = server;
		this.wrapperHandler = wrapperHandler;
		this.threadName = "flinkClusterListener";
        try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    
    public void listen() throws IOException {
    	System.out.println("executing listen on flinkResponseHandler");
		echoSocket = serverSocket.accept();
        out = new PrintWriter(echoSocket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
        System.out.println("connected to socket!!!");
        line = "empty";
        if (!(operation.startsWith("initial"))) {
        	if (layout) {
        		wrapperHandler.setSentToClientInSubStep(false);
        		while((line = in.readLine()) != null)  {
	            	System.out.println("flinkResponseHandler: " + line);
	            	wrapperHandler.addWrapper(parseWrapperString(line));
	            }
        		if (wrapperHandler.getSentToClientInSubStep() == false) server.sendToAll("enableMouse");
        		else wrapperHandler.clearOperation();
        	} else {
        		while((line = in.readLine()) != null)  {
	            	System.out.println("flinkResponseHandler: " + line);
	            	wrapperHandler.addWrapperLayout(parseWrapperStringNoCoordinates(line));
	            }
        	}	
        } else {
        	if (layout) {
        		while((line = in.readLine()) != null)  {
	            	System.out.println("flinkResponseHandler: " + line);
	            	wrapperHandler.addWrapperInitial(parseWrapperString(line));
	            }
                wrapperHandler.clearOperation();
        	} else  {
        		while((line = in.readLine()) != null)  {
	            	System.out.println("flinkResponseHandler: " + line);
	            	wrapperHandler.addWrapperInitial(parseWrapperStringNoCoordinates(line));
	            }
        	}	
        }
    	in.close();
	    out.close();
	    echoSocket.close();   
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
		System.out.println(array);
		VertexGVD sourceVertex = new VertexGVD(array[1], array[3], Integer.parseInt(array[2]), Long.parseLong(array[4]), Integer.parseInt(array[5]));
		VertexGVD targetVertex = new VertexGVD(array[6], array[8], Integer.parseInt(array[7]), Long.parseLong(array[9]), Integer.parseInt(array[10]));
		EdgeGVD edge = new EdgeGVD(array[11], array[12], array[1], array[6]);
		return new WrapperGVD(sourceVertex, targetVertex, edge);
	}
	
	private WrapperGVD parseWrapperString(String line) {
		String[] array = line.split(",");
		VertexGVD sourceVertex = new VertexGVD(array[1], array[3], 
				Integer.parseInt(array[2]), Integer.parseInt(array[4]), Integer.parseInt(array[5]), 
				Long.parseLong(array[6]), Integer.parseInt(array[7]));
		VertexGVD targetVertex = new VertexGVD(array[8], array[10], 
				Integer.parseInt(array[9]), Integer.parseInt(array[11]), Integer.parseInt(array[12]), 
				Long.parseLong(array[13]), Integer.parseInt(array[14]));
		EdgeGVD edge = new EdgeGVD(array[15], array[16], array[1], array[8]);
		return new WrapperGVD(sourceVertex, targetVertex, edge);
	}
	
	public void setOperation(String wrapperHandling) {
		this.operation = wrapperHandling;
	}
	
	public void setVerticesHaveCoordinates(Boolean have) {
		this.layout = have;
	}
	
	public String getLine() {
		return this.line;
	}
}
