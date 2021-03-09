package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
		try {
			this.listen();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
    
    public void listen() throws IOException {
    	System.out.println("Executing FlinkResponseHandler.listen()!");
		echoSocket = serverSocket.accept();
        System.out.println("Connected to Socket!");
        in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
        line = "empty";
        if (!(operation.startsWith("initial"))) {
        	if (layout) {
        		while((line = in.readLine()) != null)  {
	            	wrapperHandler.addWrapper(parseWrapperString(line));
	            }
        		synchronized (Server.serverSyn) {
        			Server.serverSyn.notify();
        		}
        	} else {
        		while((line = in.readLine()) != null)  {
	            	wrapperHandler.addWrapperLayout(parseWrapperStringNoCoordinates(line));
	            }
        		synchronized (Server.serverSyn) {
        			Server.serverSyn.notify();
        		}
        	}	
        } else {
        	if (layout) {
        		while((line = in.readLine()) != null)  {
	            	wrapperHandler.addWrapperInitial(parseWrapperString(line));
	            }
        		synchronized (Server.serverSyn) {
        			Server.serverSyn.notify();
        		}
        	} else  {

        		while((line = in.readLine()) != null)  {
	            	wrapperHandler.addWrapperInitial(parseWrapperStringNoCoordinates(line));
	            }
        		synchronized (Server.serverSyn) {
        			Server.serverSyn.notify();
        		}
        	}	
        }
    	in.close();
	    echoSocket.close();   
	    this.listen();
    }

	private WrapperGVD parseWrapperStringNoCoordinates(String line) {
		String[] array = line.split(",");
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
}
