package aljoschaRydzyk.viewportDrivenGraphStreaming.Handler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import aljoschaRydzyk.viewportDrivenGraphStreaming.Evaluator;
import aljoschaRydzyk.viewportDrivenGraphStreaming.Server;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.EdgeVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperVDrive;

public class FlinkResponseHandler extends Thread{
	private Thread t;
	private String threadName;
	private String operation;
	private boolean layout;
	private WrapperHandler wrapperHandler;
	private Server server;
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
		this.server = Server.getInstance();
        try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    
    public void listen() throws IOException {
		echoSocket = serverSocket.accept();
        System.out.println("Connected to Socket!");
        in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
        line = "empty";
        if (!(operation.startsWith("initial"))) {
        	if (layout) {
        		while((line = in.readLine()) != null)  {
	            	wrapperHandler.addWrapper(parseWrapperString(line));
	            }
        		Evaluator.setAfterJobCall(System.currentTimeMillis());
        		server.onIsLayoutedJobTermination();;
        	} else {
        		while((line = in.readLine()) != null)  {
	            	wrapperHandler.addWrapperLayout(parseWrapperStringNoCoordinates(line));
	            }
        		Evaluator.setAfterJobCall(System.currentTimeMillis());
        		server.onLayoutingJobTermination();
        	}	
        } else {
        	if (layout) {
        		while((line = in.readLine()) != null)  {
	            	wrapperHandler.addWrapperInitial(parseWrapperString(line));
	            }
        		Evaluator.setAfterJobCall(System.currentTimeMillis());
        		server.onIsLayoutedJobTermination();
        	} else  {
        		while((line = in.readLine()) != null)  {
	            	wrapperHandler.addWrapperInitial(parseWrapperStringNoCoordinates(line));
	            }
        		Evaluator.setAfterJobCall(System.currentTimeMillis());
        		server.onLayoutingJobTermination();
        	}	
        }
    	in.close();
	    echoSocket.close();   
	    this.listen();
    }

	private WrapperVDrive parseWrapperStringNoCoordinates(String line) {
		String[] array = line.split(",");
		VertexVDrive sourceVertex = new VertexVDrive(array[1], array[3], Integer.parseInt(array[2]), Long.parseLong(array[4]), Integer.parseInt(array[5]));
		VertexVDrive targetVertex = new VertexVDrive(array[6], array[8], Integer.parseInt(array[7]), Long.parseLong(array[9]), Integer.parseInt(array[10]));
		EdgeVDrive edge = new EdgeVDrive(array[11], array[12], array[1], array[6]);
		return new WrapperVDrive(sourceVertex, targetVertex, edge);
	}
	
	private WrapperVDrive parseWrapperString(String line) {
		String[] array = line.split(",");
		VertexVDrive sourceVertex = new VertexVDrive(array[1], array[3], 
				Integer.parseInt(array[2]), Integer.parseInt(array[4]), Integer.parseInt(array[5]), 
				Long.parseLong(array[6]), Integer.parseInt(array[7]));
		VertexVDrive targetVertex = new VertexVDrive(array[8], array[10], 
				Integer.parseInt(array[9]), Integer.parseInt(array[11]), Integer.parseInt(array[12]), 
				Long.parseLong(array[13]), Integer.parseInt(array[14]));
		EdgeVDrive edge = new EdgeVDrive(array[15], array[16], array[1], array[8]);
		return new WrapperVDrive(sourceVertex, targetVertex, edge);
	}
	
	public void setOperation(String wrapperHandling) {
		this.operation = wrapperHandling;
	}
	
	public void setVerticesHaveCoordinates(Boolean have) {
		this.layout = have;
	}
}
