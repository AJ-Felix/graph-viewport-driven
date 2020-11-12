package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

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
		this.threadName = "someThreadName";
        try {
			serverSocket = new ServerSocket(port);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
    public void listen() {
//    	FlinkResponseThread flinkResponseThread = new FlinkResponseThread();
//    	Thread t = new Thread(flinkResponseThread, "flinkResponseThread");
//    	t.start();
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
//	    finally {
//	        try {
//	        	System.out.println("closing serverSocket!");
//	            serverSocket.close();
//	        }
//	        catch (IOException e) {
//	            e.printStackTrace();
//	        }
//	    }
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
	
	private VVEdgeWrapper parseWrapperStringNoCoordinates(String line) {
		String[] array = line.split(",");
		VertexCustom sourceVertex = new VertexCustom(array[1], array[3], Integer.parseInt(array[2]), Long.parseLong(array[4]));
		VertexCustom targetVertex = new VertexCustom(array[5], array[7], Integer.parseInt(array[6]), Long.parseLong(array[8]));
		EdgeCustom edge = new EdgeCustom(array[9], array[10], array[1], array[5]);
		return new VVEdgeWrapper(sourceVertex, targetVertex, edge);
	}

	public void setOperation(String wrapperHandling) {
		this.operation = wrapperHandling;
	}
	
	public void setVerticesHaveCoordinates(Boolean have) {
		this.verticesHaveCoordinates = have;
	}
	
	private VVEdgeWrapper parseWrapperString(String line) {
		String[] array = line.split(",");
		VertexCustom sourceVertex = new VertexCustom(array[1], array[3], 
				Integer.parseInt(array[2]), Integer.parseInt(array[4]), Integer.parseInt(array[5]), Long.parseLong(array[6]));
		VertexCustom targetVertex = new VertexCustom(array[7], array[9], 
				Integer.parseInt(array[8]), Integer.parseInt(array[10]), Integer.parseInt(array[11]), Long.parseLong(array[12]));
		EdgeCustom edge = new EdgeCustom(array[13], array[14], array[1], array[7]);
		return new VVEdgeWrapper(sourceVertex, targetVertex, edge);
	}
	
	public String getLine() {
		return this.line;
	}
	
//	public void forwardToWrapperHandler(VVEdgeWrapper wrapper) {
//		if (wrapperHandling == "standard") handler.addWrapperInitial(wrapper);
//		else if (wrapperHandling == "layout") handler.addWrapperLayout(wrapper);
//		else if (wrapperHandling == "initial") handler.addWrapper(wrapper);
//	}
}
