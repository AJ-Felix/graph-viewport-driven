package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

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
    
	public FlinkResponseHandler(WrapperHandler wrapperHandler) {
		this.server = Server.getInstance();
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
    	try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	this.server.sendToAll("test String");
		echoSocket = serverSocket.accept();
    	this.server.sendToAll("test String2");
        out = new PrintWriter(echoSocket.getOutputStream(), true);
        InputStreamReader inReader = new InputStreamReader(echoSocket.getInputStream());
//        while(true) {
//        }
        in = new BufferedReader(new InputStreamReader(echoSocket.getInputStream()));
//        byte[] messageByte = new byte[1000];
//        Scanner scanner = new Scanner(inReader);
//        while(scanner.hasNextLine()) System.out.print(scanner.nextLine());
//        scanner.close();
//        int i;
//        int count = 0;
//        while((i = in.read()) != -1) {
//        	count += 1;
//        	StringBuilder stringBuilder = new StringBuilder();
//        	stringBuilder.append((char) i);
//        	
//        	while(((char) (i = in.read())) != ('\n')) {
//        		stringBuilder.append((char) i);
//        	}
//        	System.out.println(stringBuilder.toString());
//        	System.out.print((char) i);
//        	System.out.println(in.readLine());
//        	System.out.print(in.read());
//        	System.out.println("in loop");
//        }
//        System.out.println("Count: " + count);
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
	            	System.out.println("flinkResponseHandler, initial, layout: " + line);
//	            	synchronized (server) {
//	            		server.addMessageToQueue(parseWrapperString(line));
//	            	}
        	    	this.server.sendToAll("test String3");
//	            	wrapperHandler.addWrapperInitial(parseWrapperString(line));
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
    
//    public void closeAndReopen() {
//		try {
//			in.close();
//			out.close();
//			echoSocket.close();
//			serverSocket.close();
//			this.listen();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}  
//    }
	
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
