package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.flink.types.Row;

public class TestThread implements Runnable{
	private String threadName;
	private Thread t;
	private StreamExecutionEnvironment fsEnv;
	private FlinkCore flinkCore;
	
	public TestThread(String threadName
//			, StreamExecutionEnvironment fsEnv, FlinkCore flinkCore
			) {
		this.threadName = threadName;
		this.fsEnv = fsEnv;
		this.flinkCore = flinkCore;
	}
	
	public void run() {
		try {
			for (int i = 0; i < 10; i++) {
				System.out.println("Thread " + threadName + " " + i);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		flinkCore = new FlinkCore();
		DataStream<String> datastream = flinkCore.getFsEnv().socketTextStream("localhost", 9999);
		datastream.addSink(new SinkFunction<String>() {
			public void invoke(String element, Context context) {
				UndertowServer.sendToAll(element);
			}
		});
		flinkCore.initializeCSVGraphUtilJoin();
		Integer maxVertices = 100;
		DataStream<Row> wrapperStream = flinkCore.buildTopViewAppendJoin(maxVertices);
//		wrapperStream.print();
		wrapperStream.addSink(new SinkFunction<Row>(){
			public void invoke(Row element, Context context) {
				String sourceIdNumeric = element.getField(2).toString();
				String sourceX = element.getField(4).toString();
				String sourceY = element.getField(5).toString();
				String edgeIdGradoop = element.getField(13).toString();
				String targetIdNumeric = element.getField(8).toString();
				String targetX = element.getField(10).toString();
				String targetY = element.getField(11).toString();
				UndertowServer.sendToAll("addVertex;" + sourceIdNumeric + 
					";" + sourceX + ";" + sourceY);
				if (!edgeIdGradoop.equals("identityEdge")) {
				UndertowServer.sendToAll("addVertex;" + targetIdNumeric + 
					";" + targetX + ";" + targetY);
				UndertowServer.sendToAll("addEdge;" + edgeIdGradoop + 
					";" + sourceIdNumeric + ";" + targetIdNumeric);
				}
			}
		});
		try {
			flinkCore.getFsEnv().execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		UndertowServer.sendToAll("fitGraph");
		System.out.println("Thread " + threadName + " exiting!");
	} 
	
	public void start() {
		System.out.println("Starting Thread " + threadName);
		if (t == null) {
			t = new Thread(this, threadName);
			t.start();
		}
	}
}
