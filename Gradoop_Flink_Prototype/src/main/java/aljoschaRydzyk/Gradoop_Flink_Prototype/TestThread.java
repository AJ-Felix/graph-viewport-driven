package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestThread implements Runnable{
	private String threadName;
	private Thread t;
	private StreamExecutionEnvironment fsEnv;
	
	public TestThread(String threadName, StreamExecutionEnvironment fsEnv) {
		this.threadName = threadName;
		this.fsEnv = fsEnv;
	}
	
	public void run() {
		try {
			for (int i = 0; i < 10; i++) {
				System.out.println("Thread " + threadName + " " + i);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		DataStream<String> datastream = fsEnv.socketTextStream("localhost", 9999);
		datastream.print();
		try {
			fsEnv.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
