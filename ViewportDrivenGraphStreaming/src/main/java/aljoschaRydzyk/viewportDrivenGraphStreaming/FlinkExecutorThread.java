package aljoschaRydzyk.viewportDrivenGraphStreaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkExecutorThread extends Thread{
	private Thread t;
	private String threadName;
	private StreamExecutionEnvironment fsEnv;
	
	public FlinkExecutorThread(String threadName, StreamExecutionEnvironment fsEnv) {
		this.threadName = threadName;
		this.fsEnv = fsEnv;
	}
		
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
			fsEnv.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("thread running");
	}

}
