package aljoschaRydzyk.viewportDrivenGraphStreaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkExecutor extends Thread{
	private Thread t;
	private String operation;
	private StreamExecutionEnvironment fsEnv;
	private boolean eval;
	private String evalSpec;
	
	public FlinkExecutor(String operation, StreamExecutionEnvironment fsEnv, boolean eval, String fileSpec) {
		this.operation = operation;
		this.fsEnv = fsEnv;
		this.eval = eval;
		this.evalSpec = fileSpec;
	}
		
	@Override
	public void start() {
		if (t == null) {
			t = new Thread(this, operation);
			t.start();
		}
	}
	
	@Override
	public void run() {
		System.out.println("Flink Streaming Executor Thread with operation '" + operation + "' is running!");
		if (eval) {
			synchronized (Server.writeSyn) {
				new Evaluator(this.fsEnv, evalSpec).executeStream(operation);
			}
		} else {
			try {
				fsEnv.execute();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
	}
}
