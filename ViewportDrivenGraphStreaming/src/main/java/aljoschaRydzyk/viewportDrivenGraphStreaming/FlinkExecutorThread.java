package aljoschaRydzyk.viewportDrivenGraphStreaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import aljoschaRydzyk.viewportDrivenGraphStreaming.Eval.Evaluator;

public class FlinkExecutorThread extends Thread{
	private Thread t;
	private String operation;
	private StreamExecutionEnvironment fsEnv;
	private boolean eval;
	
	public FlinkExecutorThread(String operation, StreamExecutionEnvironment fsEnv, boolean eval) {
		this.operation = operation;
		this.fsEnv = fsEnv;
		this.eval = eval;
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
		try {
			if (eval) {
				new Evaluator(this.fsEnv).executeStream(operation);
			} else fsEnv.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
