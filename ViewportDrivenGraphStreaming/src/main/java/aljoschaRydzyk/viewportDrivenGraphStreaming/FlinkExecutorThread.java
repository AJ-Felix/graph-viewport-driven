package aljoschaRydzyk.viewportDrivenGraphStreaming;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import aljoschaRydzyk.viewportDrivenGraphStreaming.Eval.Evaluator;

public class FlinkExecutorThread extends Thread{
	private Thread t;
	private String operation;
	private StreamExecutionEnvironment fsEnv;
	private boolean eval;
	private String evalSpec;
	
	public FlinkExecutorThread(String operation, StreamExecutionEnvironment fsEnv, boolean eval, String fileSpec) {
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
				Server.sendToAll("test: " + Thread.currentThread().getId());
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
