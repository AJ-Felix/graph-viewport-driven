package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.PrintWriter;

public class Evaluator {
	private StreamExecutionEnvironment
	
	public Evaluator() {
	}
	
	public void setParameters(StreamExecutionEnvironment fsEnv) {
		
	}
	
	public void executeStreamEvaluation() {
		Long beforeJobCall = System.currentTimeMillis();
		flinkCore.getFsEnv().execute("buildTopView");
		Long afterJobCall = System.currentTimeMillis();
		Long callToResultDuration = beforeJobCall - afterJobCall;
		PrintWriter writer = new PrintWriter("/home/aljoscha/performance_evluation.log");
		writer.println("Operation: BuildTopView, call-to-result duration: " + callToResultDuration);
		writer.close();
	}
}
