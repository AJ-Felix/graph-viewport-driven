package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.PrintWriter;
import java.util.List;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

public class Evaluator {
	private StreamExecutionEnvironment fsEnv;
	
	public Evaluator() {
	}
	
	public void setParameters(StreamExecutionEnvironment fsEnv) {
		this.fsEnv = fsEnv;
	}
	
	public void executeStreamEvaluation(String operation) throws Exception {
		Long beforeJobCall = System.currentTimeMillis();
		fsEnv.execute(operation);
		Long afterJobCall = System.currentTimeMillis();
		Long callToResultDuration = beforeJobCall - afterJobCall;
		PrintWriter writer = new PrintWriter("/home/aljoscha/performance_evluation.log");
		writer.println("Operation: " + operation + ", call-to-result duration: " + callToResultDuration);
		writer.close();
	}
	
	public List<WrapperGVD> executeSetEvaluation(String operation, DataSet<WrapperGVD> wrapperSet) throws Exception {
		Long beforeJobCall = System.currentTimeMillis();
		List<WrapperGVD> wrapperCollection = wrapperSet.collect();
		Long afterJobCall = System.currentTimeMillis();
		Long callToResultDuration = beforeJobCall - afterJobCall;
		PrintWriter writer = new PrintWriter("/home/aljoscha/performance_evluation.log");
		writer.println("Operation: " + operation + ", call-to-result duration: " + callToResultDuration);
		writer.close();
		return wrapperCollection;
	}
}
