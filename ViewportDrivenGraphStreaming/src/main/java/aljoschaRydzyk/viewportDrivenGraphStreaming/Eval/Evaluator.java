package aljoschaRydzyk.viewportDrivenGraphStreaming.Eval;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

public class Evaluator {
	private StreamExecutionEnvironment fsEnv;
	
	public Evaluator(StreamExecutionEnvironment fsEnv) {
		this.fsEnv = fsEnv;
	}
	
	public Evaluator() {
		
	}
	
	private void writeToFile(String s) throws IOException{
		FileWriter fw = new FileWriter("/home/aljoscha/performance_evaluation.log", true); //the true will append the new data
	    fw.write(s);
	    fw.close();
	}
	
	public void executeStream(String operation) throws Exception {
		Long beforeJobCall = System.currentTimeMillis();
		fsEnv.execute(operation);
		Long afterJobCall = System.currentTimeMillis();
		Long callToResultDuration = afterJobCall - beforeJobCall;
		String s = "Operation: " + operation + ", call-to-result duration: " + callToResultDuration + System.lineSeparator();
		writeToFile(s);
	}
	
	public List<WrapperGVD> executeSet(String operation, DataSet<WrapperGVD> wrapperSet) throws Exception {
		Long beforeJobCall = System.currentTimeMillis();
		List<WrapperGVD> wrapperCollection = wrapperSet.collect();
		Long afterJobCall = System.currentTimeMillis();
		Long callToResultDuration = afterJobCall - beforeJobCall;
		String s = "Operation: " + operation + ", call-to-result duration: " + callToResultDuration + System.lineSeparator();
		writeToFile(s);
		return wrapperCollection;
	}
}
