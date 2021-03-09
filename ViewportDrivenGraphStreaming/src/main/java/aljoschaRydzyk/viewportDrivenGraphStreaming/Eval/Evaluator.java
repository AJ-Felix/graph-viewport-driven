package aljoschaRydzyk.viewportDrivenGraphStreaming.Eval;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

public class Evaluator {
	private StreamExecutionEnvironment fsEnv;
	private String fileName;

	public Evaluator(StreamExecutionEnvironment fsEnv, String fileName) {
		this.fsEnv = fsEnv;
		this.fileName = fileName;
	}
	
	public Evaluator(String fileSpec) {
		this.fileName = fileSpec;
	}
	
	private void writeToFile(String s) throws IOException{
		BufferedWriter bw;
		if (this.fileName.equals("default")) {
			bw = new BufferedWriter(new FileWriter("/home/aljoscha/server_evaluation.log", true)); 
		} else {
			bw = new BufferedWriter(new FileWriter("/home/aljoscha/server_evaluation_" + fileName + ".log", true));
		}
	    bw.write(s);
	    bw.close();
	}
	
	public void executeStream(String operation) {
		Long beforeJobCall = System.currentTimeMillis();
		String s;
		try {
			fsEnv.execute(operation);
			s = "Operation: " + operation;
		} catch (Exception e) {
			s = "Operation (cancelled): " + operation;
			System.out.println("Job was probably cancelled by server application.");	
		}
		Long afterJobCall = System.currentTimeMillis();
		Long callToResultDuration = afterJobCall - beforeJobCall;
		s += ", call-to-result duration: " + callToResultDuration;
		try {
			writeToFile(s);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public List<WrapperGVD> executeSet(String operation, DataSet<WrapperGVD> wrapperSet) throws Exception {
		Long beforeJobCall = System.currentTimeMillis();
		List<WrapperGVD> wrapperCollection = wrapperSet.collect();
		Long afterJobCall = System.currentTimeMillis();
		Long callToResultDuration = afterJobCall - beforeJobCall;
		String s = "Operation: " + operation + ", call-to-result duration: " + callToResultDuration;
		writeToFile(s);
		return wrapperCollection;
	}
}
