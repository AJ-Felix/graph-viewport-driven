package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperVDrive;

public class Evaluator {
	private StreamExecutionEnvironment fsEnv;
	private String fileSpec;
	private static Long afterJobCall;

	public Evaluator(StreamExecutionEnvironment fsEnv, String fileName) {
		this.fsEnv = fsEnv;
		this.fileSpec = fileName;
	}
	
	public Evaluator(String fileSpec) {
		this.fileSpec = fileSpec;
	}
	
	private void writeToFile(String s) throws IOException{
		BufferedWriter bw;
		if (this.fileSpec.equals("default")) {
			bw = new BufferedWriter(new FileWriter("/home/aljoscha/server_evaluation.log", true)); 
		} else {
			bw = new BufferedWriter(new FileWriter("/home/aljoscha/server_evaluation_" + fileSpec + ".log", true));
		}
	    bw.write(s);
	    bw.close();
	}
	
	public void executeStream(String operation) {
		Long beforeJobCall = System.currentTimeMillis();
		String s;
		try {
			
			fsEnv.execute(operation);
			s = "notAvailable," + operation;
		} catch (Exception e) {
			s = "Operation (cancelled)," + operation;
			System.out.println("Job was probably cancelled by server application.");	
		}
		Long afterJobCallValue = System.currentTimeMillis();
		if (afterJobCall == null) setAfterJobCall(afterJobCallValue);
		else if (afterJobCallValue < afterJobCall) setAfterJobCall(afterJobCallValue);
		Long callToResultDuration = afterJobCall - beforeJobCall;
		s += "," + callToResultDuration;
		try {
			writeToFile(s);
		} catch (IOException e) {
			e.printStackTrace();
		}
		setAfterJobCall(null);
	}
	
	public List<WrapperVDrive> executeSet(String operation, DataSet<WrapperVDrive> wrapperSet) throws Exception {
		Long beforeJobCall = System.currentTimeMillis();
		List<WrapperVDrive> wrapperCollection = wrapperSet.collect();
		Long afterJobCall = System.currentTimeMillis();
		Long callToResultDuration = afterJobCall - beforeJobCall;
		String s = operation + "," + callToResultDuration;
		writeToFile(s);
		return wrapperCollection;
	}
	
	public synchronized static void setAfterJobCall(Long afterJobCallValue) {
		afterJobCall = afterJobCallValue;
	}
}
