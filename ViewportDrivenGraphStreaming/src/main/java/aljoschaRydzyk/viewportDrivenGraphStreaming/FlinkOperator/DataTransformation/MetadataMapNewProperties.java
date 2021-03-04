package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;

public class MetadataMapNewProperties implements MapFunction<String,String>{
	private List<String> operations;
	
	public MetadataMapNewProperties(List<String> operations) {
		this.operations = operations;
	}
	
	@Override
	public String map(String value) throws Exception {
		if (value.startsWith("v")) {
			value += ",numericId:long,zoomLevel:int";
			if (operations.contains("degree")) value += ",degree:long";
			if (operations.contains("layout")) value += ",X:int,Y:int";
		}
		return value;
	}

}
