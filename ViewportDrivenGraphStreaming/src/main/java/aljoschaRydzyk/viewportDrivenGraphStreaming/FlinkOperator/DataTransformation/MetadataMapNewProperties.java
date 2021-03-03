package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.common.functions.MapFunction;

public class MetadataMapNewProperties implements MapFunction<String,String>{

	@Override
	public String map(String value) throws Exception {
		if (value.startsWith("v")) value += ",numericId:long,zoomLevel:int";
		return value;
	}

}
