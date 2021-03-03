package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.java.functions.KeySelector;

public class MetadataKeyselectorVertexLabel implements KeySelector<String,String>{

	@Override
	public String getKey(String value) throws Exception {
		String key = "undefined";
		if (value.startsWith("v")) key = value.split(";")[1];
		return key;
	}

}
