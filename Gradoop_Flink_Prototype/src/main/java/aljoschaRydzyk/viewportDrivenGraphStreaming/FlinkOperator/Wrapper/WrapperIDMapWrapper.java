package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

public class WrapperIDMapWrapper implements MapFunction<String,Row>{
	Map<String,Row> wrapperMap;
	
	public WrapperIDMapWrapper(Map<String,Row> wrapperMap) {
		this.wrapperMap = wrapperMap;
	}
	
	@Override
	public Row map(String value) throws Exception {
		return wrapperMap.get(value);
	}
}
