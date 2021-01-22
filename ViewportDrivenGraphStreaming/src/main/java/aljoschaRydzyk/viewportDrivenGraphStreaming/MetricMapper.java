package aljoschaRydzyk.viewportDrivenGraphStreaming;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.types.Row;

public class MetricMapper extends RichMapFunction<Row,Row>{

	@Override
	public void open(Configuration config) {
		getRuntimeContext();
	}
	
	@Override
	public Row map(Row value) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

}
