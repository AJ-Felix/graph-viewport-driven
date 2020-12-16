package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class WrapperFilterZoomLevelTrue implements FilterFunction<Row> {
	private int zoomLevel;
	
	public WrapperFilterZoomLevelTrue(int zoomLevel) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public boolean filter(Row value) throws Exception {
		return (int) value.getField(14) <= zoomLevel;
	}
}
