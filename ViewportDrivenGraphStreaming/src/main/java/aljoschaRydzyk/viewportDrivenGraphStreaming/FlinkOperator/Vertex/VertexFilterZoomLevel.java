package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterZoomLevel implements FilterFunction<Row>{
	private int zoomLevel;
	
	public VertexFilterZoomLevel(int zoomLevel) {
		this.zoomLevel = zoomLevel;
	}

	@Override
	public boolean filter(Row value) throws Exception {
		return (int) value.getField(7) <= zoomLevel;
	}
}
