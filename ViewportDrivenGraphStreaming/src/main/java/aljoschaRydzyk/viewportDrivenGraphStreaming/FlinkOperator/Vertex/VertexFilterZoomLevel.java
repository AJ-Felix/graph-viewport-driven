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
		System.out.println("zoomLevel: " + zoomLevel);
		System.out.println("vertexZoomLevel: " + value.getField(7));
		System.out.println((int) value.getField(7) <= zoomLevel);
		return (int) value.getField(7) <= zoomLevel;
	}
}
