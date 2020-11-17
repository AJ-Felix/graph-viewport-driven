package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.VertexGVD;

public class VertexFilterIsLayoutedOutside implements FilterFunction<Row> {
	private Map<String,VertexGVD> layoutedVertices;
	private Float topModel;
	private Float rightModel;
	private Float bottomModel;
	private Float leftModel;
	
	public VertexFilterIsLayoutedOutside (Map<String,VertexGVD> layoutedVertices, Float topModel, Float rightModel, Float bottomModel, 
			Float leftModel) {
		this.layoutedVertices = layoutedVertices;
		this.topModel = topModel;
		this.rightModel = rightModel;
		this.bottomModel = bottomModel;
		this.leftModel = leftModel;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		if (this.layoutedVertices.containsKey(value.getField(1))) {
			Integer x = this.layoutedVertices.get(value.getField(1)).getX();
			Integer y = this.layoutedVertices.get(value.getField(1)).getY();
			if (x >= leftModel && x <= rightModel && y >= topModel && y <= bottomModel) {
				return false;
			} else {
				return true;
			} 
		} else {
			return false;
		}
	}
}
