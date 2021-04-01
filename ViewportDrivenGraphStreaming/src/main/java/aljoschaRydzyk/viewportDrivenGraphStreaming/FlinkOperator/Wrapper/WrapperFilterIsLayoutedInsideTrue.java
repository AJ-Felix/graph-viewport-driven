package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;

public class WrapperFilterIsLayoutedInsideTrue implements FilterFunction<Row> {
	private Map<String,VertexVDrive> layoutedVertices;
	private Float topModel;
	private Float rightModel;
	private Float bottomModel;
	private Float leftModel;
	
	public WrapperFilterIsLayoutedInsideTrue (Map<String,VertexVDrive> layoutedVertices, Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
		this.layoutedVertices = layoutedVertices;
		this.topModel = topModel;
		this.rightModel = rightModel;
		this.bottomModel = bottomModel;
		this.leftModel = leftModel;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		if (this.layoutedVertices.containsKey(value.getField(8).toString())) {
			int x = this.layoutedVertices.get(value.getField(8).toString()).getX();
			int y = this.layoutedVertices.get(value.getField(8).toString()).getY();
			if (x >= leftModel && x <= rightModel && y >= topModel && y <= bottomModel) {
				return true;
			} else {
				return false;
			} 
		} else {
			return false;
		}
	}
}
