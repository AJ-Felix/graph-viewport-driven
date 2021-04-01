package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;

public class WrapperFilterIsLayoutedOutsideReverse implements FilterFunction<Row> {
	private Map<String,VertexVDrive> layoutedVertices;
	private Float topModel;
	private Float rightModel;
	private Float bottomModel;
	private Float leftModel;
	
	public WrapperFilterIsLayoutedOutsideReverse (Map<String,VertexVDrive> layoutedVertices, Float topModel, Float rightModel, Float bottomModel, 
			Float leftModel) {
		this.layoutedVertices = layoutedVertices;
		this.topModel = topModel;
		this.rightModel = rightModel;
		this.bottomModel = bottomModel;
		this.leftModel = leftModel;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		if (this.layoutedVertices.containsKey(value.getField(1).toString())) {
			int x = this.layoutedVertices.get(value.getField(1).toString()).getX();
			int y = this.layoutedVertices.get(value.getField(1).toString()).getY();
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
