package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;

public class WrapperFilterIsLayoutedInnerOldNotNewReverse implements FilterFunction<Row> {
	private Map<String,VertexVDrive> layoutedVertices;
	private Float leftNew;
	private Float rightNew;
	private Float topNew;
	private Float bottomNew;
	private Float leftOld;
	private Float rightOld;
	private Float topOld;
	private Float bottomOld;
	
	public WrapperFilterIsLayoutedInnerOldNotNewReverse(Map<String,VertexVDrive> layoutedVertices,
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld, 
			Float leftOld) {
		this.layoutedVertices = layoutedVertices;
		this.leftNew = leftNew;
		this.rightNew = rightNew;
		this.topNew = topNew;
		this.bottomNew = bottomNew;
		this.leftOld = leftOld;
		this.rightOld = rightOld;
		this.topOld = topOld;
		this.bottomOld = bottomOld;
		
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		if (this.layoutedVertices.containsKey(value.getField(1).toString())) {
			int x = this.layoutedVertices.get(value.getField(1).toString()).getX();
			int y = this.layoutedVertices.get(value.getField(1).toString()).getY();
			if (leftOld <= x &&  x <= rightOld && topOld <= y && y <= bottomOld
					&& (leftNew > x || x > rightNew || topNew > y || y > bottomNew)) {
				return true;
			} else {
				return false;
			}	
		} else {
			return false;
		}
	}
}
