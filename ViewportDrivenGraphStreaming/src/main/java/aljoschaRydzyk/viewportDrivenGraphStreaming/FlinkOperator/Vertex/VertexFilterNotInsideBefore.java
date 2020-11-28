package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;

public class VertexFilterNotInsideBefore implements FilterFunction<Row> {
	private Map<String,VertexGVD> layoutedVertices;
	private Float topModelOld;
	private Float rightModelOld;
	private Float bottomModelOld;
	private Float leftModelOld;
	
	public VertexFilterNotInsideBefore (Map<String,VertexGVD> layoutedVertices, Float topModelOld, Float rightModelOld, Float bottomModelOld, 
			Float leftModelOld) {
		this.layoutedVertices = layoutedVertices;
		this.topModelOld = topModelOld;
		this.rightModelOld = rightModelOld;
		this.bottomModelOld = bottomModelOld;
		this.leftModelOld = leftModelOld;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		int x = this.layoutedVertices.get(value.getField(1).toString()).getX();
		int y = this.layoutedVertices.get(value.getField(1).toString()).getY();
		System.out.println("VertexFilterNotInsideBefore, Id: " + value.getField(1) + ", X: " + x + ",Y: " + y);
		if (x >= leftModelOld && x <= rightModelOld && y >= topModelOld && y <= bottomModelOld) {
			return false;
		} else {
			return true;
		} 
	}
}
