package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Map;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterNotInsideBefore implements FilterFunction<Row> {
	private Map<String,VertexCustom> layoutedVertices;
	private Float topModelOld;
	private Float rightModelOld;
	private Float bottomModelOld;
	private Float leftModelOld;
	
	public VertexFilterNotInsideBefore (Map<String,VertexCustom> layoutedVertices, Float topModelOld, Float rightModelOld, Float bottomModelOld, 
			Float leftModelOld) {
		this.layoutedVertices = layoutedVertices;
		this.topModelOld = topModelOld;
		this.rightModelOld = rightModelOld;
		this.bottomModelOld = bottomModelOld;
		this.leftModelOld = leftModelOld;
	}
	
	@Override
	public boolean filter(Row value) throws Exception {
		Integer x = this.layoutedVertices.get(value.getField(1)).getX();
		Integer y = this.layoutedVertices.get(value.getField(1)).getY();
		if (x >= leftModelOld && x <= rightModelOld && y >= topModelOld && y <= bottomModelOld) {
			return false;
		} else {
			return true;
		} 
	}
}
