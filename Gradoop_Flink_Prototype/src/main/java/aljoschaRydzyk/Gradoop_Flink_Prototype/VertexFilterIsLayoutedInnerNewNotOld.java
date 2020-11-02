package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterIsLayoutedInnerNewNotOld implements FilterFunction<Row> {
	private Map<String,VertexCustom> layoutedVertices;
	Float leftNew;
	Float rightNew;
	Float topNew;
	Float bottomNew;
	Float leftOld;
	Float rightOld;
	Float topOld;
	Float bottomOld;
	
	public VertexFilterIsLayoutedInnerNewNotOld(Map<String,VertexCustom> layoutedVertices,
			Float leftNew, Float rightNew, Float topNew, Float bottomNew, Float leftOld, Float rightOld, Float topOld, 
			Float bottomOld) {
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
		if (this.layoutedVertices.containsKey(value.getField(1))) {
			Integer x = this.layoutedVertices.get(value.getField(1)).getX();
			Integer y = this.layoutedVertices.get(value.getField(1)).getY();
			if ((leftNew <= x) &&  (x <= rightNew) && (topNew <= y) && (y <= bottomNew)
					&& ((leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld))) {
				return true;
			} else {
				return false;
			}	
		} else {
			return false;
		}
	}
}
