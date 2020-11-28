package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;

public class VertexFilterIsLayoutedInnerNewNotOld implements FilterFunction<Row> {
	private Map<String,VertexGVD> layoutedVertices;
	private Float leftNew;
	private Float rightNew;
	private Float topNew;
	private Float bottomNew;
	private Float leftOld;
	private Float rightOld;
	private Float topOld;
	private Float bottomOld;
	
	public VertexFilterIsLayoutedInnerNewNotOld(Map<String,VertexGVD> layoutedVertices,
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
		if (this.layoutedVertices.containsKey(value.getField(1).toString())) {
			Integer x = this.layoutedVertices.get(value.getField(1).toString()).getX();
			Integer y = this.layoutedVertices.get(value.getField(1).toString()).getY();
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
