package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterInnerNewNotOld implements FilterFunction<Row>{
	private Float leftNew;
	private Float rightNew;
	private Float topNew;
	private Float bottomNew;
	private Float leftOld;
	private Float rightOld;
	private Float topOld;
	private Float bottomOld;
	
	public VertexFilterInnerNewNotOld(Float leftNew, Float rightNew, Float topNew, Float bottomNew, Float leftOld, Float rightOld, Float topOld,
			Float bottomOld) {
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
		Integer x = (Integer) value.getField(4);
		Integer y = (Integer) value.getField(5);
		return (leftNew <= x) &&  (x <= rightNew) && (topNew <= y) && (y <= bottomNew)
				&& ((leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld));
	}
}
