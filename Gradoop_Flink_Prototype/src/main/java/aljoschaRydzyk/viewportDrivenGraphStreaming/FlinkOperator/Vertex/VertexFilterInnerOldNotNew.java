package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterInnerOldNotNew implements FilterFunction<Row> {
	Float leftNew;
	Float rightNew;
	Float topNew;
	Float bottomNew;
	Float leftOld;
	Float rightOld;
	Float topOld;
	Float bottomOld;
	
	public VertexFilterInnerOldNotNew(Float leftNew, Float rightNew, Float topNew, Float bottomNew, Float leftOld, Float rightOld, Float topOld,
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
		return (leftOld <= x) && (x <= rightOld) && (topOld <= y) && (y <= bottomOld) && 
				((leftNew > x) || (x > rightNew) || (topNew > y) || (y > bottomNew));
	}
}
