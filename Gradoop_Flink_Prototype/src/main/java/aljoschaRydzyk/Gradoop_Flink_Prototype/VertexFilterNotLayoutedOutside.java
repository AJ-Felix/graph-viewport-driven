package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class VertexFilterNotLayoutedOutside implements FilterFunction<Row> {
	private Map<String,VertexCustom> layoutedVertices;
	private Float topModel;
	private Float rightModel;
	private Float bottomModel;
	private Float leftModel;
	
	public VertexFilterNotLayoutedOutside (Map<String,VertexCustom> layoutedVertices, Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
		this.layoutedVertices = layoutedVertices;
		this.topModel = topModel;
		this.rightModel = rightModel;
		this.bottomModel = bottomModel;
		this.leftModel = leftModel;
	}
	@Override
	public boolean filter(Row value) throws Exception {
		if (!this.layoutedVertices.containsKey(value.getField(1))) {
			return true;
		} else {
			Integer x = (Integer) value.getField(4);
			Integer y = (Integer) value.getField(5);
			if (x >= leftModel && x <= rightModel && y >= topModel && y <= bottomModel) {
				return true;
			} else {
				return false;
			}
		}
	}

}
