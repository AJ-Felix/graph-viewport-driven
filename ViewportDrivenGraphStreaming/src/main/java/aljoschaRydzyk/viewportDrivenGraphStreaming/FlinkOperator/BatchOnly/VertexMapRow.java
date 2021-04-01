package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.BatchOnly;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMVertex;

public class VertexMapRow implements MapFunction<EPGMVertex,Row>{

	@Override
	public Row map(EPGMVertex value) throws Exception {
		return Row.of(value.getGraphIds().iterator().next().toString(),
				value.getId(), 
				value.getPropertyValue("numericId").getLong(),
				value.getLabel(),
				value.getPropertyValue("X").getInt(),
				value.getPropertyValue("Y").getInt(),
				value.getPropertyValue("degree").getLong(),
				value.getPropertyValue("zoomLevel").getInt());
	}

}
