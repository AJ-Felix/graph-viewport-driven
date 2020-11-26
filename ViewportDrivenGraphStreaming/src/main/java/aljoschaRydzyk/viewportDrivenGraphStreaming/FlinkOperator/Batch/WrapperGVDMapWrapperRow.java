package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

public class WrapperGVDMapWrapperRow implements MapFunction<WrapperGVD,Row> {
	@Override
	public Row map(WrapperGVD wrapper) throws Exception {
		return Row.of(
				wrapper.getEdgeIdGradoop(),
				wrapper.getEdgeLabel(),
				wrapper.getSourceVertex().getIdGradoop(),
				wrapper.getSourceIdNumeric(),
				wrapper.getSourceDegree(),
				wrapper.getSourceX(),
				wrapper.getSourceY(),
				wrapper.getTargetVertex().getIdGradoop(),
				wrapper.getTargetIdNumeric(),
				wrapper.getTargetDegree(),
				wrapper.getTargetX(),
				wrapper.getTargetY()
				);
	}
}
