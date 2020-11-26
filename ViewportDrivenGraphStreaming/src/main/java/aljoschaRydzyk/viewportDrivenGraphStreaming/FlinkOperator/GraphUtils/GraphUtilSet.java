package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.io.IOException;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

public interface GraphUtilSet extends GraphUtil{
	DataSet<WrapperGVD> zoom(Float top, Float right, Float bottom, Float left);
	DataSet<WrapperGVD> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld,
			Float bottomOld, Float leftOld);
}
