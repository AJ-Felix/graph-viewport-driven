package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.io.IOException;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public interface GraphUtilSet extends GraphUtil{
	DataSet<Row> zoom(Float top, Float right, Float bottom, Float left);
}
