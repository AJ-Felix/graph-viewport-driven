package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.IOException;
import java.util.Set;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

public interface GraphUtil { 
	DataStream<Row> initializeStreams() throws Exception;
	DataStream<Row> getVertexStream();
	DataStream<Row> zoom(Float topModel, Float rightModel, Float bottomModel, Float leftModel) throws IOException;
	DataStream<Row> pan(Float topOld, Float rightOld, Float bottomOld, Float leftOld, Float xModelDiff,
			Float yModelDiff) throws IOException;
	void setVisualizedVertices(Set<String> visualizedVertices);
	void setVisualizedWrappers(Set<String> visualizedWrappers);
}
