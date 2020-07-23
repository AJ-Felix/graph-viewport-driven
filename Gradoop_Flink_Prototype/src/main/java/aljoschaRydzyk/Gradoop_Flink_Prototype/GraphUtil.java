package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

public interface GraphUtil {
	DataStream<Row> produceWrapperStream() throws Exception;
	DataStream<Row> getWrapperStream() throws Exception;
}
