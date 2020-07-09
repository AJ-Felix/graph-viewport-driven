package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.datastream.DataStream;

public interface GraphUtil {
	DataStream<VVEdgeWrapper> produceWrapperStream() throws Exception;
	DataStream<VVEdgeWrapper> getWrapperStream() throws Exception;
}
