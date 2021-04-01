package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtil;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;

public interface GraphUtilStream extends GraphUtil{ 
	DataStream<Row> zoom(Float top, Float right, Float bottom, Float left) throws IOException;
	DataStream<Row> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, 
			Float bottomOld, Float leftOld);
	DataStream<Row> zoomOutLayoutStep2(Map<String, VertexVDrive> layoutedVertices,
			Map<String, VertexVDrive> newVertices, Float top, Float right, Float bottom,
			Float left);
	DataStream<Row> zoomOutLayoutStep1(Map<String, VertexVDrive> layoutedVertices, Float topNew,
			Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld,
			Float bottomOld, Float leftOld);
	DataStream<Row> zoomInLayoutStep4(Map<String, VertexVDrive> layoutedVertices,
			Map<String, VertexVDrive> innerVertices, Map<String, VertexVDrive> newVertices, Float top,
			Float right, Float bottom, Float left);
	DataStream<Row> panZoomInLayoutStep3(Map<String, VertexVDrive> layoutedVertices);
	DataStream<Row> panZoomInLayoutStep2(Map<String, VertexVDrive> layoutedVertices,
			Map<String, VertexVDrive> unionMap);
	DataStream<Row> panZoomInLayoutStep1(Map<String, VertexVDrive> layoutedVertices,
			Map<String, VertexVDrive> innerVertices, Float top, Float right, Float bottom,
			Float left);
	DataStream<Row> panLayoutStep4(Map<String, VertexVDrive> layoutedVertices,
			Map<String, VertexVDrive> newVertices, Float topNew, Float rightNew, Float bottomNew,
			Float leftNew, Float topOld, Float rightOld, Float bottomOld, Float leftOld);
	DataStream<Row> getMaxDegreeSubset(int numberVertices) throws IOException;
}
