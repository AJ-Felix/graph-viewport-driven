package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;

//graphIdGradoop ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; edgeIdGradoop ; edgeLabel

public interface GraphUtilStream extends GraphUtil{ 
	DataStream<Row> getVertexStream();
	DataStream<Row> zoom(Float top, Float right, Float bottom, Float left) throws IOException;
	DataStream<Row> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, 
			Float bottomOld, Float leftOld);
	DataStream<Row> zoomOutLayoutSecondStep(Map<String, VertexGVD> layoutedVertices,
			Map<String, VertexGVD> newVertices, Float top, Float right, Float bottom,
			Float left);
	DataStream<Row> zoomOutLayoutFirstStep(Map<String, VertexGVD> layoutedVertices, Float topNew,
			Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld,
			Float bottomOld, Float leftOld);
	DataStream<Row> zoomInLayoutStep4(Map<String, VertexGVD> layoutedVertices,
			Map<String, VertexGVD> innerVertices, Map<String, VertexGVD> newVertices, Float top,
			Float right, Float bottom, Float left);
	DataStream<Row> panZoomInLayoutStep3(Map<String, VertexGVD> layoutedVertices);
	DataStream<Row> panZoomInLayoutStep2(Map<String, VertexGVD> layoutedVertices,
			Map<String, VertexGVD> unionMap);
	DataStream<Row> panZoomInLayoutStep1(Map<String, VertexGVD> layoutedVertices,
			Map<String, VertexGVD> innerVertices, Float top, Float right, Float bottom,
			Float left);
	DataStream<Row> panLayoutFourthStep(Map<String, VertexGVD> layoutedVertices,
			Map<String, VertexGVD> newVertices, Float topNew, Float rightNew, Float bottomNew,
			Float leftNew, Float topOld, Float rightOld, Float bottomOld, Float leftOld);
}
