package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

//graphIdGradoop ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; edgeIdGradoop ; edgeLabel

public interface GraphUtil { 
	void initializeStreams() throws Exception;
	DataStream<Row> getVertexStream();
	DataStream<Row> zoom(Float top, Float right, Float bottom, Float left) throws IOException;
	DataStream<Row> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, 
			Float bottomOld, Float leftOld);
	void setVisualizedVertices(Set<String> visualizedVertices);
	void setVisualizedWrappers(Set<String> visualizedWrappers);
	Map<String,Map<String,String>> buildAdjacencyMatrix() throws Exception;
	Map<String, Map<String, String>> getAdjMatrix();
	DataStream<Row> zoomOutLayoutSecondStep(Map<String, VertexCustom> layoutedVertices,
			Map<String, VertexCustom> newVertices, Float top, Float right, Float bottom,
			Float left);
	DataStream<Row> zoomOutLayoutFirstStep(Map<String, VertexCustom> layoutedVertices, Float topNew,
			Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld,
			Float bottomOld, Float leftOld);
	DataStream<Row> zoomInLayoutFourthStep(Map<String, VertexCustom> layoutedVertices,
			Map<String, VertexCustom> innerVertices, Map<String, VertexCustom> newVertices, Float top,
			Float right, Float bottom, Float left);
	DataStream<Row> panZoomInLayoutThirdStep(Map<String, VertexCustom> layoutedVertices);
	DataStream<Row> panZoomInLayoutSecondStep(Map<String, VertexCustom> layoutedVertices,
			Map<String, VertexCustom> unionMap);
	DataStream<Row> panZoomInLayoutFirstStep(Map<String, VertexCustom> layoutedVertices,
			Map<String, VertexCustom> innerVertices, Float top, Float right, Float bottom,
			Float left);
	DataStream<Row> panLayoutFourthStep(Map<String, VertexCustom> layoutedVertices,
			Map<String, VertexCustom> newVertices, Float topNew, Float rightNew, Float bottomNew,
			Float leftNew, Float topOld, Float rightOld, Float bottomOld, Float leftOld);
}
