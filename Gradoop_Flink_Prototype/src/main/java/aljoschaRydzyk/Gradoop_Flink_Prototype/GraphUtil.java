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
	DataStream<Row> zoom(Float topModel, Float rightModel, Float bottomModel, Float leftModel) throws IOException;
	DataStream<Row> pan(Float topOld, Float rightOld, Float bottomOld, Float leftOld, Float xModelDiff,
			Float yModelDiff);
	void setVisualizedVertices(Set<String> visualizedVertices);
	void setVisualizedWrappers(Set<String> visualizedWrappers);
	Map<String,Map<String,String>> buildAdjacencyMatrix() throws Exception;
	Map<String, Map<String, String>> getAdjMatrix();
	DataStream<Row> zoomOutLayoutSecondStep(Map<String, VertexCustom> layoutedVertices,
			Map<String, VertexCustom> newVertices, Float topModelPos, Float rightModelPos, Float bottomModelPos,
			Float leftModelPos);
	DataStream<Row> zoomOutLayoutFirstStep(Map<String, VertexCustom> layoutedVertices, Float topModelPos,
			Float rightModelPos, Float bottomModelPos, Float leftModelPos, Float topModelPosOld, Float rightModelPosOld,
			Float bottomModelPosOld, Float leftModelPosOld);
	DataStream<Row> zoomInLayoutFourthStep(Map<String, VertexCustom> layoutedVertices,
			Map<String, VertexCustom> innerVertices, Map<String, VertexCustom> newVertices, Float topModelPos,
			Float rightModelPos, Float bottomModelPos, Float leftModelPos);
	DataStream<Row> panZoomInLayoutThirdStep(Map<String, VertexCustom> layoutedVertices);
	DataStream<Row> panZoomInLayoutSecondStep(Map<String, VertexCustom> layoutedVertices,
			Map<String, VertexCustom> unionMap);
	DataStream<Row> panZoomInLayoutFirstStep(Map<String, VertexCustom> layoutedVertices,
			Map<String, VertexCustom> innerVertices, Float topModelPos, Float rightModelPos, Float bottomModelPos,
			Float leftModelPos);
	DataStream<Row> panLayoutFourthStep(Map<String, VertexCustom> layoutedVertices,
			Map<String, VertexCustom> newVertices, Float topModelPos, Float rightModelPos, Float bottomModelPos,
			Float leftModelPos, Float xModelDiff, Float yModelDiff);
}
