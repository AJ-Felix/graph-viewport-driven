package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtil;

import java.util.Set;

//wrapper format:
//graphIdGradoop ; 
//sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree ; sourceZoomLevel ;
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; targetZoomLevel ;
//edgeIdGradoop ; edgeLabel


public interface GraphUtil {
	void initializeDataSets() throws Exception;
	void setVisualizedVertices(Set<String> visualizedVertices);
	void setVisualizedWrappers(Set<String> visualizedWrappers);
	void setVertexZoomLevel(int zoomLevel);
}
