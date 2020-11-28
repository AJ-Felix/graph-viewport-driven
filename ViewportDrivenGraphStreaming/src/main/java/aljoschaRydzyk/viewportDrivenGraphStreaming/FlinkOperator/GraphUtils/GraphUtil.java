package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.util.Set;

//graphIdGradoop ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; edgeIdGradoop ; edgeLabel


public interface GraphUtil {
	void initializeDataSets() throws Exception;
	void setVisualizedVertices(Set<String> visualizedVertices);
	void setVisualizedWrappers(Set<String> visualizedWrappers);
}
