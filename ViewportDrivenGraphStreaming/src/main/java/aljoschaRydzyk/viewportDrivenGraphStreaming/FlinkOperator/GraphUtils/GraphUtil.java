package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.util.Map;
import java.util.Set;

public interface GraphUtil {
	void initializeDataSets() throws Exception;
	void setVisualizedVertices(Set<String> visualizedVertices);
	void setVisualizedWrappers(Set<String> visualizedWrappers);
}
