package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.util.Map;

import org.apache.flink.api.java.DataSet;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

public interface GraphUtilSet extends GraphUtil{
	DataSet<WrapperGVD> zoom(Float top, Float right, Float bottom, Float left);
	DataSet<WrapperGVD> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld,
			Float bottomOld, Float leftOld);
	DataSet<WrapperGVD> panZoomInLayoutStep1(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices,
			Float topNew, Float rightNew, Float bottomNew, Float leftNew);
	DataSet<WrapperGVD> panZoomInLayoutStep2(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> unionMap);
	DataSet<WrapperGVD> panZoomInLayoutStep3(Map<String, VertexGVD> layoutedVertices);
	DataSet<WrapperGVD> zoomInLayoutStep4(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices,
			Map<String, VertexGVD> newVertices, Float top, Float right, Float bottom, Float left);
	DataSet<WrapperGVD> panLayoutStep4(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices,
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld,
			Float leftOld);
	DataSet<WrapperGVD> zoomOutLayoutStep1(Map<String, VertexGVD> layoutedVertices, Float topNew, Float rightNew,
			Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld, Float leftOld);
	DataSet<WrapperGVD> zoomOutLayoutStep2(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices,
			Float top, Float right, Float bottom, Float left);
}
