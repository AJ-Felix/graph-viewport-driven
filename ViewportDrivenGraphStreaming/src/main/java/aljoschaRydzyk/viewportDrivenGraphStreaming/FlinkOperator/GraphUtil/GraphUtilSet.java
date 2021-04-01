package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtil;

import java.util.Map;

import org.apache.flink.api.java.DataSet;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperVDrive;

public interface GraphUtilSet extends GraphUtil{
	DataSet<WrapperVDrive> getMaxDegreeSubset(int numberVertices);
	DataSet<WrapperVDrive> zoom(Float top, Float right, Float bottom, Float left);
	DataSet<WrapperVDrive> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld,
			Float bottomOld, Float leftOld);
	DataSet<WrapperVDrive> panZoomInLayoutStep1(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> innerVertices,
			Float topNew, Float rightNew, Float bottomNew, Float leftNew);
	DataSet<WrapperVDrive> panZoomInLayoutStep2(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> unionMap);
	DataSet<WrapperVDrive> panZoomInLayoutStep3(Map<String, VertexVDrive> layoutedVertices);
	DataSet<WrapperVDrive> zoomInLayoutStep4(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> innerVertices,
			Map<String, VertexVDrive> newVertices, Float top, Float right, Float bottom, Float left);
	DataSet<WrapperVDrive> panLayoutStep4(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices,
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld,
			Float leftOld);
	DataSet<WrapperVDrive> zoomOutLayoutStep1(Map<String, VertexVDrive> layoutedVertices, Float topNew, Float rightNew,
			Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld, Float leftOld);
	DataSet<WrapperVDrive> zoomOutLayoutStep2(Map<String, VertexVDrive> layoutedVertices, Map<String, VertexVDrive> newVertices,
			Float top, Float right, Float bottom, Float left);
}
