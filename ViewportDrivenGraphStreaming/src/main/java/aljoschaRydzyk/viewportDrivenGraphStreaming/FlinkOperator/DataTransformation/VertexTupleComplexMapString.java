package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;

public class VertexTupleComplexMapString implements 
MapFunction<Tuple2<String, Tuple2<Long, Tuple2<EPGMVertex, Long>>>,String>{
	private String graphId;
	private int zoomLevelSetSize;
	
	public VertexTupleComplexMapString(String graphId, int zoomLevelSetSize) {
		this.graphId = graphId;
		this.zoomLevelSetSize = zoomLevelSetSize;
	}


	@Override
	public String map(Tuple2<String, Tuple2<Long, Tuple2<EPGMVertex, Long>>> value) throws Exception {
		String metadata = value.f0;
		EPGMVertex vertex = value.f1.f1.f0;
		String labelMetadata = metadata.split(";")[2];
		String[] properties = labelMetadata.split(",");
		String vertexString = vertex.getId().toString() + ";[" + this.graphId + "];" + 
				vertex.getLabel() + ";";
		for (int i = 0; i < properties.length; i++) {
			String propertyKey = properties[i].split(":")[0];
			String propertyValue = null;
			if (vertex.getProperties().containsKey(propertyKey)) 
				propertyValue = vertex.getPropertyValue(propertyKey).toString();
			if (propertyValue != null) vertexString += vertex.getPropertyValue(propertyKey).toString();
			vertexString += "|";
		}
		vertexString += value.f1.f0 + "|" + 
				String.valueOf(Integer.parseInt(String.valueOf(value.f1.f0)) / zoomLevelSetSize);
//		EPGMVertex vertex = value.f1.f0;
//		String s = "";
//		s += vertex.getId().toString() + ";";
//		s += "[" + this.graphId + "]" + ";";
//		s += vertex.getLabel() + ";";
//		for (String key :vertex.getPropertyKeys()) {
//			s += vertex.getPropertyValue(key) + "|";
//		}
//		s += value.f0 + "|";
//		s += String.valueOf(Integer.parseInt(String.valueOf(value.f0)) / zoomLevelSetSize);
//		return s;		return null;
		return vertexString;
	}
	
}
