package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import aljoschaRydzyk.viewportDrivenGraphStreaming.VertexGVD;

public class VertexFlatMapIsLayoutedOutsideBi implements FlatMapFunction<Row,String>{
	private Map<String,VertexGVD> layoutedVertices;
	private Map<String,Map<String,String>> adjMatrix;
	private Float topModel;
	private Float rightModel;
	private Float bottomModel;
	private Float leftModel;
	
	public VertexFlatMapIsLayoutedOutsideBi (Map<String,VertexGVD> layoutedVertices, Map<String,Map<String,String>> adjMatrix,
			Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
		this.layoutedVertices = layoutedVertices;
		this.adjMatrix = adjMatrix;
		this.topModel = topModel;
		this.rightModel = rightModel;
		this.bottomModel = bottomModel;
		this.leftModel = leftModel;
	}
	
	@Override
	public void flatMap(Row value, Collector<String> out) throws Exception {
		String sourceId = value.getField(1).toString();
		for (Map.Entry<String, String> entry : adjMatrix.get(sourceId).entrySet()) {
			String targetId = entry.getKey();
			if (layoutedVertices.containsKey(targetId)) {
				Integer x = layoutedVertices.get(targetId).getX();
				Integer y = layoutedVertices.get(targetId).getY();
				if (!(x >= leftModel && x <= rightModel && y >= topModel && y <= bottomModel)) {
					out.collect(entry.getValue());
				}
			}
		}
	}	
}
