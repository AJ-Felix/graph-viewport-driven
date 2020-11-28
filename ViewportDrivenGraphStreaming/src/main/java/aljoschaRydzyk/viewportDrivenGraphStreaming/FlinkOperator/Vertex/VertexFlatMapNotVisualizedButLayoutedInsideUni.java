package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;

public class VertexFlatMapNotVisualizedButLayoutedInsideUni implements FlatMapFunction<Row,String>{
	Map<String,Map<String,String>> adjMatrix;
	Map<String,VertexGVD> layoutedVertices;
	Set<String> innerVertices;
	Float top;
	Float right;
	Float bottom;
	Float left;
	
	public VertexFlatMapNotVisualizedButLayoutedInsideUni(Map<String,Map<String,String>> adjMatrix, 
			Map<String,VertexGVD> layoutedVertices, Set<String> innerVertices,
			Float top, Float right, Float bottom, Float left) {
		this.adjMatrix = adjMatrix;
		this.layoutedVertices = layoutedVertices;
		this.innerVertices = innerVertices;
		this.top = top;
		this.right = right;
		this.bottom = bottom;
		this.left = left;
	}
	
	@Override
	public void flatMap(Row value, Collector<String> out) throws Exception {
		String sourceId = value.getField(1).toString();
		System.out.println("in VertexFlatMapNotVisualizedButLayoutedInside, sourceId: " + sourceId);
		for (Map.Entry<String, String> entry : adjMatrix.get(sourceId).entrySet()) {
			String targetId = entry.getKey();
			if (!innerVertices.contains(targetId) && layoutedVertices.containsKey(targetId)) {
				VertexGVD layoutedVertex = layoutedVertices.get(targetId);
				Integer x = layoutedVertex.getX();
				Integer y = layoutedVertex.getY();
				if (x >= left && x <= right && y >= top && y <= bottom && sourceId.compareTo(targetId) > 0) {
					out.collect(entry.getValue());
				}
			}
		}
	}		
}
