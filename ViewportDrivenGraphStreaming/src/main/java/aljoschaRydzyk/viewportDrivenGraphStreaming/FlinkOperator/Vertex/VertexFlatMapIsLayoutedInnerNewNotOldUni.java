package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import aljoschaRydzyk.viewportDrivenGraphStreaming.VertexGVD;

public class VertexFlatMapIsLayoutedInnerNewNotOldUni implements FlatMapFunction<Row,String>{
	Map<String,Map<String,String>> adjMatrix;
	Map<String,VertexGVD> layoutedVertices;
	Float topNew;
	Float rightNew;
	Float bottomNew;
	Float leftNew;
	Float topOld;
	Float rightOld;
	Float bottomOld;
	Float leftOld;
	
	public VertexFlatMapIsLayoutedInnerNewNotOldUni(Map<String,Map<String,String>> adjMatrix, 
			Map<String,VertexGVD> layoutedVertices, Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, 
			Float rightOld, Float bottomOld, Float leftOld) {
		this.adjMatrix = adjMatrix;
		this.layoutedVertices = layoutedVertices;
		this.topNew = topNew;
		this.rightNew = rightNew;
		this.bottomNew = bottomNew;
		this.leftNew = leftNew;
		this.topOld = topOld;
		this.rightOld = rightOld;
		this.bottomOld = bottomOld;
		this.leftOld = leftOld;
	}
	
	@Override
	public void flatMap(Row value, Collector<String> out) throws Exception {
		String sourceId = value.getField(1).toString();
		for (Map.Entry<String, String> entry : adjMatrix.get(sourceId).entrySet()) {
			String targetId = entry.getKey();
			if (layoutedVertices.containsKey(targetId)) {
				Integer x = layoutedVertices.get(targetId).getX();
				Integer y = layoutedVertices.get(targetId).getY();
				if ((leftNew <= x) &&  (x <= rightNew) && (topNew <= y) && (y <= bottomNew)
						&& ((leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld))
						&& sourceId.compareTo(targetId) > 0) {
					out.collect(entry.getValue());
				}
			}
		}
	}	
}