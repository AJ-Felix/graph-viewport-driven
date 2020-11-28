package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;

public class VertexFlatMapIsLayoutedInnerNewNotOldUni implements FlatMapFunction<Row,String>{
	private Map<String,Map<String,String>> adjMatrix;
	private Map<String,VertexGVD> layoutedVertices;
	private Float topNew;
	private Float rightNew;
	private Float bottomNew;
	private Float leftNew;
	private Float topOld;
	private Float rightOld;
	private Float bottomOld;
	private Float leftOld;
	
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
				int x = layoutedVertices.get(targetId).getX();
				int y = layoutedVertices.get(targetId).getY();
				if ((leftNew <= x) &&  (x <= rightNew) && (topNew <= y) && (y <= bottomNew)
						&& ((leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld))
						&& sourceId.compareTo(targetId) > 0) {
					out.collect(entry.getValue());
				}
			}
		}
	}	
}
