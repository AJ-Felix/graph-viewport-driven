package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex;

import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;

public class VertexFlatMapIsLayoutedInnerOldNotNewBi implements FlatMapFunction<Row,String> {
	private Map<String,VertexGVD> layoutedVertices;
	private Map<String,Map<String,String>> adjMatrix;
	private float topNew;
	private float rightNew;
	private float bottomNew;
	private float leftNew;
	private float topOld;
	private float rightOld;
	private float bottomOld;
	private float leftOld;
	
	public VertexFlatMapIsLayoutedInnerOldNotNewBi (Map<String,VertexGVD> layoutedVertices, Map<String,Map<String,String>> adjMatrix,
			float topNew, float rightNew, float bottomNew, float leftNew, float topOld, float rightOld, 
			float bottomOld, float leftOld) {
		this.layoutedVertices = layoutedVertices;
		this.adjMatrix = adjMatrix;
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
				if (x >= leftOld && x <= rightOld && y >= topOld && y <= bottomOld &&
						(leftNew > x || x > rightNew || topNew > y || y > bottomNew)) {
					out.collect(entry.getValue());
				}
			}
		}
	}	
}
