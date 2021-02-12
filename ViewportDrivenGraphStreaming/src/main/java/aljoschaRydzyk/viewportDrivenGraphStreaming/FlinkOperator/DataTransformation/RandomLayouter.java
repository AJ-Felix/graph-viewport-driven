package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.functions.TransformationFunction;

public class RandomLayouter implements TransformationFunction<EPGMVertex>{
//
//	@Override
//	public EPGMVertex map(EPGMVertex vertex) throws Exception {
//		int x = (int) Math.round(Math.random() * 4000);
//		int y = (int) Math.round(Math.random() * 4000);
//		vertex.setProperty("X", x);
//		vertex.setProperty("Y", y);
//		return null;
//	}

	@Override
	public EPGMVertex apply(EPGMVertex vertex, EPGMVertex transformed) {
		int x = (int) Math.round(Math.random() * 4000);
		int y = (int) Math.round(Math.random() * 4000);
		vertex.setProperty("X", x);
		vertex.setProperty("Y", y);		
		return vertex;
	}
	
}
