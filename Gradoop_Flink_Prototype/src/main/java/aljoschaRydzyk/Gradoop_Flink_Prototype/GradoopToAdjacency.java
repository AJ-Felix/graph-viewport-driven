package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

public class GradoopToAdjacency {
	public static void parseGradoopToAdjacencyMatrix(LogicalGraph graph, String outPath) throws Exception {
		List<EPGMGraphHead> lGraphHead = graph.getGraphHead().collect();
		List<EPGMVertex> lVertices = graph.getVertices().collect();
		List<EPGMEdge> lEdges = graph.getEdges().collect();
		Map<String,Integer> vertexIdMap =  new HashMap<String,Integer>();
		List<VVEdgeWrapper> lVVEdgeWrapper = new ArrayList<VVEdgeWrapper>();
		File verticesFile = new File(outPath + "_vertices");
		verticesFile.createNewFile();
		PrintWriter verticesWriter = new PrintWriter(verticesFile);
		
	}
}
