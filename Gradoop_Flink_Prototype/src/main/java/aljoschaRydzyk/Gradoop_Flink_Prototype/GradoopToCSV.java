package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

//graphIdGradoop ; edgeIdGradoop ; edgeLabel ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree

public class GradoopToCSV {
	
	public static class DegreeComparator implements Comparator<VVEdgeWrapper>{
		@Override
		public int compare(VVEdgeWrapper w1, VVEdgeWrapper w2) {
			Long maxDegree = w1.getSourceDegree();
			if (w1.getTargetDegree() > maxDegree) maxDegree = w1.getTargetDegree();
			if ((w2.getSourceDegree() > maxDegree) || (w2.getTargetDegree() > maxDegree)){
				return 1;
			} else if ((w2.getSourceDegree() == maxDegree) || (w2.getTargetDegree() == maxDegree)){
				return 0;
			} else {
				return -1;
			}
		}
	}
	
	public static void parseGradoopToCSV(LogicalGraph graph, String outPath) throws Exception {
		List<EPGMGraphHead> lGraphHead = graph.getGraphHead().collect();
		List<EPGMVertex> lVertices = graph.getVertices().collect();
		List<EPGMEdge> lEdges = graph.getEdges().collect();
		Map<String,Integer> vertexIdMap =  new HashMap<String,Integer>();
		List<VVEdgeWrapper> lVVEdgeWrapper = new ArrayList<VVEdgeWrapper>();
		for (int i = 0; i < lVertices.size(); i++) 	vertexIdMap.put(lVertices.get(i).getId().toString(), i);
		for (EPGMEdge edge : lEdges) {
			for (EPGMVertex sourceVertex : lVertices) {
				for (EPGMVertex targetVertex : lVertices) {
					GradoopId edgeSourceId = edge.getSourceId();
					if ((edgeSourceId.equals(sourceVertex.getId())) && (edge.getTargetId().equals(targetVertex.getId()))) {
						EdgeCustom edgeCustom = new EdgeCustom(edge.getId().toString(), edge.getLabel(), edgeSourceId.toString(), edge.getTargetId().toString());
						VertexCustom sourceVertexCustom = new VertexCustom(sourceVertex.getId().toString(), sourceVertex.getLabel(), 
								vertexIdMap.get(sourceVertex.getId().toString()), 
								sourceVertex.getPropertyValue("X").getInt(), sourceVertex.getPropertyValue("Y").getInt(),
								sourceVertex.getPropertyValue("degree").getLong());
						VertexCustom targetVertexCustom = new VertexCustom(targetVertex.getId().toString(), targetVertex.getLabel(),
								vertexIdMap.get(targetVertex.getId().toString()),
								targetVertex.getPropertyValue("X").getInt(), targetVertex.getPropertyValue("Y").getInt(),
								targetVertex.getPropertyValue("degree").getLong());
						lVVEdgeWrapper.add(new VVEdgeWrapper(sourceVertexCustom, targetVertexCustom, edgeCustom));
					}
				}
			}
		}
		lVVEdgeWrapper.sort(new DegreeComparator());
		File file = new File(outPath);
		file.createNewFile();
		PrintWriter writer = new PrintWriter(file);
		for (VVEdgeWrapper wrapper : lVVEdgeWrapper) {
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append(lGraphHead.get(0).getId());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getEdgeIdGradoop());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getEdgeLabel());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getSourceIdGradoop());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getSourceIdNumeric());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getSourceLabel());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getSourceX());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getSourceY());	
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getSourceDegree());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getTargetIdGradoop());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getTargetIdNumeric());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getTargetLabel());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getTargetX());			
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getTargetY());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getTargetDegree());
			stringBuilder.append("\n");
			writer.write(stringBuilder.toString());
		}
		writer.close();
	}
}
