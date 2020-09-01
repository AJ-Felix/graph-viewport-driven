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

import Temporary.VertexCustom;

//graphIdGradoop ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; edgeIdGradoop ; edgeLabel

public class GradoopToCSV {
	
	public static class WrapperDegreeComparator implements Comparator<VVEdgeWrapper>{
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
	
	public static class VertexDegreeComparator implements Comparator<EPGMVertex>{
		@Override
		public int compare(EPGMVertex v1, EPGMVertex v2) {
			if (v1.getPropertyValue("degree").getLong() > v2.getPropertyValue("degree").getLong()) return -1;
			else if (v1.getPropertyValue("degree").getLong() == v2.getPropertyValue("degree").getLong()) return 0;
			else return 1;
		}
		
	}
	
	public static void parseGradoopToCSV(LogicalGraph graph, String outPath) throws Exception {
		List<EPGMGraphHead> lGraphHead = graph.getGraphHead().collect();
		List<EPGMVertex> lVertices = graph.getVertices().collect();
		List<EPGMEdge> lEdges = graph.getEdges().collect();
		Map<String,Integer> vertexIdMap =  new HashMap<String,Integer>();
		List<VVEdgeWrapper> lVVEdgeWrapper = new ArrayList<VVEdgeWrapper>();
		File verticesFile = new File(outPath + "_vertices");
		verticesFile.createNewFile();
		PrintWriter verticesWriter = new PrintWriter(verticesFile);
		lVertices.sort(new VertexDegreeComparator());
		for (int i = 0; i < lVertices.size(); i++) 	{
			vertexIdMap.put(lVertices.get(i).getId().toString(), i);
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append(lGraphHead.get(0).toString());
			stringBuilder.append(";");
			stringBuilder.append(lVertices.get(i).getId().toString());
			stringBuilder.append(";");
			stringBuilder.append(i);
			stringBuilder.append(";");
			stringBuilder.append(lVertices.get(i).getLabel());
			stringBuilder.append(";");
			stringBuilder.append(lVertices.get(i).getPropertyValue("X"));
			stringBuilder.append(";");
			stringBuilder.append(lVertices.get(i).getPropertyValue("Y"));
			stringBuilder.append(";");
			stringBuilder.append(lVertices.get(i).getPropertyValue("degree"));
			stringBuilder.append(";");
			stringBuilder.append(lVertices.get(i).getId().toString());
			stringBuilder.append(";");
			stringBuilder.append(i);
			stringBuilder.append(";");
			stringBuilder.append(lVertices.get(i).getLabel());
			stringBuilder.append(";");
			stringBuilder.append(lVertices.get(i).getPropertyValue("X"));
			stringBuilder.append(";");
			stringBuilder.append(lVertices.get(i).getPropertyValue("Y"));
			stringBuilder.append(";");
			stringBuilder.append(lVertices.get(i).getPropertyValue("degree"));
			stringBuilder.append(";");
			stringBuilder.append("identityEdge");
			stringBuilder.append(";");
			stringBuilder.append("identityEdge");
			stringBuilder.append("\n");
			verticesWriter.write(stringBuilder.toString());
		}
		verticesWriter.close();
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
		lVVEdgeWrapper.sort(new WrapperDegreeComparator());
		File wrapperFile = new File(outPath + "_wrappers");
		wrapperFile.createNewFile();
		PrintWriter wrapperWriter = new PrintWriter(wrapperFile);
		for (VVEdgeWrapper wrapper : lVVEdgeWrapper) {
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append(lGraphHead.get(0).getId());
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
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getEdgeIdGradoop());
			stringBuilder.append(";");
			stringBuilder.append(wrapper.getEdgeLabel());
			stringBuilder.append("\n");
			wrapperWriter.write(stringBuilder.toString());
		}
		wrapperWriter.close();
		File adjacencyFile = new File(outPath + "_adjacency");
		adjacencyFile.createNewFile();
		PrintWriter adjacencyWriter = new PrintWriter(adjacencyFile);
		for (int i = 0; i < lVertices.size(); i++) 	{
			StringBuilder stringBuilder = new StringBuilder();
			GradoopId vId1 = lVertices.get(i).getId();
			stringBuilder.append(vId1);
			stringBuilder.append(";");
			for (int j = 0; j < lVertices.size(); j++) 	{
				boolean incident = false;
				GradoopId vId2 = lVertices.get(j).getId();
				for (int k = 0; k < lEdges.size(); k++) 	{
					EPGMEdge edge = lEdges.get(k);
					GradoopId sourceId = edge.getSourceId();
					GradoopId targetId = edge.getTargetId();
					if (sourceId.equals(vId1) && targetId.equals(vId2) || sourceId.equals(vId2) && targetId.equals(vId1)) {
						incident = true;
					}
				}
				if (incident) {
					stringBuilder.append(vId2 + "," + "1");
				} else {
					stringBuilder.append(vId2 + "," + "0");
				}
				stringBuilder.append(";");
			}
			stringBuilder.substring(0, stringBuilder.length() - 1);
			stringBuilder.append("\n");
			adjacencyWriter.write(stringBuilder.toString());
		}
		adjacencyWriter.close();
	}
}
