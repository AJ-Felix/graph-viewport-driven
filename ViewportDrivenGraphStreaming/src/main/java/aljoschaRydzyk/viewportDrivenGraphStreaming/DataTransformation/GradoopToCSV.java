package aljoschaRydzyk.viewportDrivenGraphStreaming.DataTransformation;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import aljoschaRydzyk.viewportDrivenGraphStreaming.VertexEPGMDegreeComparator;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.EdgeSourceIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.EdgeTargetIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.VertexIDRowKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperSourceIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperTargetIDKeySelector;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch.WrapperTupleMapWrapperGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.EdgeGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.WrapperGVD;

//graphIdGradoop ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; edgeIdGradoop ; edgeLabel

public class GradoopToCSV {
	private static int zoomLevelCoefficient = 250;
	
	public static class WrapperDegreeComparator implements Comparator<WrapperGVD>{
		@Override
		public int compare(WrapperGVD w1, WrapperGVD w2) {
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
	
	public static void parseGradoopToCSV(LogicalGraph graph, String outPath, String graphId) throws Exception {
		int numberVertices = Integer.parseInt(String.valueOf(graph.getVertices().count()));
		int numberZoomLevels = (numberVertices + zoomLevelCoefficient - 1) / zoomLevelCoefficient;
		int zoomLevelSetSize = (numberVertices + numberZoomLevels - 1) / numberZoomLevels;
		
		//vertices
		DataSet<Row> verticesIndexed = DataSetUtils.zipWithIndex((graph.getVertices()
				.map(new MapFunction<EPGMVertex, Tuple2<EPGMVertex,Long>>() {
					@Override
					public Tuple2<EPGMVertex, Long> map(EPGMVertex vertex) throws Exception {
						return Tuple2.of(vertex, vertex.getPropertyValue("degree").getLong());
					}
				})
				.sortPartition(1, Order.DESCENDING).setParallelism(1)
			))
			.map(new MapFunction<Tuple2<Long,Tuple2<EPGMVertex,Long>>,Row>() {
				@Override
				public Row map(Tuple2<Long,Tuple2<EPGMVertex,Long>> tuple) throws Exception {
					EPGMVertex vertex = tuple.f1.f0;
					return Row.of(graphId, vertex.getId(), tuple.f0, vertex.getLabel(),
							vertex.getPropertyValue("X").getInt(), vertex.getPropertyValue("Y").getInt(),
							vertex.getPropertyValue("degree").getLong(), 
							Integer.parseInt(String.valueOf(tuple.f0)) / zoomLevelSetSize);
				}
			});
		MapOperator<Row, Tuple8<String, String, Long, String, Integer, Integer, Long, Integer>> verticesTupled = 
				verticesIndexed.map(new MapFunction<Row,Tuple8<String,String,Long,String,Integer,Integer,Long,Integer>>(){

			@Override
			public Tuple8<String, String, Long, String, Integer, Integer, Long, Integer> map(Row value)
					throws Exception {
				// TODO Auto-generated method stub
				return Tuple8.of(	value.getField(0).toString(),
									value.getField(1).toString(),
									(long) value.getField(2),
									value.getField(3).toString(),
									(int) value.getField(4),
									(int) value.getField(5),
									(long) value.getField(6),
									(int) value.getField(7));
			}
			
		});
		verticesTupled.writeAsCsv(outPath + "_vertices", "\n", ";", WriteMode.OVERWRITE).setParallelism(1);
		
		//wrappers
		DataSet<EPGMEdge> edges = graph.getEdges();
		DataSet<Tuple2<Tuple2<Row, EPGMEdge>, Row>> wrapperTuple = 
				verticesIndexed.join(edges).where(new VertexIDRowKeySelector())
			.equalTo(new EdgeSourceIDKeySelector())
			.join(verticesIndexed).where(new EdgeTargetIDKeySelector())
			.equalTo(new VertexIDRowKeySelector());
		DataSet<Row> wrapper = wrapperTuple.map(new MapFunction<Tuple2<Tuple2<Row, EPGMEdge>,Row>,Row>(){
			@Override
			public Row map(Tuple2<Tuple2<Row, EPGMEdge>, Row> tuple) throws Exception {
				EPGMEdge edge = tuple.f0.f1;
				Row sourceVertex = tuple.f0.f0;
				Row targetVertex = tuple.f1;
				Row vertices = Row.join(sourceVertex, Row.project(targetVertex, new int[] {1, 2, 3, 4, 5, 6, 7}));
				System.out.println(vertices.getArity());
				return Row.join(vertices, Row.of(edge.getId().toString(), edge.getLabel()));
			}
		});
		MapOperator<Row, Tuple17<String, String, Long, String, Integer, Integer, Long, Integer, String, Long, 
		String, Integer, Integer, Long, Integer, String, String>> wrapperTupled = wrapper
		.map(new MapFunction<Row,Tuple17<
				String,
				String,Long,String,Integer,Integer,Long,Integer,
				String,Long,String,Integer,Integer,Long,Integer,
				String,String>>(){

					@Override
					public Tuple17<String, String, Long, String, Integer, Integer, Long, Integer, String, Long, String, Integer, Integer, Long, Integer, String, String> map(
							Row value) throws Exception {
						return Tuple17.of(
								value.getField(0).toString(),
								value.getField(1).toString(),
								(long) value.getField(2),
								value.getField(3).toString(),
								(int) value.getField(4),
								(int) value.getField(5),
								(long) value.getField(6),
								(int) value.getField(7),
								value.getField(8).toString(),
								(long) value.getField(9),
								value.getField(10).toString(),
								(int) value.getField(11),
								(int) value.getField(12),
								(long) value.getField(13),
								(int) value.getField(14),
								value.getField(15).toString(),
								value.getField(16).toString());
					}
			
		});
		
		wrapperTupled.writeAsCsv(outPath + "_wrappers", "\n", ";", WriteMode.OVERWRITE).setParallelism(1);
		
		//adjacency
		DataSet<Tuple2<Tuple2<Row, Row>, Row>> wrapperSet = 
				verticesIndexed.join(wrapper).where(new VertexIDRowKeySelector())
			.equalTo(new WrapperSourceIDKeySelector())
			.join(verticesIndexed).where(new WrapperTargetIDKeySelector())
			.equalTo(new VertexIDRowKeySelector());
		MapOperator<Tuple2<Tuple2<Row, Row>, Row>, Tuple3<String, String, String>> wrapperSet1 = 
				wrapperSet.map(new MapFunction<Tuple2<Tuple2<Row, Row>, Row>, Tuple3<String,String,String>>(){

				@Override
				public Tuple3<String, String, String> map(Tuple2<Tuple2<Row, Row>, Row> value) throws Exception {
					return Tuple3.of(
							value.f0.f0.getField(1).toString(), 
							value.f0.f1.getField(15).toString(),
							value.f1.getField(1).toString());
				}
				
			});
		MapOperator<Tuple2<Tuple2<Row, Row>, Row>, Tuple3<String, String, String>> wrapperSet2 = 
				wrapperSet.map(new MapFunction<Tuple2<Tuple2<Row, Row>, Row>, Tuple3<String,String,String>>(){

				@Override
				public Tuple3<String, String, String> map(Tuple2<Tuple2<Row, Row>, Row> value) throws Exception {
					return Tuple3.of(
							value.f1.getField(1).toString(),
							value.f0.f1.getField(15).toString(),
							value.f0.f0.getField(1).toString());

				}
				
			});
		
		UnionOperator<Tuple3<String, String, String>> wrapperSetUnited = wrapperSet1.union(wrapperSet2);
		 UnsortedGrouping<Tuple3<String, String, String>> wrapperSetGrouped1 = wrapperSet1
				 .groupBy(new KeySelector<Tuple3<String, String, String>,String>(){

			@Override
			public String getKey(Tuple3<String, String, String> value) throws Exception {
				return value.f0.toString();
			}
			
		});
		 UnsortedGrouping<Tuple3<String, String, String>> wrapperSetGrouped2 = wrapperSet2
				 .groupBy(new KeySelector<Tuple3<String, String, String>,String>(){

			@Override
			public String getKey(Tuple3<String, String, String> value) throws Exception {
				return value.f0.toString();
			}
			
		});
		 
		
		
		GroupCombineOperator<Tuple3<String, String, String>, Tuple2<String, List<Tuple2<String, String>>>> 
		wrapperSetCombined1 = wrapperSetGrouped1.combineGroup(new GroupCombineFunction<Tuple3<String,String,String>, 
				Tuple2<String,List<Tuple2<String,String>>>>(){

					@Override
					public void combine(Iterable<Tuple3<String, String, String>> values,
							Collector<Tuple2<String, List<Tuple2<String, String>>>> out) throws Exception {
						List<Tuple2<String,String>> list = new ArrayList<Tuple2<String,String>>();
						String key = null;
						for (Tuple3<String,String,String> tuple3 : values) {
							list.add(Tuple2.of(tuple3.f2, tuple3.f1));
							key = tuple3.f0.toString();
						}
						out.collect(Tuple2.of(key, list));
					}	
					
		});
		System.out.println("wrapperSetCombined: " + wrapperSetCombined1.count());
		GroupCombineOperator<Tuple3<String, String, String>, Tuple2<String, List<Tuple2<String, String>>>> 
		wrapperSetCombined2 = wrapperSetGrouped2.combineGroup(new GroupCombineFunction<Tuple3<String,String,String>, 
				Tuple2<String,List<Tuple2<String,String>>>>(){

					@Override
					public void combine(Iterable<Tuple3<String, String, String>> values,
							Collector<Tuple2<String, List<Tuple2<String, String>>>> out) throws Exception {
						List<Tuple2<String,String>> list = new ArrayList<Tuple2<String,String>>();
						String key = null;
						for (Tuple3<String,String,String> tuple3 : values) {
							list.add(Tuple2.of(tuple3.f2, tuple3.f1));
							key = tuple3.f0.toString();
						}
						out.collect(Tuple2.of(key, list));
					}	
					
		});
		System.out.println("wrapperSetCombined: " + wrapperSetCombined2.count());

		MapOperator<Tuple2<String, List<Tuple2<String, String>>>, String> adjaSet = wrapperSetCombined1.map(new MapFunction<Tuple2<String, List<Tuple2<String, String>>>, String>() {

			@Override
			public String map(Tuple2<String, List<Tuple2<String, String>>> value) throws Exception {
				String result = value.f0.toString();
				for (Tuple2<String,String> tuple2 : value.f1) {
					result += ";" + tuple2.f0.toString() + "," + tuple2.f1.toString();
				}
//				result += "\n";
				return result;
			}
			
		});
		
		adjaSet.writeAsText(outPath + "_adjacency", WriteMode.OVERWRITE).setParallelism(1);
		
		MapOperator<Tuple2<String, List<Tuple2<String, String>>>, String> debugWrapper1 = 
				wrapperSetCombined1.map(new MapFunction<Tuple2<String, List<Tuple2<String, String>>>, String>() {

			@Override
			public String map(Tuple2<String, List<Tuple2<String, String>>> value) throws Exception {
				String result = value.f0.toString();
				return result;
			}
			
		});
		debugWrapper1.writeAsText(outPath + "_adja_debug1", WriteMode.OVERWRITE).setParallelism(1);
		MapOperator<Tuple2<String, List<Tuple2<String, String>>>, String> debugWrapper2 = 
				wrapperSetCombined1.map(new MapFunction<Tuple2<String, List<Tuple2<String, String>>>, String>() {

			@Override
			public String map(Tuple2<String, List<Tuple2<String, String>>> value) throws Exception {
				String result = value.f0.toString();
				return result;
			}
			
		});
		debugWrapper2.writeAsText(outPath + "_adja_debug2", WriteMode.OVERWRITE).setParallelism(1);
//		List<EPGMGraphHead> lGraphHead = graph.getGraphHead().collect();
//		List<EPGMVertex> lVertices = graph.getVertices().collect();
//		List<EPGMEdge> lEdges = graph.getEdges().collect();
//		Map<String,Map<String,Integer>> vertexIdMap =  new HashMap<String,Map<String,Integer>>();
//		List<WrapperGVD> lVVEdgeWrapper = new ArrayList<WrapperGVD>();
//		File verticesFile = new File(outPath + "_vertices");
//		verticesFile.createNewFile();
//		PrintWriter verticesWriter = new PrintWriter(verticesFile);
//		lVertices.sort(new VertexEPGMDegreeComparator());		
//		for (int i = 0; i < lVertices.size(); i++) 	{
//			int vertexZoomLevel = i / zoomLevelSetSize;
//			Map<String,Integer> map = new HashMap<String,Integer>();
//			map.put("numericId", i);
//			map.put("zoomLevel", vertexZoomLevel);
//			vertexIdMap.put(lVertices.get(i).getId().toString(), map);
//			StringBuilder stringBuilder = new StringBuilder();
//			stringBuilder.append(lGraphHead.get(0).toString());
//			stringBuilder.append(";");
//			stringBuilder.append(lVertices.get(i).getId().toString());
//			stringBuilder.append(";");
//			stringBuilder.append(i);
//			stringBuilder.append(";");
//			stringBuilder.append(lVertices.get(i).getLabel());
//			stringBuilder.append(";");
//			stringBuilder.append(lVertices.get(i).getPropertyValue("X"));
//			stringBuilder.append(";");
//			stringBuilder.append(lVertices.get(i).getPropertyValue("Y"));
//			stringBuilder.append(";");
//			stringBuilder.append(lVertices.get(i).getPropertyValue("degree"));
//			stringBuilder.append(";");
//			stringBuilder.append(vertexZoomLevel);
//			stringBuilder.append(";");
//			stringBuilder.append(lVertices.get(i).getId().toString());
//			stringBuilder.append(";");
//			stringBuilder.append(i);
//			stringBuilder.append(";");
//			stringBuilder.append(lVertices.get(i).getLabel());
//			stringBuilder.append(";");
//			stringBuilder.append(lVertices.get(i).getPropertyValue("X"));
//			stringBuilder.append(";");
//			stringBuilder.append(lVertices.get(i).getPropertyValue("Y"));
//			stringBuilder.append(";");
//			stringBuilder.append(lVertices.get(i).getPropertyValue("degree"));
//			stringBuilder.append(";");
//			stringBuilder.append(vertexZoomLevel);
//			stringBuilder.append(";");
//			stringBuilder.append("identityEdge");
//			stringBuilder.append(";");
//			stringBuilder.append("identityEdge");
//			stringBuilder.append("\n");
//			verticesWriter.write(stringBuilder.toString());
//		}
//		verticesWriter.close();
//		for (EPGMEdge edge : lEdges) {
//			for (EPGMVertex sourceVertex : lVertices) {
//				for (EPGMVertex targetVertex : lVertices) {
//					GradoopId edgeSourceId = edge.getSourceId();
//					if ((edgeSourceId.equals(sourceVertex.getId())) && (edge.getTargetId().equals(targetVertex.getId()))) {
//						EdgeGVD edgeCustom = new EdgeGVD(edge.getId().toString(), edge.getLabel(), edgeSourceId.toString(), edge.getTargetId().toString());
//						VertexGVD sourceVertexCustom = new VertexGVD(sourceVertex.getId().toString(), sourceVertex.getLabel(), 
//								vertexIdMap.get(sourceVertex.getId().toString()).get("numericId"), 
//								sourceVertex.getPropertyValue("X").getInt(), sourceVertex.getPropertyValue("Y").getInt(),
//								sourceVertex.getPropertyValue("degree").getLong(),
//								vertexIdMap.get(sourceVertex.getId().toString()).get("zoomLevel"));
//						VertexGVD targetVertexCustom = new VertexGVD(targetVertex.getId().toString(), targetVertex.getLabel(),
//								vertexIdMap.get(targetVertex.getId().toString()).get("numericId"),
//								targetVertex.getPropertyValue("X").getInt(), targetVertex.getPropertyValue("Y").getInt(),
//								targetVertex.getPropertyValue("degree").getLong(),
//								vertexIdMap.get(sourceVertex.getId().toString()).get("zoomLevel"));
//						lVVEdgeWrapper.add(new WrapperGVD(sourceVertexCustom, targetVertexCustom, edgeCustom));
//					}
//				}
//			}
//		}
//		lVVEdgeWrapper.sort(new WrapperDegreeComparator());
//		File wrapperFile = new File(outPath + "_wrappers");
//		wrapperFile.createNewFile();
//		PrintWriter wrapperWriter = new PrintWriter(wrapperFile);
//		for (WrapperGVD wrapper : lVVEdgeWrapper) {
//			StringBuilder stringBuilder = new StringBuilder();
//			stringBuilder.append(lGraphHead.get(0).getId());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getSourceIdGradoop());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getSourceIdNumeric());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getSourceLabel());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getSourceX());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getSourceY());	
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getSourceDegree());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getSourceZoomLevel());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getTargetIdGradoop());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getTargetIdNumeric());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getTargetLabel());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getTargetX());			
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getTargetY());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getTargetDegree());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getTargetZoomLevel());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getEdgeIdGradoop());
//			stringBuilder.append(";");
//			stringBuilder.append(wrapper.getEdgeLabel());
//			stringBuilder.append("\n");
//			wrapperWriter.write(stringBuilder.toString());
//		}
//		wrapperWriter.close();
//		File adjacencyFile = new File(outPath + "_adjacency");
//		adjacencyFile.createNewFile();
//		PrintWriter adjacencyWriter = new PrintWriter(adjacencyFile);
//		for (int i = 0; i < lVertices.size(); i++) 	{
//			StringBuilder stringBuilder = new StringBuilder();
//			GradoopId vId1 = lVertices.get(i).getId();
//			stringBuilder.append(vId1);
//			stringBuilder.append(";");
//			for (int j = 0; j < lVertices.size(); j++) 	{
//				boolean incident = false;
//				EPGMEdge connectingEdge = null;
//				GradoopId vId2 = lVertices.get(j).getId();
//				for (int k = 0; k < lEdges.size(); k++) 	{
//					EPGMEdge edge = lEdges.get(k);
//					GradoopId sourceId = edge.getSourceId();
//					GradoopId targetId = edge.getTargetId();
//					if (sourceId.equals(vId1) && targetId.equals(vId2) || sourceId.equals(vId2) && targetId.equals(vId1)) {
//						incident = true;
//						connectingEdge = edge;
//					}
//				}
//				if (incident) {
//					stringBuilder.append(vId2 + "," + connectingEdge.getId());
//					stringBuilder.append(";");
//				} 
//			}
//			stringBuilder.substring(0, stringBuilder.length() - 1);
//			stringBuilder.append("\n");
//			adjacencyWriter.write(stringBuilder.toString());
//		}
//		adjacencyWriter.close();
	}
}
