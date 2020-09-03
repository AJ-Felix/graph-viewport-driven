package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class AdjacencyGraphUtil implements GraphUtil{
	private StreamExecutionEnvironment fsEnv;
	private String inPath;
	private DataStreamSource<Row> vertexStream = null;
	private Map<String,Map<String,String>> adjMatrix;
	private Map<String,Row> wrapperMap;
	private Map<String,Row> vertexMap;
	private Set<String> visualizedWrappers;
	private Set<String> visualizedVertices;
	
	public AdjacencyGraphUtil(StreamExecutionEnvironment fsEnv, String inPath) {
		this.fsEnv = fsEnv;
		this.inPath = inPath;
	}

	@Override
	public void initializeStreams() {
		Path verticesFilePath = Path.fromLocalFile(new File(this.inPath + "_vertices"));
		RowCsvInputFormat verticesFormat = new RowCsvInputFormat(verticesFilePath, new TypeInformation[] {Types.STRING, Types.STRING, Types.INT, Types.STRING, 
				Types.INT, Types.INT, Types.LONG});
		verticesFormat.setFieldDelimiter(";");
		this.vertexStream = this.fsEnv.readFile(verticesFormat, this.inPath + "_vertices").setParallelism(1);
	}
	
	@Override
	public DataStream<Row> getVertexStream() {
		return this.vertexStream;
	}

	@Override
	public void setVisualizedVertices(Set<String> visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}

	@Override
	public void setVisualizedWrappers(Set<String> visualizedWrappers) {
		this.visualizedWrappers = visualizedWrappers;
	}
	
	@Override
	public DataStream<Row> zoom(Float topModel, Float rightModel, Float bottomModel, Float leftModel) throws IOException {
		DataStream<Row> vertexStreamInner = this.getVertexStream().filter(new VertexFilterInner(topModel, rightModel, bottomModel, leftModel));
		Map<String, Map<String, String>> adjMatrix = this.buildAdjacencyMatrix();
		Map<String, Row> vertexMap = this.buildVertexMap();
		
		//produce NonIdentity Wrapper Stream
		DataStream<String> wrapperKeys = vertexStreamInner.flatMap(new FlatMapFunction<Row,String>(){
			@Override
			public void flatMap(Row value, Collector<String> out) throws Exception {
				String sourceId = (String) value.getField(1);
				Map<String,String> map = adjMatrix.get(sourceId);
				for (Map.Entry<String, String> entry : map.entrySet()) {
					String targetId = entry.getKey();
					Row targetVertex = vertexMap.get(targetId);
					Integer targetX = (Integer) targetVertex.getField(4);
					Integer targetY = (Integer) targetVertex.getField(5);
					if ((leftModel > targetX) ||  (targetX > rightModel) || (topModel > targetY) || (targetY > bottomModel)) {
						out.collect(entry.getValue());
					} else {
						if (sourceId.compareTo(targetId) < 0) {
							out.collect(entry.getValue());
						}
					}
				}
			}
		});
		DataStream<Row> nonIdentityWrapper = wrapperKeys.map(new WrapperIDMapWrapper(this.wrapperMap));
		nonIdentityWrapper = nonIdentityWrapper.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertexStreamInner.map(new VertexMapIdentityWrapper());
		Set<String> visualizedVertices = this.visualizedVertices;
		identityWrapper = identityWrapper.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !(visualizedVertices.contains(value.getField(2).toString()));
			}
		});
		
		return nonIdentityWrapper.union(identityWrapper);
	}
	
	@Override
	public DataStream<Row> pan(Float topOld, Float rightOld, Float bottomOld, Float leftOld, Float xModelDiff, Float yModelDiff) throws IOException{
		Float topNew = topOld + yModelDiff;
		Float rightNew = rightOld + xModelDiff;
		Float bottomNew = bottomOld + yModelDiff;
		Float leftNew = leftOld + xModelDiff;
		DataStream<Row> vertexStreamInnerNewNotOld = this.vertexStream
			.filter(new VertexFilterInnerNewNotOld(leftNew, rightNew, topNew, bottomNew, leftOld, rightOld, topOld, bottomOld));
		Map<String, Map<String, String>> adjMatrix = this.adjMatrix;
		Map<String, Row> vertexMap = this.vertexMap;

		//produce NonIdentity Wrapper Stream
		DataStream<String> wrapperKeysDefNotVis = vertexStreamInnerNewNotOld.flatMap(new FlatMapFunction<Row,String>(){
			@Override
			public void flatMap(Row value, Collector<String> out) throws Exception {			
				String sourceId = (String) value.getField(1);
				Map<String,String> map = adjMatrix.get(sourceId);
				for (Map.Entry<String, String> entry : map.entrySet()) {
					String targetId = entry.getKey();
					Row targetVertex = vertexMap.get(targetId);
					Integer targetX = (Integer) targetVertex.getField(4);
					Integer targetY = (Integer) targetVertex.getField(5);
					if (((leftOld > targetX) || (targetX > rightOld) || (topOld > targetY) || (targetY > bottomOld)) && 
							((leftNew > targetX) || (targetX > rightNew) || (topNew > targetY) || (targetY > bottomNew))) {
						out.collect(entry.getValue());
					} else {
						if (((leftOld > targetX) || (targetX > rightOld) || (topOld > targetY) || (targetY > bottomOld)) 
								&& (sourceId.compareTo(targetId) < 0)) {
							out.collect(entry.getValue());
						}
					}
				}
			}
		});
		DataStream<Row> wrapperDefNotVis = wrapperKeysDefNotVis.map(new WrapperIDMapWrapper(this.wrapperMap));
		DataStream<Row> vertexStreamOldInnerNotNewInner = this.vertexStream
				.filter(new VertexFilterInnerOldNotNew(leftNew, rightNew, topNew, bottomNew, leftOld, rightOld, topOld, bottomOld));
		DataStream<String> wrapperKeysMaybeVis = vertexStreamOldInnerNotNewInner.flatMap(new FlatMapFunction<Row,String>(){
			@Override
			public void flatMap(Row value, Collector<String> out) throws Exception {			
				String sourceId = (String) value.getField(1);
				Map<String,String> map = adjMatrix.get(sourceId);
				for (Map.Entry<String, String> entry : map.entrySet()) {
					String targetId = entry.getKey();
					Row targetVertex = vertexMap.get(targetId);
					Integer targetX = (Integer) targetVertex.getField(4);
					Integer targetY = (Integer) targetVertex.getField(5);
					if ((leftNew <= targetX) &&  (targetX <= rightNew) && (topNew <= targetY) && (targetY <= bottomNew)) {
						out.collect(entry.getValue());
					} 
				}
			}
		});
		DataStream<Row> wrapperMaybeVis = wrapperKeysMaybeVis.map(new WrapperIDMapWrapper(this.wrapperMap));
		wrapperMaybeVis = wrapperMaybeVis.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertexStreamInnerNewNotOld.map(new VertexMapIdentityWrapper());
		return wrapperMaybeVis.union(wrapperDefNotVis).union(identityWrapper);
	}
	
	public DataStream<Row> getMaxDegreeSubset(Integer numberVertices) throws IOException{
		DataStream<Row> vertices = this.vertexStream.filter(new VertexFilterMaxDegree(numberVertices));
		Map<String, Map<String, String>> adjMatrix = this.buildAdjacencyMatrix();
		this.buildWrapperMap();
		Map<String, Row> vertexMap = this.buildVertexMap();
		
		//produce NonIdentity Wrapper Stream
		DataStream<String> wrapperKeys = vertices.flatMap(new FlatMapFunction<Row,String>(){
			@Override
			public void flatMap(Row value, Collector<String> out) throws Exception {
				String sourceId = (String) value.getField(1);
				Map<String,String> map = adjMatrix.get(sourceId);
				for (Map.Entry<String, String> entry : map.entrySet()) {
					String targetId = entry.getKey();
					Row targetVertex = vertexMap.get(targetId);
					if ((Integer) targetVertex.getField(2) < numberVertices) {
						out.collect(entry.getValue());
					} 
				}
			}
		});
		DataStream<Row> nonIdentityWrapper = wrapperKeys.map(new WrapperIDMapWrapper(this.wrapperMap));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertices.map(new VertexMapIdentityWrapper());
		return nonIdentityWrapper.union(identityWrapper);
	}
	
	public Map<String,Map<String,String>> buildAdjacencyMatrix() throws IOException {
		this.adjMatrix = new HashMap<String, Map<String,String>>();
		BufferedReader csvReader = new BufferedReader(new FileReader(this.inPath + "_adjacency"));
		String row;
		while ((row = csvReader.readLine()) != null) {
		    String[] arr = row.split(";");
		    String vertexIdRow = arr[0];
		    String[] vertexRows = Arrays.copyOfRange(arr, 1, arr.length);
		    Map<String,String> map = new HashMap<String,String>();
		    for (String column: vertexRows) {
		    	String[] entry = column.split(","); 
		    	map.put(entry[0], entry[1]);
		    }
		    this.adjMatrix.put(vertexIdRow, map);
		}
		csvReader.close();
		return this.adjMatrix;
	}
	
	public Map<String,Row> buildWrapperMap() throws NumberFormatException, IOException {
		this.wrapperMap = new HashMap<String,Row>();
		BufferedReader csvReader = new BufferedReader(new FileReader(this.inPath + "_wrappers"));
		String row;
		int i = 0;
		while ((row = csvReader.readLine()) != null) {
			i += 1;
			System.out.println(i);
			System.out.println(this.wrapperMap.size());
		    String[] arr = row.split(";");
		    Row wrapper = Row.of(arr[0], arr[1], Integer.parseInt(arr[2]), arr[3], Integer.parseInt(arr[4]), Integer.parseInt(arr[5]), Long.parseLong(arr[6]),
		    		arr[7], Integer.parseInt(arr[8]), arr[9], Integer.parseInt(arr[10]), Integer.parseInt(arr[11]), Long.parseLong(arr[12]), arr[13], arr[14]);
		    this.wrapperMap.put(arr[13], wrapper);
		}
		csvReader.close();
		System.out.println("wrapperMap size");
		System.out.println(wrapperMap.size());
		return this.wrapperMap;
	}
	
	public Map<String,Row> buildVertexMap() throws NumberFormatException, IOException {
		this.vertexMap = new HashMap<String,Row>();
		BufferedReader csvReader = new BufferedReader(new FileReader(this.inPath + "_vertices"));
		String row;
		while ((row = csvReader.readLine()) != null) {
		    String[] arr = row.split(";");
		    Row vertex = Row.of(arr[0], arr[1], Integer.parseInt(arr[2]), arr[3], Integer.parseInt(arr[4]), Integer.parseInt(arr[5]), Long.parseLong(arr[6]));
		    this.vertexMap.put(arr[1], vertex);
		}
		csvReader.close();
		return this.vertexMap;
	}
}
