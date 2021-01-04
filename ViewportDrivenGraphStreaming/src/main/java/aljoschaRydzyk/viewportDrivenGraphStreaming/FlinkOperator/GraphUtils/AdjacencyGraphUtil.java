package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.GroupCombineOperator;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Partition.StringMapAdjacencyMatrix;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Partition.VertexStringIDPartitioner;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Partition.WrapperMapAddPartitionKey;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Partition.WrapperMapPartitioner;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInner;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInnerNewNotOld;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInnerOldNotNew;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInnerNewNotOld;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInside;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsVisualized;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterMaxDegree;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotLayouted;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotVisualized;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterZoomLevel;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFlatMapWrapperBi;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFlatMapWrapperUni;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexMapIdentityWrapperRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.Adjacency.VertexFlatMapMaxDegree;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.Adjacency.VertexFlatMapPanDefNotVis;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.Adjacency.VertexFlatMapPanMaybeVis;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.Adjacency.VertexFlatMapZoom;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperDirectionTupleMapWrapper;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterIsLayoutedInnerNewNotOldReverse;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterIsLayoutedInnerNewNotOldTrue;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterIsLayoutedInnerOldNotNewReverse;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterIsLayoutedInnerOldNotNewTrue;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterIsLayoutedInsideReverse;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterIsLayoutedInsideTrue;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterIsLayoutedOutsideReverse;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterIsLayoutedOutsideTrue;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterNotLayoutedReverse;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterNotLayoutedTrue;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterNotVisualizedReverse;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterNotVisualizedTrue;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterReverseDirection;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterTrueDirection;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterVisualizedWrappers;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterZoomLevelReverse;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterZoomLevelTrue;

public class AdjacencyGraphUtil implements GraphUtilStream{
	private StreamExecutionEnvironment fsEnv;
	private ExecutionEnvironment env;
	private String inPath;
	private DataStreamSource<Row> vertexStream = null;
	private Map<String,Map<String,String>> adjMatrix;
	private Map<String,Row> wrapperMap;
	private Set<String> visualizedWrappers;
	private Set<String> visualizedVertices;
	private FilterFunction<Row> zoomOutVertexFilter;
	private int zoomLevel;
	private DataSet<Tuple2<BigInteger, Row>> wrapperMapPart;
	private DataSet<Tuple2<String, Map<String, String>>> adjMatrixPart;

	
	public AdjacencyGraphUtil(StreamExecutionEnvironment fsEnv, ExecutionEnvironment env, String inPath) {
		this.fsEnv = fsEnv;
		this.inPath = inPath;
		this.env = env;
	}

	@Override
	public void initializeDataSets() {
		Path verticesFilePath = Path.fromLocalFile(new File(this.inPath + "_vertices"));
		RowCsvInputFormat verticesFormat = new RowCsvInputFormat(verticesFilePath, new TypeInformation[] {
				Types.STRING, Types.STRING, Types.LONG, Types.STRING, 
				Types.INT, Types.INT, Types.LONG, Types.INT});
		verticesFormat.setFieldDelimiter(";");
		this.vertexStream = this.fsEnv.readFile(verticesFormat, this.inPath + "_vertices").setParallelism(1);
		try {
			this.wrapperMap = this.buildWrapperMap();
			this.adjMatrix = this.buildAdjacencyMatrix();
			
			//partition
//			this.buildWrapperMapStream();
			this.buildWrapperMapPart();
			this.buildAdjacencyMatrixPart();
			
//			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
//		System.out.println("adjMatrix: " + this.adjMatrix);
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
	public DataStream<Row> getMaxDegreeSubset(int numberVertices) throws IOException{
		DataStream<Row> vertices = this.vertexStream
				.filter(new VertexFilterMaxDegree(numberVertices))
				.filter(new VertexFilterZoomLevel(zoomLevel));
		Map<String, Map<String, String>> adjMatrix = this.adjMatrix;
		Map<String, Row> wrapperMap = this.wrapperMap;
		
		//produce NonIdentity Wrapper Stream
		DataStream<Row> nonIdentityWrapper = vertices.flatMap(new VertexFlatMapMaxDegree(adjMatrix,
				wrapperMap, numberVertices));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertices.map(new VertexMapIdentityWrapperRow());
		return nonIdentityWrapper.union(identityWrapper);
	}
	
	@Override
	public DataStream<Row> zoom(Float top, Float right, Float bottom, Float left) {
		DataStream<Row> vertexStreamInner = this.vertexStream
				.partitionCustom(new VertexStringIDPartitioner(), new KeySelector<Row,String>(){

					@Override
					public String getKey(Row value) throws Exception {
						return value.getField(1).toString();
					}
					
				})
				.keyBy(new KeySelector<Row,Integer>(){
					@Override
					public Integer getKey(Row value) throws Exception {
						 
						BigInteger vertexBigIntegerId = new BigInteger(value.getField(1).toString(), 16);
						Integer partition = vertexBigIntegerId.remainder(new BigInteger("4", 10)).intValue();
						return partition;
					}
					
				})
				.filter(new VertexFilterInner(top, right, bottom, left))
				.filter(new VertexFilterZoomLevel(zoomLevel))
		.filter(new FilterFunction<Row>() {

			@Override
			public boolean filter(Row value) throws Exception {
				BigInteger vertexIdBigInteger = new BigInteger(value.getField(1).toString(), 16);
				Integer partition = vertexIdBigInteger.remainder(new BigInteger("4", 10)).intValue();
				System.out.println("Vertex ID hex string, BigInteger, partition: " + value.getField(1) + ", " + vertexIdBigInteger + ", " + partition);
				return true;
			}
			
		});
//		Map<String, Map<String, String>> adjMatrix = this.adjMatrix;
//		Map<String, Row> wrapperMap = this.wrapperMap;
		
		List<Map<String, Map<String, String>>> casedAdjacencyMatrix = null;
		DataSet<Map<String,Map<String,String>>> casedAdjacencyMatrixDataSet = null;
		try {
			casedAdjacencyMatrixDataSet = this.adjMatrixPart.groupBy(new KeySelector<Tuple2<String,Map<String,String>>, Integer>(){

				@Override
				public Integer getKey(Tuple2<String, Map<String, String>> value) throws Exception {
					BigInteger gradoopIntegerId = new BigInteger(value.f0, 16);
					Integer partitionKey = gradoopIntegerId.remainder(new BigInteger(String.valueOf(4), 10)).intValue();
					System.out.println("partitionKey in groupBy: " + partitionKey);
					return partitionKey;
				}
			}).combineGroup(new GroupCombineFunction<Tuple2<String,Map<String,String>>, Map<String,Map<String,String>>>(){

				@Override
				public void combine(Iterable<Tuple2<String, Map<String, String>>> values,
						Collector<Map<String, Map<String, String>>> out) throws Exception {
					Map<String,Map<String,String>> adjacencyMatrix = new HashMap<String,Map<String,String>>();
					for (Tuple2<String,Map<String,String>> entry : values) {
						adjacencyMatrix.put(entry.f0, entry.f1);
						System.out.println("adjEntry in combine: " + entry);
						BigInteger gradoopIntegerId = new BigInteger(entry.f0, 16);
						Integer partitionKey = gradoopIntegerId.remainder(new BigInteger(String.valueOf(4), 10)).intValue();
						System.out.println("partitionKey in combine: " + partitionKey);
					}
					System.out.println("in adjMatrix combineGroup, adjMatrix size: " + adjacencyMatrix.size());
					out.collect(adjacencyMatrix);
				}
			});
			casedAdjacencyMatrix = casedAdjacencyMatrixDataSet.collect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			env.execute();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		System.out.println("casedAdjacencyMatrix, length: " + casedAdjacencyMatrix.size());
		
		Map<String, Map<String, String>> adjMatrix = casedAdjacencyMatrix.get(0);
		System.out.println("adjacencyMatrixPart, zoom, size: " + adjMatrix.size());
		
		List<Map<String,Row>> casedWrapperMap = null;
		try {
			casedWrapperMap = this.wrapperMapPart.groupBy(new KeySelector<Tuple2<BigInteger,Row>, Integer>(){

				@Override
				public Integer getKey(Tuple2<BigInteger, Row> value) throws Exception {
					Integer partitionKey = value.f0.remainder(new BigInteger(String.valueOf(4), 10)).intValue();
					System.out.println("wrapperMap, partitionKey in groupBy: " + partitionKey);
					return partitionKey;
				}
			}).combineGroup(new GroupCombineFunction<Tuple2<BigInteger,Row>, Map<String,Row>>(){

					@Override
					public void combine(Iterable<Tuple2<BigInteger,Row>> values,
							Collector<Map<String, Row>> out) throws Exception {
						Map<String,Row> wrapperMap = new HashMap<String,Row>();
						for (Tuple2<BigInteger,Row> entry : values) {
							System.out.println("wrapperMap in combine: " + entry);
							wrapperMap.put(entry.f1.getField(15).toString(), entry.f1);
							Integer partitionKey = entry.f0.remainder(new BigInteger(String.valueOf(4), 10)).intValue();
							System.out.println("wrapperMap, partitionKey in combine: " + partitionKey);
						}
						out.collect(wrapperMap);
					}
				}).collect();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		try {
//			env.execute();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		Map<String,Row> wrapperMap = casedWrapperMap.get(0);
		for (String key : casedAdjacencyMatrix.get(0).keySet()) System.out.println("adjacencyMatrixPart: " + key);
		for (Row row : casedWrapperMap.get(0).values()) System.out.println("wrapperMapPart: " + row.getField(15));

		//with partitioning
//		DataSet<Tuple4<Long,String, String, String>> partitioned = this.adjMatrixPart.map(new MapFunction<Tuple3<String,String,String>,
//				Tuple4<Long,String,String,String>>(){
//					@Override
//					public Tuple4<Long, String, String, String> map(Tuple3<String, String, String> value)
//							throws Exception {
//						long partition = Math.round(Math.random() * 3.5);
//						Tuple4<Long, String, String, String> tuple = Tuple4.of(partition, value.f0, value.f1, value.f2);
//						return tuple;
//					}
//		});
//		partitioned = partitioned.filter(new FilterFunction<Tuple4<Long,String,String,String>>(){
//
//			@Override
//			public boolean filter(Tuple4<Long,String, String, String> value) throws Exception {
//				BigInteger integerID = new BigInteger(value.f1, 16);
//				BigInteger remainder = integerID.remainder(new BigInteger("4", 10));
//				System.out.println("partitioned filter: " + value);
//				System.out.println("integerID: " + integerID);
//				System.out.println("remainder: " + remainder);
//				return true;
//			}
//			
//		});
//		try {
//			partitioned.print();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		//partition wrapper map
//		DataSet<Tuple18<
//			Long, 
//			String, 
//			String, Long, String, Integer, Integer, Long, Integer, 
//			String, Long, String, Integer, Integer, Long, Integer, 
//			String, String>> 
//				partitionedWrapperMap = this.wrapperMapPart.partitionByHash(0);
//		
//		partitionedWrapperMap = partitionedWrapperMap.filter(new FilterFunction<Tuple18<
//				Long, 
//				String, 
//				String, Long, String, Integer, Integer, Long, Integer, 
//				String, Long, String, Integer, Integer, Long, Integer, 
//				String, String>>() {
//
//					@Override
//					public boolean filter(
//							Tuple18<Long, String, String, Long, String, Integer, Integer, Long, Integer, String, Long, String, Integer, Integer, Long, Integer, String, String> value)
//							throws Exception {
//						System.out.println("partitioned filter wrapper map: " + value);
//						return true;
//					}
//			
//		});
//		try {
//			partitionedWrapperMap.print();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		//produce NonIdentity Wrapper Stream
		DataStream<Row> nonIdentityWrapper = vertexStreamInner.flatMap(new VertexFlatMapZoom(
				adjMatrix, wrapperMap, top, right, bottom, left));
		nonIdentityWrapper = nonIdentityWrapper.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));

		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertexStreamInner.map(new VertexMapIdentityWrapperRow());
		identityWrapper = identityWrapper.filter(new VertexFilterNotVisualized(this.visualizedVertices));
		return nonIdentityWrapper.union(identityWrapper);
	}
	
	@Override
	public DataStream<Row> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld,
			Float leftOld) {
		
		//zoomLevel
		DataStream<Row> vertices = this.vertexStream.filter(new VertexFilterZoomLevel(zoomLevel));
		
		DataStream<Row> vertexStreamInnerNewNotOld = vertices
			.filter(new VertexFilterInnerNewNotOld(leftNew, rightNew, topNew, bottomNew, leftOld, rightOld, topOld, bottomOld));
		Map<String, Map<String, String>> adjMatrix = this.adjMatrix;
		Map<String, Row> wrapperMap = this.wrapperMap;

		//produce NonIdentity Wrapper Stream
		DataStream<Row> wrapperDefNotVis = vertexStreamInnerNewNotOld.flatMap(
				new VertexFlatMapPanDefNotVis(adjMatrix, wrapperMap, topNew, rightNew, bottomNew, leftNew,
						topOld, rightOld, bottomOld, leftOld));
		DataStream<Row> vertexStreamOldInnerNotNewInner = vertices
				.filter(new VertexFilterInnerOldNotNew(leftNew, rightNew, topNew, bottomNew, leftOld, rightOld, topOld, bottomOld));
		DataStream<Row> wrapperMaybeVis = vertexStreamOldInnerNotNewInner.flatMap(
				new VertexFlatMapPanMaybeVis(adjMatrix, wrapperMap, topNew,  rightNew,  bottomNew, leftNew));
		wrapperMaybeVis = wrapperMaybeVis.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertexStreamInnerNewNotOld.map(new VertexMapIdentityWrapperRow());
		return wrapperMaybeVis.union(wrapperDefNotVis).union(identityWrapper);
	}
	
	public Map<String,Map<String,String>> buildAdjacencyMatrix() throws Exception {
		this.adjMatrix = new HashMap<String, Map<String,String>>();
		DataSet<String> lineSet = env.readTextFile(this.inPath + "_adjacency");
		List<String> lineList = null;
		lineList = lineSet.collect();
		for (int i = 0; i < lineList.toArray().length; i++ ) {
			String line = lineList.get(i);
			String[] cols = line.split(";");
			String sourceId = cols[0];
			cols = Arrays.copyOfRange(cols, 1, cols.length);
			Map<String,String> map = new HashMap<String,String>();
			for (String col : cols) {
				String[] entry = col.split(",");
				map.put(entry[0], entry[1]);
			}
			this.adjMatrix.put(sourceId, map);
		}
		return this.adjMatrix;
	}
	
	public Map<String,Row> buildWrapperMap() throws Exception {
		this.wrapperMap = new HashMap<String,Row>();
		CsvReader reader = env.readCsvFile(this.inPath + "_wrappers");
		reader.fieldDelimiter(";");
		DataSet<Tuple17<
			String,
			String,Long,String,Integer,Integer,Long,Integer,
			String,Long,String,Integer,Integer,Long,Integer,
			String,String>> source = reader.types(
					String.class, 
					String.class, Long.class, String.class, Integer.class, Integer.class, Long.class, Integer.class,
					String.class, Long.class, String.class, Integer.class, Integer.class, Long.class, Integer.class,
					String.class, String.class);
		List<Tuple17<
			String,
			String,Long,String,Integer,Integer,Long,Integer,
			String,Long,String,Integer,Integer,Long,Integer,
			String,String>> list = null;
		list = source.collect();
		for (int i = 0; i < list.toArray().length; i++ ) {
			Tuple17<String,
					String,Long,String,Integer,Integer,Long,Integer,
					String,Long,String,Integer,Integer,Long,Integer,
					String,String> tuple = list.get(i);
			wrapperMap.put(tuple.f15, Row.of(tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f4, tuple.f5, tuple.f6,
					tuple.f7, tuple.f8, tuple.f9, tuple.f10, tuple.f11, tuple.f12, tuple.f13, tuple.f14, tuple.f15,
					tuple.f16));
		}
//		for (Row row : this.wrapperMap.values()) System.out.println("wrapperMap: " + row);
		DataSet<Tuple2<String,Map<String,String>>> var;
		return this.wrapperMap;
	}
	
	/*
	 * General workflow for this GraphUtil:
	 * 		-	produce a vertex stream
	 * 		- 	flatMap and produce a stream of all relevant wrapperIDs using adjacency matrix
	 * 		- 	map wrapperIDstream to wrapperStream
	 */
	
	private void buildAdjacencyMatrixPart() {
//		CsvReader reader = env.readCsvFile(this.inPath + "_wrappers");
//		reader.fieldDelimiter(";");
//		DataSet<Tuple17<
//			String,
//			String,Long,String,Integer,Integer,Long,Integer,
//			String,Long,String,Integer,Integer,Long,Integer,
//			String,String>> source = reader.types(
//					String.class, 
//					String.class, Long.class, String.class, Integer.class, Integer.class, Long.class, Integer.class,
//					String.class, Long.class, String.class, Integer.class, Integer.class, Long.class, Integer.class,
//					String.class, String.class);
//		this.adjMatrixPart = source.map(new MapFunction<Tuple17<
//			String,
//			String,Long,String,Integer,Integer,Long,Integer,
//			String,Long,String,Integer,Integer,Long,Integer,
//			String,String>,Tuple3<String,String,String>>(){
//				@Override
//				public Tuple3<String, String, String> map(
//						Tuple17<String, String, Long, String, Integer, Integer, Long, Integer, String, Long, String, Integer, Integer, Long, Integer, String, String> value)
//						throws Exception {
//					return Tuple3.of(value.f1, value.f8, value.f15);
//				}		
//		});
		DataSet<String> stringReader = env.readTextFile(this.inPath + "_adjacency");
		DataSet<Tuple2<String,Map<String,String>>> adjacencyMatrix = stringReader.map(new StringMapAdjacencyMatrix());
		this.adjMatrixPart = adjacencyMatrix.partitionCustom(new VertexStringIDPartitioner(), 0);	
		try {
			System.out.println("adjMatrix, building, size: " + this.adjMatrixPart.count());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void buildWrapperMapPart() {
		CsvReader reader = env.readCsvFile(this.inPath + "_wrappers");
		reader.fieldDelimiter(";");
		DataSet<Tuple17<
			String,
			String,Long,String,Integer,Integer,Long,Integer,
			String,Long,String,Integer,Integer,Long,Integer,
			String,String>> 
		wrapperSource = reader.types(
				String.class, 
				String.class, Long.class, String.class, Integer.class, Integer.class, Long.class, Integer.class,
				String.class, Long.class, String.class, Integer.class, Integer.class, Long.class, Integer.class,
				String.class, String.class);
		DataSet<Tuple2<BigInteger, Row>> wrapperPartitionKeyed = wrapperSource.flatMap(new WrapperMapAddPartitionKey());
		this.wrapperMapPart = wrapperPartitionKeyed.partitionCustom(new WrapperMapPartitioner(), 0);
		
		//debug
//		try {
//			System.out.println("wrapperPartitionedKeyed, size: " + wrapperPartitionKeyed.count());
//
//			wrapperPartitionKeyed.print();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
	@Override
	public DataStream<Row> panZoomInLayoutStep1(Map<String,VertexGVD> layoutedVertices, Map<String,VertexGVD> innerVertices,
			Float top, Float right, Float bottom, Float left) {
		System.out.println("in panZoomInLayoutFirstStep");
		/*
		 * First substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that were
		 * layouted before and have their coordinates in the current model window but are not visualized yet.
		 */
		Set<String> innerVerticeskeySet = new HashSet<String>(innerVertices.keySet());
		DataStream<Row> vertices = this.vertexStream
				.filter(new VertexFilterIsLayoutedInside(layoutedVertices, top, right, bottom, left))
				.filter(new VertexFilterNotVisualized(innerVerticeskeySet))
				.filter(new VertexFilterZoomLevel(zoomLevel));
		DataStream<Tuple2<Boolean, Row>> wrapper = vertices
				.flatMap(new VertexFlatMapWrapperUni(adjMatrix, wrapperMap));
		DataStream<Row> wrapperTrue = wrapper
				.filter(new WrapperFilterTrueDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterNotVisualizedTrue(innerVerticeskeySet))
				.filter(new WrapperFilterIsLayoutedInsideTrue(layoutedVertices, top, right, bottom, left))
				.filter(new WrapperFilterZoomLevelTrue(zoomLevel));
		DataStream<Row> wrapperReverse = wrapper
				.filter(new WrapperFilterReverseDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterNotVisualizedReverse(innerVerticeskeySet))
				.filter(new WrapperFilterIsLayoutedInsideReverse(layoutedVertices, top, right, bottom, left))
				.filter(new WrapperFilterZoomLevelReverse(zoomLevel));
//		DataStream<String> wrapperIds = vertices
//				.flatMap(new VertexFlatMapNotVisualizedButLayoutedInsideUni(adjMatrix, layoutedVertices, innerVerticeskeySet, zoomLevel, top, right, 
//						bottom, left));
//		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertices.map(new VertexMapIdentityWrapperRow());
		
		return wrapperTrue.union(wrapperReverse).union(identityWrapper);
	}
	
	@Override
	public DataStream<Row> panZoomInLayoutStep2(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> unionMap){
		/*
		 * Second substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are not yet layouted on the
		 * other hand.
		 */
		System.out.println("in panZoomInLayoutSecondStep");
		
		Set<String> unionkeySet = new HashSet<String>(unionMap.keySet());
		Set<String> layoutedVerticeskeySet = new HashSet<String>(layoutedVertices.keySet());
		DataStream<Row> visualizedVertices = this.vertexStream.filter(new VertexFilterIsVisualized(unionkeySet));
		DataStream<Tuple2<Boolean, Row>> wrapper = visualizedVertices.flatMap(new VertexFlatMapWrapperBi(adjMatrix, wrapperMap));
		DataStream<Row> wrapperTrue = wrapper
				.filter(new WrapperFilterTrueDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterNotLayoutedTrue(layoutedVerticeskeySet))
				.filter(new WrapperFilterZoomLevelTrue(zoomLevel));
		DataStream<Row> wrapperReverse = wrapper
				.filter(new WrapperFilterReverseDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterNotLayoutedReverse(layoutedVerticeskeySet))
				.filter(new WrapperFilterZoomLevelReverse(zoomLevel));
		
//		DataStream<String> wrapperIds = visualizedVertices.flatMap(new VertexFlatMapNotLayoutedBi(adjMatrix, layoutedVerticeskeySet));
//		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		
		return wrapperTrue.union(wrapperReverse);
	}
	
	@Override
	public DataStream<Row> panZoomInLayoutStep3(Map<String, VertexGVD> layoutedVertices){		
		/*
		 * Third substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * not yet layouted starting with highest degree.
		 */
		System.out.println("in panZoomInLayoutThirdStep");
		
		Set<String> layoutedVerticesKeySet = new HashSet<String>(layoutedVertices.keySet());
		DataStream<Row> notLayoutedVertices = this.vertexStream
				.filter(new VertexFilterNotLayouted(layoutedVerticesKeySet))
				.filter(new VertexFilterZoomLevel(zoomLevel));
		
		DataStream<Tuple2<Boolean, Row>> wrapper = notLayoutedVertices
				.flatMap(new VertexFlatMapWrapperUni(adjMatrix, wrapperMap));
		DataStream<Row> wrapperTrue = wrapper
				.filter(new WrapperFilterTrueDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterNotLayoutedTrue(layoutedVerticesKeySet))
				.filter(new WrapperFilterZoomLevelTrue(zoomLevel));
		DataStream<Row> wrapperReverse = wrapper
				.filter(new WrapperFilterReverseDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterNotLayoutedReverse(layoutedVerticesKeySet))
				.filter(new WrapperFilterZoomLevelReverse(zoomLevel));
//		DataStream<String> wrapperIds = notLayoutedVertices.flatMap(new VertexFlatMapNotLayoutedUni(adjMatrix, layoutedVerticesKeySet));
//		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = notLayoutedVertices.map(new VertexMapIdentityWrapperRow());
//		return nonIdentityWrapper.union(identityWrapper);
		return wrapperTrue.union(wrapperReverse.union(identityWrapper));
	}
	
	@Override
	public DataStream<Row> zoomInLayoutStep4(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices, 
			Map<String, VertexGVD> newVertices, Float top, Float right, Float bottom, Float left){
		/*
		 * Fourth substep for zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		System.out.println("in ZoomInLayoutFourthStep");

		//unite maps of already visualized vertices before this zoom-in operation and vertices added in this zoom-in operation
		Map<String,VertexGVD> unionMap = new HashMap<String,VertexGVD>(innerVertices);
		unionMap.putAll(newVertices);
		
		Set<String> unionkeySet = new HashSet<String>(unionMap.keySet());
		DataStream<Row> visualizedVertices = this.vertexStream.filter(new VertexFilterIsVisualized(unionkeySet));
		
		DataStream<Tuple2<Boolean, Row>> wrapper = visualizedVertices.flatMap(new VertexFlatMapWrapperBi(adjMatrix, wrapperMap));
		DataStream<Row> wrapperTrue = wrapper
				.filter(new WrapperFilterTrueDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterIsLayoutedOutsideTrue(layoutedVertices, top, right, bottom, left))
				.filter(new WrapperFilterZoomLevelTrue(zoomLevel));
		DataStream<Row> wrapperReverse = wrapper
				.filter(new WrapperFilterReverseDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterIsLayoutedOutsideReverse(layoutedVertices, top, right, bottom, left))
				.filter(new WrapperFilterZoomLevelReverse(zoomLevel));
		
//		DataStream<String> wrapperIds = visualizedVerticesStream.flatMap(new VertexFlatMapIsLayoutedOutsideBi(layoutedVertices,
//				adjMatrix, top, right, bottom, left));
//		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));

		//filter out already visualized edges in wrapper stream
		DataStream<Row> nonIdentityWrapper = wrapperReverse.union(wrapperTrue).filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return nonIdentityWrapper;
	}
	
	@Override
	public DataStream<Row> panLayoutStep4(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices, 
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld, Float leftOld){
		/*
		 * Fourth substep for pan operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * newly visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		System.out.println("in panLayoutFourthStep");	
		
		//produce wrapper stream from C to D and vice versa
		Set<String> newVerticesKeySet = new HashSet<String>(newVertices.keySet());
		DataStream<Row> cVertices = this.vertexStream.filter(new VertexFilterIsVisualized(newVerticesKeySet))
				.filter(new VertexFilterIsLayoutedInside(layoutedVertices, topOld, rightOld, bottomOld, leftOld));
		DataStream<Tuple2<Boolean, Row>> wrapperC = cVertices.flatMap(new VertexFlatMapWrapperBi(adjMatrix, wrapperMap));
		DataStream<Row> wrapperTrueC = wrapperC
				.filter(new WrapperFilterTrueDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterIsLayoutedInnerOldNotNewTrue(layoutedVertices, topNew, rightNew, bottomNew, leftNew, topOld, rightOld, 
						bottomOld, leftOld));
		DataStream<Row> wrapperReverseC = wrapperC
				.filter(new WrapperFilterReverseDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterIsLayoutedInnerOldNotNewReverse(layoutedVertices, topNew, rightNew, bottomNew, leftNew, topOld, rightOld, 
						bottomOld, leftOld));

//		DataStream<String> wrapperIds = cVertices.flatMap(new VertexFlatMapIsLayoutedInnerOldNotNewBi(
//				layoutedVertices, adjMatrix, topNew, rightNew, bottomNew, leftNew, topOld, rightOld, 
//				bottomOld, leftOld));
		
		//produce wrapper stream from A to B+D and vice versa
		DataStream<Row> aVertices = this.vertexStream.filter(new VertexFilterIsVisualized(newVerticesKeySet))
				.filter(new VertexFilterIsLayoutedInnerNewNotOld(layoutedVertices,
						topNew, rightNew, bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld));
		DataStream<Tuple2<Boolean, Row>> wrapperA = aVertices.flatMap(new VertexFlatMapWrapperBi(adjMatrix, wrapperMap));
		DataStream<Row> wrapperTrueA = wrapperA
				.filter(new WrapperFilterTrueDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterIsLayoutedOutsideTrue(layoutedVertices, topNew, rightNew, bottomNew, leftNew));
		DataStream<Row> wrapperReverseA = wrapperA
				.filter(new WrapperFilterReverseDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterIsLayoutedOutsideReverse(layoutedVertices, topNew, rightNew, bottomNew, leftNew));
		
		DataStream<Row> wrapperTrue = wrapperTrueA.union(wrapperTrueC).filter(new WrapperFilterZoomLevelTrue(zoomLevel));
		DataStream<Row> wrapperReverse = wrapperReverseA.union(wrapperReverseC).filter(new WrapperFilterZoomLevelTrue(zoomLevel));

		
//		DataStream<String> wrapperIds2 = aVertices.flatMap(new VertexFlatMapIsLayoutedOutsideBi(
//				layoutedVertices, adjMatrix, topNew, rightNew, bottomNew, leftNew));
//		
//		DataStream<Row> nonIdentityWrapper = wrapperIds.union(wrapperIds2)
//				.map(new WrapperIDMapWrapper(this.wrapperMap));
		
		DataStream<Row> nonIdentityWrapper = wrapperTrue.union(wrapperReverse).filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		for (String wrapperId: this.visualizedWrappers) System.out.println("visualizedWrapper: " + wrapperId);
		nonIdentityWrapper.print();
		return nonIdentityWrapper;
	}
	
	@Override
	public DataStream<Row> zoomOutLayoutStep1(Map<String, VertexGVD> layoutedVertices, 
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, 
			Float topOld, Float rightOld, Float bottomOld, Float leftOld){
		/*
		 * First substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * layouted inside the model space which was added by operation.
		 */
		System.out.println("in zoomOutLayoutFirstStep");
		
		//IDENTITY WRAPPER NEEDED
		
		zoomOutVertexFilter = new VertexFilterIsLayoutedInnerNewNotOld(layoutedVertices, topNew, rightNew, bottomNew, 
				leftNew, topOld, rightOld, bottomOld, leftOld);
		DataStream<Row> vertices = this.vertexStream
				.filter(zoomOutVertexFilter)
				.filter(new VertexFilterZoomLevel(zoomLevel));
		
		DataStream<Tuple2<Boolean, Row>> wrapperC = vertices.flatMap(new VertexFlatMapWrapperUni(adjMatrix, wrapperMap));
		DataStream<Row> wrapperTrue = wrapperC
				.filter(new WrapperFilterTrueDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterIsLayoutedInnerNewNotOldTrue(layoutedVertices, topNew, rightNew, bottomNew, leftNew, topOld, rightOld, 
						bottomOld, leftOld))
				.filter(new WrapperFilterZoomLevelTrue(zoomLevel));
		DataStream<Row> wrapperReverse = wrapperC
				.filter(new WrapperFilterReverseDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterIsLayoutedInnerNewNotOldReverse(layoutedVertices, topNew, rightNew, bottomNew, leftNew, topOld, rightOld, 
						bottomOld, leftOld))
				.filter(new WrapperFilterZoomLevelReverse(zoomLevel));
		
//		DataStream<String> wrapperIds = vertices.flatMap(new VertexFlatMapIsLayoutedInnerNewNotOldUni(adjMatrix, layoutedVertices, 
//				topNew, rightNew, bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld));
//		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertices.map(new VertexMapIdentityWrapperRow());
		return wrapperTrue.union(wrapperReverse).union(identityWrapper);
	}
	
	@Override
	public DataStream<Row> zoomOutLayoutStep2(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices, 
			Float top, Float right, Float bottom, Float left){
		/*
		 * Second substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the model space which was added by operation on the one hand, neighbour vertices that are layouted with 
		 * coordinates outside the current model window on the other hand.
		 */
		System.out.println("in zoomOutLayoutSecondStep");
		
		Set<String> newVerticesKeySet = new HashSet<String>(newVertices.keySet());
		DataStream<Row> newlyVisualizedVertices = this.vertexStream
				.filter(new VertexFilterIsVisualized(newVerticesKeySet))
				.filter(zoomOutVertexFilter);
		
		DataStream<Tuple2<Boolean, Row>> wrapper = newlyVisualizedVertices.flatMap(new VertexFlatMapWrapperBi(adjMatrix, wrapperMap));
		DataStream<Row> wrapperTrue = wrapper
				.filter(new WrapperFilterTrueDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterIsLayoutedOutsideTrue(layoutedVertices, top, right, bottom, left))
				.filter(new WrapperFilterZoomLevelTrue(zoomLevel));
		DataStream<Row> wrapperReverse = wrapper
				.filter(new WrapperFilterReverseDirection())
				.map(new WrapperDirectionTupleMapWrapper())
				.filter(new WrapperFilterIsLayoutedOutsideReverse(layoutedVertices, top, right, bottom, left))
				.filter(new WrapperFilterZoomLevelReverse(zoomLevel));
		
//		DataStream<String> wrapperIds = newlyVisualizedVertices.flatMap(new VertexFlatMapIsLayoutedOutsideBi(layoutedVertices,
//				adjMatrix, top, right, bottom, left));
//		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		return wrapperTrue.union(wrapperReverse);
	}

	@Override
	public void setVertexZoomLevel(int zoomLevel) {
		this.zoomLevel = zoomLevel;
	}
}
