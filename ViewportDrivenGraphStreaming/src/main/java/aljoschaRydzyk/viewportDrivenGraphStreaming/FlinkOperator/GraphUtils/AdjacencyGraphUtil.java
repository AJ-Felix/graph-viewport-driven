package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple15;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInner;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInnerNewNotOld;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInnerOldNotNew;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInnerNewNotOld;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInside;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsVisualized;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterMaxDegree;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotInsideBefore;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotLayouted;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotVisualized;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFlatMapIsLayoutedInnerNewNotOldUni;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFlatMapIsLayoutedOutsideBi;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFlatMapNotLayoutedBi;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFlatMapNotLayoutedUni;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFlatMapNotVisualizedButLayoutedInsideUni;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexMapIdentityWrapperRow;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterVisualizedWrappers;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperIDMapWrapper;

public class AdjacencyGraphUtil implements GraphUtilStream{
	private StreamExecutionEnvironment fsEnv;
	private ExecutionEnvironment env;
	private String inPath;
	private DataStreamSource<Row> vertexStream = null;
	private Map<String,Map<String,String>> adjMatrix;
	private Map<String,Row> wrapperMap;
//	private Map<String,Row> vertexMap;
	private Set<String> visualizedWrappers;
	private Set<String> visualizedVertices;
	private FilterFunction<Row> zoomOutVertexFilter;

	
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
				Types.INT, Types.INT, Types.LONG});
		verticesFormat.setFieldDelimiter(";");
		this.vertexStream = this.fsEnv.readFile(verticesFormat, this.inPath + "_vertices").setParallelism(1);
		try {
//			this.vertexMap = this.buildVertexMap();
			this.wrapperMap = this.buildWrapperMap();
			this.adjMatrix = this.buildAdjacencyMatrix();
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
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
	
	public Map<String,Map<String,String>> getAdjMatrix(){
		return this.adjMatrix;
	}
	
	@Override
	public DataStream<Row> zoom(Float top, Float right, Float bottom, Float left) throws IOException {
		DataStream<Row> vertexStreamInner = this.getVertexStream().filter(new VertexFilterInner(top, right, bottom, left));
		Map<String, Map<String, String>> adjMatrix = this.adjMatrix;
		Map<String, Row> wrapperMap = this.wrapperMap;
		
		//produce NonIdentity Wrapper Stream
		DataStream<String> wrapperKeys = vertexStreamInner.flatMap(new FlatMapFunction<Row,String>(){
			@Override
			public void flatMap(Row value, Collector<String> out) throws Exception {
				String firstVertexId = (String) value.getField(1);
				Map<String,String> map = adjMatrix.get(firstVertexId);
				for (String wrapperId : map.values()) {
					Row wrapper = wrapperMap.get(wrapperId);
					String secondVertexId;
					int secondVertexX;
					int secondVertexY;
					if (firstVertexId.equals(wrapper.getField(1))) {
						secondVertexId = wrapper.getField(7).toString();
						secondVertexX = (int) wrapper.getField(10);
						secondVertexY = (int) wrapper.getField(11);
					} else {
						secondVertexId = wrapper.getField(1).toString();
						secondVertexX = (int) wrapper.getField(4);
						secondVertexY = (int) wrapper.getField(5);
					}
					if ((left > secondVertexX) ||  (secondVertexX > right) || (top > secondVertexY) || (secondVertexY > bottom)) {
						out.collect(wrapperId);
					} else {
						if (firstVertexId.compareTo(secondVertexId) < 0) {
							out.collect(wrapperId);
						}
					}
				}
			}
		});
		DataStream<Row> nonIdentityWrapper = wrapperKeys.map(new WrapperIDMapWrapper(this.wrapperMap));
		nonIdentityWrapper = nonIdentityWrapper.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertexStreamInner.map(new VertexMapIdentityWrapperRow());
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
	public DataStream<Row> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld,
			Float leftOld) {
		DataStream<Row> vertexStreamInnerNewNotOld = this.vertexStream
			.filter(new VertexFilterInnerNewNotOld(leftNew, rightNew, topNew, bottomNew, leftOld, rightOld, topOld, bottomOld));
		Map<String, Map<String, String>> adjMatrix = this.adjMatrix;
		Map<String, Row> wrapperMap = this.wrapperMap;

		//produce NonIdentity Wrapper Stream
		DataStream<String> wrapperKeysDefNotVis = vertexStreamInnerNewNotOld.flatMap(new FlatMapFunction<Row,String>(){
			@Override
			public void flatMap(Row value, Collector<String> out) throws Exception {			
				String firstVertexId = (String) value.getField(1);
				Map<String,String> map = adjMatrix.get(firstVertexId);
				for (String entry : map.values()) {
					Row wrapper = wrapperMap.get(entry);
					String secondVertexId;
					int secondVertexX;
					int secondVertexY;
					if (firstVertexId.equals(wrapper.getField(1))) {
						secondVertexId = wrapper.getField(7).toString();
						secondVertexX = (int) wrapper.getField(10);
						secondVertexY = (int) wrapper.getField(11);
					} else {
						secondVertexId = wrapper.getField(1).toString();
						secondVertexX = (int) wrapper.getField(4);
						secondVertexY = (int) wrapper.getField(5);
					}
					if (((leftOld > secondVertexX) || (secondVertexX > rightOld) || (topOld > secondVertexY) || (secondVertexY > bottomOld)) && 
							((leftNew > secondVertexX) || (secondVertexX > rightNew) || (topNew > secondVertexY) || (secondVertexY > bottomNew))) {
						out.collect(entry);
					} else {
						if (((leftOld > secondVertexX) || (secondVertexX > rightOld) || (topOld > secondVertexY) || (secondVertexY > bottomOld)) 
								&& (firstVertexId.compareTo(secondVertexId) < 0)) {
							out.collect(entry);
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
			public void flatMap(Row vertexId, Collector<String> out) throws Exception {			
				String firstVertexId = (String) vertexId.getField(1);
				Map<String,String> map = adjMatrix.get(firstVertexId);
				for (String wrapperId : map.values()) {
					int secondVertexX;
					int secondVertexY;
					Row wrapper = wrapperMap.get(wrapperId);
					if (firstVertexId.equals(wrapper.getField(1))) {
						secondVertexX = (int) wrapper.getField(10);
						secondVertexY = (int) wrapper.getField(11);
					} else {
						secondVertexX = (int) wrapper.getField(4);
						secondVertexY = (int) wrapper.getField(5);
					}
					if ((leftNew <= secondVertexX) &&  (secondVertexX <= rightNew) && (topNew <= secondVertexY) && (secondVertexY <= bottomNew)) {
						out.collect(wrapperId);
					} 
				}
			}
		});
		DataStream<Row> wrapperMaybeVis = wrapperKeysMaybeVis.map(new WrapperIDMapWrapper(this.wrapperMap));
		wrapperMaybeVis = wrapperMaybeVis.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertexStreamInnerNewNotOld.map(new VertexMapIdentityWrapperRow());
		return wrapperMaybeVis.union(wrapperDefNotVis).union(identityWrapper);
	}
	
	public DataStream<Row> getMaxDegreeSubset(Integer numberVertices) throws IOException{
		DataStream<Row> vertices = this.vertexStream.filter(new VertexFilterMaxDegree(numberVertices));
		Map<String, Map<String, String>> adjMatrix = this.adjMatrix;
		Map<String, Row> wrapperMap = this.wrapperMap;
		
		//produce NonIdentity Wrapper Stream
		DataStream<String> wrapperKeys = vertices.flatMap(new FlatMapFunction<Row,String>(){
			@Override
			public void flatMap(Row value, Collector<String> out) throws Exception {
				String firstVertexIdGradoop = (String) value.getField(1);
				Long firstVertexIdNumeric = (Long) value.getField(2);
				Map<String,String> map = adjMatrix.get(firstVertexIdGradoop);
				System.out.println("sourceID: " + firstVertexIdGradoop);
				for (String wrapperId : map.values()) {
					Row wrapper = wrapperMap.get(wrapperId);
					Long secondVertexIdNumeric;
					if (wrapper.getField(1).equals(firstVertexIdGradoop)) {
						secondVertexIdNumeric = (long) wrapper.getField(8);
					} else {
						secondVertexIdNumeric = (long) wrapper.getField(2);
					}
					if (secondVertexIdNumeric < numberVertices && firstVertexIdNumeric > secondVertexIdNumeric) {
						out.collect(wrapperId);
					} 
				}
			}
		});
		DataStream<Row> nonIdentityWrapper = wrapperKeys.map(new WrapperIDMapWrapper(this.wrapperMap));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertices.map(new VertexMapIdentityWrapperRow());
		return nonIdentityWrapper.union(identityWrapper);
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
		DataSet<Tuple15<String,String,Long,String,Integer,Integer,Long,String,Long,String,Integer,Integer,Long,
			String,String>> source = reader.types(
					String.class, String.class, Long.class, String.class, Integer.class, Integer.class, Long.class,
					String.class, Long.class, String.class, Integer.class, Integer.class, Long.class,
					String.class, String.class);
		List<Tuple15<String,String,Long,String,Integer,Integer,Long,String,Long,String,Integer,Integer,Long,
		String,String>> list = null;
		list = source.collect();
		for (int i = 0; i < list.toArray().length; i++ ) {
			Tuple15<String,String,Long,String,Integer,Integer,Long,String,Long,String,Integer,Integer,Long,
				String,String> tuple = list.get(i);
			wrapperMap.put(tuple.f13, Row.of(tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f4, tuple.f5, tuple.f6,
					tuple.f7, tuple.f8, tuple.f9, tuple.f10, tuple.f11, tuple.f12, tuple.f13, tuple.f14));
		}
		for (Row row : this.wrapperMap.values()) System.out.println("wrapperMap: " + row);
		return this.wrapperMap;
	}
	
//	public Map<String,Row> buildVertexMap() throws Exception {
//		this.vertexMap = new HashMap<String,Row>();
//		CsvReader reader = env.readCsvFile(this.inPath + "_vertices");
//		reader.fieldDelimiter(";");
//		DataSet<Tuple7<String,String,Long,String,Integer,Integer,Long>> source = reader.types(
//					String.class, String.class, Long.class, String.class, Integer.class, Integer.class, Long.class);
//		List<Tuple7<String,String,Long,String,Integer,Integer,Long>> list = null;
//		list = source.collect();
//		for (int i = 0; i < list.toArray().length; i++ ) {
//			Tuple7<String,String,Long,String,Integer,Integer,Long> tuple = list.get(i);
//			this.vertexMap.put(tuple.f1, Row.of(tuple.f0, tuple.f1, tuple.f2, tuple.f3, tuple.f4, tuple.f5, 
//					tuple.f6));
//		}
//		return this.vertexMap;
//	}
	
	/*
	 * General workflow for this GraphUtil:
	 * 		-	produce a vertex stream
	 * 		- 	flatMap and produce a stream of all relevant wrapperIDs using adjacency matrix
	 * 		- 	map wrapperIDstream to wrapperStream
	 */
	
	@Override
	public DataStream<Row> panZoomInLayoutStep1(Map<String,VertexGVD> layoutedVertices, Map<String,VertexGVD> innerVertices,
			Float top, Float right, Float bottom, Float left) {
		System.out.println("in panZoomInLayoutFirstStep");
		/*
		 * First substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that were
		 * layouted before and have their coordinates in the current model window but are not visualized yet.
		 */
		DataStream<Row> vertices = this.vertexStream
				.filter(new VertexFilterIsLayoutedInside(layoutedVertices, top, right, bottom, left))
				.filter(new VertexFilterNotVisualized(innerVertices));
		DataStream<String> wrapperIds = vertices
				.flatMap(new VertexFlatMapNotVisualizedButLayoutedInsideUni(adjMatrix, layoutedVertices, innerVertices, top, right, 
						bottom, left));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertices.map(new VertexMapIdentityWrapperRow());
		return nonIdentityWrapper.union(identityWrapper);
	}
	
	@Override
	public DataStream<Row> panZoomInLayoutStep2(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> unionMap){
		/*
		 * Second substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are not yet layouted on the
		 * other hand.
		 */
		System.out.println("in panZoomInLayoutSecondStep");

		DataStream<Row> visualizedVertices = this.vertexStream.filter(new VertexFilterIsVisualized(unionMap));
		DataStream<String> wrapperIds = visualizedVertices.flatMap(new VertexFlatMapNotLayoutedBi(adjMatrix, layoutedVertices));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		return nonIdentityWrapper;
	}
	
	@Override
	public DataStream<Row> panZoomInLayoutStep3(Map<String, VertexGVD> layoutedVertices){		
		/*
		 * Third substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * not yet layouted starting with highest degree.
		 */
		System.out.println("in panZoomInLayoutThirdStep");

		DataStream<Row> notLayoutedVertices = this.vertexStream.filter(new VertexFilterNotLayouted(layoutedVertices));
		DataStream<String> wrapperIds = notLayoutedVertices.flatMap(new VertexFlatMapNotLayoutedUni(adjMatrix, layoutedVertices));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = notLayoutedVertices.map(new VertexMapIdentityWrapperRow());
		return nonIdentityWrapper.union(identityWrapper);
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
		
		DataStream<Row> visualizedVerticesStream = this.vertexStream.filter(new VertexFilterIsVisualized(unionMap));
		DataStream<String> wrapperIds = visualizedVerticesStream.flatMap(new VertexFlatMapIsLayoutedOutsideBi(layoutedVertices,
				adjMatrix, top, right, bottom, left));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));

		//filter out already visualized edges in wrapper stream
		nonIdentityWrapper = nonIdentityWrapper.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
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
		DataStream<Row> newlyAddedInsideVertices = this.vertexStream.filter(new VertexFilterIsVisualized(newVertices))
				.filter(new VertexFilterNotInsideBefore(layoutedVertices, topOld, rightOld, bottomOld, leftOld));
		DataStream<String> wrapperIds = newlyAddedInsideVertices.flatMap(new VertexFlatMapIsLayoutedOutsideBi(layoutedVertices,
				adjMatrix, topNew, rightNew, bottomNew, leftNew));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		nonIdentityWrapper = nonIdentityWrapper.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return nonIdentityWrapper;
	}
	
	@Override
	public DataStream<Row> zoomOutLayoutFirstStep(Map<String, VertexGVD> layoutedVertices, 
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, 
			Float topOld, Float rightOld, Float bottomOld, Float leftOld){
		/*
		 * First substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * layouted inside the model space which was added by operation.
		 */
		System.out.println("in zoomOutLayoutFirstStep");
		
		//IDENTITY WRAPPER NEEDED
		
		zoomOutVertexFilter = new VertexFilterIsLayoutedInnerNewNotOld(layoutedVertices, leftNew, rightNew, topNew, 
				bottomNew, leftOld, rightOld, topOld, bottomOld);
		DataStream<Row> vertices = this.vertexStream.filter(zoomOutVertexFilter);
		DataStream<String> wrapperIds = vertices.flatMap(new VertexFlatMapIsLayoutedInnerNewNotOldUni(adjMatrix, layoutedVertices, 
				topNew, rightNew, bottomNew, leftNew, topOld, rightOld, bottomOld, leftOld));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = vertices.map(new VertexMapIdentityWrapperRow());
		return nonIdentityWrapper.union(identityWrapper);
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

		DataStream<Row> newlyVisualizedVertices = this.vertexStream
				.filter(new VertexFilterIsVisualized(newVertices))
				.filter(zoomOutVertexFilter);
		DataStream<String> wrapperIds = newlyVisualizedVertices.flatMap(new VertexFlatMapIsLayoutedOutsideBi(layoutedVertices,
				adjMatrix, top, right, bottom, left));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		return nonIdentityWrapper;
	}
}
