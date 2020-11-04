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
	private FilterFunction<Row> zoomOutVertexFilter;

	
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
		System.out.println("vis wrappers");
		System.out.println(visualizedWrappers);
		this.visualizedWrappers = visualizedWrappers;
	}
	
	public Map<String,Map<String,String>> getAdjMatrix(){
		return this.adjMatrix;
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
	public DataStream<Row> pan(Float top, Float right, Float bottom, Float left, Float xModelDiff, Float yModelDiff) {
		Float topOld = top - yModelDiff;
		Float rightOld = right - xModelDiff;
		Float bottomOld = bottom - yModelDiff;
		Float leftOld = left - xModelDiff;
		DataStream<Row> vertexStreamInnerNewNotOld = this.vertexStream
			.filter(new VertexFilterInnerNewNotOld(left, right, top, bottom, leftOld, rightOld, topOld, bottomOld));
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
							((left > targetX) || (targetX > right) || (top > targetY) || (targetY > bottom))) {
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
				.filter(new VertexFilterInnerOldNotNew(left, right, top, bottom, leftOld, rightOld, topOld, bottomOld));
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
					if ((left <= targetX) &&  (targetX <= right) && (top <= targetY) && (targetY <= bottom)) {
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
				String sourceIdGradoop = (String) value.getField(1);
				Integer sourceIdNumeric = (Integer) value.getField(2);
				Map<String,String> map = adjMatrix.get(sourceIdGradoop);
				for (Map.Entry<String, String> entry : map.entrySet()) {
					String targetIdGradoop = entry.getKey();
					Row targetVertex = vertexMap.get(targetIdGradoop);
					Integer targetIdNumeric = (Integer) targetVertex.getField(2);
					if (targetIdNumeric < numberVertices && sourceIdNumeric > targetIdNumeric) {
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
	
	@Override
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
		@SuppressWarnings("unused")
		int i = 0;
		while ((row = csvReader.readLine()) != null) {
			i += 1;
		    String[] arr = row.split(";");
		    Row wrapper = Row.of(arr[0], arr[1], Integer.parseInt(arr[2]), arr[3], Integer.parseInt(arr[4]), Integer.parseInt(arr[5]), Long.parseLong(arr[6]),
		    		arr[7], Integer.parseInt(arr[8]), arr[9], Integer.parseInt(arr[10]), Integer.parseInt(arr[11]), Long.parseLong(arr[12]), arr[13], arr[14]);
		    this.wrapperMap.put(arr[13], wrapper);
		}
		csvReader.close();
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
	
//	public boolean vertexIsInside(VertexCustom vertex, Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
//		Integer x = vertex.getX();
//		Integer y = vertex.getY();
//		return (x >= leftModel && x <= rightModel && y >= topModel && y <= bottomModel);
//	}
	
	/*
	 * General workflow for this GraphUtil:
	 * 		-	produce a vertex stream
	 * 		- 	flatMap and produce a stream of all relevant wrapperIDs using adjacency matrix
	 * 		- 	map wrapperIDstream to wrapperStream
	 */
	
	public DataStream<Row> panZoomInLayoutFirstStep(Map<String,VertexCustom> layoutedVertices, Map<String,VertexCustom> innerVertices,
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
		DataStream<Row> identityWrapper = vertices.map(new VertexMapIdentityWrapper());
		return nonIdentityWrapper.union(identityWrapper);
	}
	
	public DataStream<Row> panZoomInLayoutSecondStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> unionMap){
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
	
	public DataStream<Row> panZoomInLayoutThirdStep(Map<String, VertexCustom> layoutedVertices){		
		/*
		 * Third substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * not yet layouted starting with highest degree.
		 */
		System.out.println("in panZoomInLayoutThirdStep");

		DataStream<Row> notLayoutedVertices = this.vertexStream.filter(new VertexFilterNotLayouted(layoutedVertices));
		DataStream<String> wrapperIds = notLayoutedVertices.flatMap(new VertexFlatMapNotLayoutedUni(adjMatrix, layoutedVertices));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		
		//produce Identity Wrapper Stream
		DataStream<Row> identityWrapper = notLayoutedVertices.map(new VertexMapIdentityWrapper());
		return nonIdentityWrapper.union(identityWrapper);
	}
	
	public DataStream<Row> zoomInLayoutFourthStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> innerVertices, 
			Map<String, VertexCustom> newVertices, Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		/*
		 * Fourth substep for zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		System.out.println("in ZoomInLayoutFourthStep");

		//unite maps of already visualized vertices before this zoom-in operation and vertices added in this zoom-in operation
		Map<String,VertexCustom> unionMap = new HashMap<String,VertexCustom>(innerVertices);
		unionMap.putAll(newVertices);
		
		DataStream<Row> visualizedVerticesStream = this.vertexStream.filter(new VertexFilterIsVisualized(unionMap));
		DataStream<String> wrapperIds = visualizedVerticesStream.flatMap(new VertexFlatMapIsLayoutedOutsideBi(layoutedVertices,
				adjMatrix, topModel, rightModel, bottomModel, leftModel));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));

		//filter out already visualized edges in wrapper stream
		nonIdentityWrapper = nonIdentityWrapper.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return nonIdentityWrapper;
	}
	
	public DataStream<Row> panLayoutFourthStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
			Float top, Float right, Float bottom, Float left, Float xModelDiff, Float yModelDiff){
		/*
		 * Fourth substep for pan operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * newly visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		System.out.println("in panLayoutFourthStep");

		//calculate previous model coordinate borders
		Float topOld = top - yModelDiff;
		Float rightOld = right - xModelDiff;
		Float bottomOld = bottom - yModelDiff;
		Float leftOld = left - xModelDiff;
		
		DataStream<Row> newlyAddedInsideVertices = this.vertexStream.filter(new VertexFilterIsVisualized(newVertices))
				.filter(new VertexFilterNotInsideBefore(layoutedVertices, topOld, rightOld, bottomOld, leftOld));
		DataStream<String> wrapperIds = newlyAddedInsideVertices.flatMap(new VertexFlatMapIsLayoutedOutsideBi(layoutedVertices,
				adjMatrix, top, right, bottom, left));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		nonIdentityWrapper = nonIdentityWrapper.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return nonIdentityWrapper;
	}
	
	public DataStream<Row> zoomOutLayoutFirstStep(Map<String, VertexCustom> layoutedVertices, 
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
		DataStream<Row> identityWrapper = vertices.map(new VertexMapIdentityWrapper());
		return nonIdentityWrapper.union(identityWrapper);
	}
	
	public DataStream<Row> zoomOutLayoutSecondStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
			Float topNew, Float rightNew, Float bottomNew, Float leftNew){
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
				adjMatrix, topNew, rightNew, bottomNew, leftNew));
		DataStream<Row> nonIdentityWrapper = wrapperIds.map(new WrapperIDMapWrapper(this.wrapperMap));
		return nonIdentityWrapper;
	}
}
