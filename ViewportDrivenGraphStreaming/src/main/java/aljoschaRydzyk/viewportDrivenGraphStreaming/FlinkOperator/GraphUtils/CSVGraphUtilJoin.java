package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import aljoschaRydzyk.viewportDrivenGraphStreaming.VertexGVD;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInner;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterInnerOldNotNew;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInnerNewNotOld;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedInside;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsLayoutedOutside;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterIsVisualized;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterMaxDegree;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotInsideBefore;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotLayouted;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterNotVisualized;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterOuter;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Vertex.VertexFilterOuterBoth;
import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Wrapper.WrapperFilterVisualizedWrappers;

public class CSVGraphUtilJoin implements GraphUtilStream{
	private StreamExecutionEnvironment fsEnv;
	private StreamTableEnvironment fsTableEnv;
	private String inPath;
	private DataStream<Row> wrapperStream = null;
	private DataStreamSource<Row> vertexStream = null;
	private Set<String> visualizedWrappers;
	private Set<String> visualizedVertices;
	private String vertexFields;
	private String wrapperFields;
	@SuppressWarnings("rawtypes")
	private TypeInformation[] wrapperFormatTypeInfo;
	private RowTypeInfo wrapperRowTypeInfo; 
	private Table wrapperTable;
	@SuppressWarnings("rawtypes")
	private TypeInformation[] vertexFormatTypeInfo;
	private Map<String,Map<String,String>> adjMatrix;
	private FilterFunction<Row> zoomOutVertexFilter;
	
	//Area Definition
		//A	: Inside viewport after operation
		//B : Outside viewport before and after operation
		//C : Inside viewport before and after operation
		//D : Outside viewport after operation
	
	public CSVGraphUtilJoin(StreamExecutionEnvironment fsEnv, StreamTableEnvironment fsTableEnv, String inPath, String vertexFields, String wrapperFields) {
		this.fsEnv = fsEnv;
		this.fsTableEnv = fsTableEnv;
		this.inPath = inPath;
		this.vertexFields = vertexFields;
		this.visualizedWrappers = new HashSet<String>();
		this.visualizedVertices = new HashSet<String>();
		this.wrapperFields = wrapperFields;
		this.wrapperFormatTypeInfo = new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING};
		this.vertexFormatTypeInfo = new TypeInformation[] {Types.STRING, Types.STRING, Types.INT, Types.STRING, 
				Types.INT, Types.INT, Types.LONG};
		this.wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
	}
	
	@Override
	public void setVisualizedWrappers(Set<String> visualizedWrappers) {
		this.visualizedWrappers = visualizedWrappers;
	}
	
	@Override
	public void setVisualizedVertices(Set<String> visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	public DataStream<Row> getWrapperStream() {
		return this.wrapperStream;
	}
	
	@Override
	public DataStream<Row> getVertexStream() {
		return this.vertexStream;
	}
	
	@Override
	public void initializeDataSets(){
		
		//NOTE: Flink needs seperate instances of RowCsvInputFormat for each data import, although they might be identical		
		//initialize wrapper stream
		Path wrappersFilePath = Path.fromLocalFile(new File(this.inPath + "_wrappers"));
		RowCsvInputFormat wrappersFormatIdentity = new RowCsvInputFormat(wrappersFilePath, this.wrapperFormatTypeInfo);
		wrappersFormatIdentity.setFieldDelimiter(";");
		DataStream<Row> wrapperStreamIdentity = this.fsEnv.readFile(wrappersFormatIdentity, this.inPath + "_vertices").setParallelism(1);
		RowCsvInputFormat wrappersFormat = new RowCsvInputFormat(wrappersFilePath, this.wrapperFormatTypeInfo);
		wrappersFormat.setFieldDelimiter(";");
		this.wrapperStream = wrapperStreamIdentity.union(this.fsEnv.readFile(wrappersFormat, this.inPath + "_wrappers").setParallelism(1));
		this.wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		
		//initialize vertex stream
		Path verticesFilePath = Path.fromLocalFile(new File(this.inPath + "_vertices"));
		RowCsvInputFormat verticesFormat = new RowCsvInputFormat(verticesFilePath, this.vertexFormatTypeInfo);
		verticesFormat.setFieldDelimiter(";");
		this.vertexStream = this.fsEnv.readFile(verticesFormat, this.inPath + "_vertices").setParallelism(1);
	}
	
	public DataStream<Row> getMaxDegreeSubset(Integer numberVertices){
		
		//filter for X vertices with highest degree where X is 'numberVertices' to retain a subset
		DataStream<Row> verticesMaxDegree = this.vertexStream.filter(new VertexFilterMaxDegree(numberVertices));
		Table vertexTable = fsTableEnv.fromDataStream(verticesMaxDegree).as(this.vertexFields);
			
		//produce wrappers containing only the subset of vertices
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable, wrapperRowTypeInfo);
		return wrapperStream;
	}

	@Override
	public DataStream<Row> zoom (Float top, Float right, Float bottom, Float left){
		/*
		 * Zoom function for graphs with layout
		 */
		
		System.out.println("Zoom, in csv zoom function ... top, right, bottom, left:" + top + " " + right + " "+ bottom + " " + left);

		
		//vertex stream filter for in-view and out-view area and conversion to Flink Tables
		DataStream<Row> vertexStreamInner = this.vertexStream.filter(new VertexFilterInner(top, right, bottom, left));
		DataStream<Row> vertexStreamOuter = this.vertexStream.filter(new VertexFilterOuter(top, right, bottom, left));
		Table vertexTable = fsTableEnv.fromDataStream(vertexStreamInner).as(this.vertexFields);		
		Table vertexTableOuter = fsTableEnv.fromDataStream(vertexStreamOuter).as(this.vertexFields);
		
		//produce wrapper stream from in-view area to in-view area
		Table wrapperTableInIn = wrapperTable
				.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		//produce wrapper stream from in-view area to out-view area and vice versa
		Table wrapperTableInOut = wrapperTable
				.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(vertexTableOuter).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOutIn = wrapperTable
				.join(vertexTableOuter).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		//stream union
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTableInIn, wrapperRowTypeInfo).union(fsTableEnv.toAppendStream(wrapperTableInOut, wrapperRowTypeInfo))
				.union(fsTableEnv.toAppendStream(wrapperTableOutIn, wrapperRowTypeInfo));
		
		//filter out already visualized edges in wrapper stream
		 wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		
		//filter out already visualized vertices in wrapper stream (identity wrappers)
		Set<String> visualizedVertices = this.visualizedVertices;
		wrapperStream = wrapperStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !(visualizedVertices.contains(value.getField(2).toString()) && value.getField(14).equals("identityEdge"));
			}
		});		
		return wrapperStream;
	}
	
	@Override
	public DataStream<Row> pan(Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld,
			Float bottomOld, Float leftOld){
		/*
		 * Pan function for graphs with layout
		 */
		
		//vertex stream filter and conversion to Flink Tables for areas A, B and C
		DataStream<Row> vertexStreamInner = this.vertexStream.filter(new VertexFilterInner(topNew, rightNew, bottomNew, leftNew));
		DataStream<Row> vertexStreamInnerNewNotOld = vertexStreamInner.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld);
				}
			});
		DataStream<Row> vertexStreamOldOuterBoth = this.vertexStream.filter(new VertexFilterOuterBoth(leftNew, rightNew, topNew, bottomNew, leftOld, rightOld, topOld, bottomOld));
		DataStream<Row> vertexStreamOldInnerNotNewInner = this.vertexStream.filter(new VertexFilterInnerOldNotNew(leftNew, rightNew, topNew, bottomNew, leftOld, rightOld, topOld, bottomOld));
		Table vertexTableInnerNew = fsTableEnv.fromDataStream(vertexStreamInnerNewNotOld).as(this.vertexFields);
		Table vertexTableOldOuterExtend = fsTableEnv.fromDataStream(vertexStreamOldOuterBoth).as(this.vertexFields);
		Table vertexTableOldInNotNewIn = fsTableEnv.fromDataStream(vertexStreamOldInnerNotNewInner).as(this.vertexFields);
		Table vertexTableInner = fsTableEnv.fromDataStream(vertexStreamInner).as(this.vertexFields);
		
		//produce wrapperStream from A to B and vice versa
		Table wrapperTableInOut = wrapperTable
				.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(vertexTableOldOuterExtend).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOutIn = wrapperTable
				.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
				.join(vertexTableOldOuterExtend).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		
		//produce wrapperStream from A to A
		Table wrapperTableInIn = wrapperTable
				.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
			//filter out redundant identity edges
			DataStream<Row> wrapperStreamInIn = fsTableEnv.toAppendStream(wrapperTableInIn, wrapperRowTypeInfo)
//					.filter(new WrapperFilterIdentity())
					;
		
		//produce wrapperStream from A+C to D and vice versa
		Table wrapperTableOldInNewInInOut = wrapperTable
				.join(vertexTableInner).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(vertexTableOldInNotNewIn).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOldInNewInOutIn = wrapperTable
				.join(vertexTableOldInNotNewIn).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(vertexTableInner).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
			//filter out already visualized edges
			DataStream<Row> wrapperStreamOldInNewIn = fsTableEnv.toAppendStream(wrapperTableOldInNewInInOut, wrapperRowTypeInfo)
					.union(fsTableEnv.toAppendStream(wrapperTableOldInNewInOutIn, wrapperRowTypeInfo));	
			wrapperStreamOldInNewIn = wrapperStreamOldInNewIn.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
			
		//stream union
		DataStream<Row> wrapperStream = wrapperStreamInIn
				.union(fsTableEnv.toAppendStream(wrapperTableOutIn, wrapperRowTypeInfo))
				.union(wrapperStreamOldInNewIn)
				.union(fsTableEnv.toAppendStream(wrapperTableInOut, wrapperRowTypeInfo));
		return wrapperStream;
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
	
	@Override
	public Map<String, Map<String, String>> getAdjMatrix() {
		return this.adjMatrix;
	}
	
	@Override
	public DataStream<Row> panZoomInLayoutFirstStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices, 
			Float top, Float right, Float bottom, Float left){
		/*
		 * First substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that were
		 * layouted before and have their coordinates in the current model window but are not visualized yet.
		 */
		
		DataStream<Row> vertices = this.vertexStream.filter(new VertexFilterIsLayoutedInside(layoutedVertices, top, right, bottom, left))
			.filter(new VertexFilterNotVisualized(innerVertices));
		Table verticesTable = fsTableEnv.fromDataStream(vertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(verticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(verticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields),
			wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	@Override
	public DataStream<Row> panZoomInLayoutSecondStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> unionMap){
		/*
		 * Second substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are not yet layouted on the
		 * other hand.
		 */
		
		DataStream<Row> visualizedVertices = this.vertexStream.filter(new VertexFilterIsVisualized(unionMap));
		DataStream<Row> neighbours = this.vertexStream
				.filter(new VertexFilterNotLayouted(layoutedVertices));
		Table visualizedVerticesTable = fsTableEnv.fromDataStream(visualizedVertices).as(this.vertexFields);
		Table neighboursTable = fsTableEnv.fromDataStream(neighbours).as(this.vertexFields);
		DataStream<Row> wrapperStream = 
			fsTableEnv.toAppendStream(wrapperTable
					.join(visualizedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
					.join(neighboursTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
				, wrapperRowTypeInfo)
			.union(fsTableEnv.toAppendStream(wrapperTable
					.join(neighboursTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
					.join(visualizedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
				, wrapperRowTypeInfo));
		return wrapperStream;
	}
	
	@Override
	public DataStream<Row> panZoomInLayoutThirdStep(Map<String, VertexGVD> layoutedVertices){		
		/*
		 * Third substep for pan/zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * not yet layouted starting with highest degree.
		 */
		DataStream<Row> notLayoutedVertices = this.vertexStream.filter(new VertexFilterNotLayouted(layoutedVertices));
		Table notLayoutedVerticesTable = fsTableEnv.fromDataStream(notLayoutedVertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(notLayoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(notLayoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
			, wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	@Override
	public DataStream<Row> zoomInLayoutFourthStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> innerVertices, 
			Map<String, VertexGVD> newVertices, Float top, Float right, Float bottom, Float left){
		/*
		 * Fourth substep for zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		
		//unite maps of already visualized vertices before this zoom-in operation and vertices added in this zoom-in operation
		Map<String,VertexGVD> unionMap = new HashMap<String,VertexGVD>(innerVertices);
		unionMap.putAll(newVertices);
		
		DataStream<Row> visualizedVerticesStream = this.vertexStream.filter(new VertexFilterIsVisualized(unionMap));
		DataStream<Row> layoutedVerticesStream = this.vertexStream.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, 
			top, right, bottom, left));
		Table visualizedVerticesTable = this.fsTableEnv.fromDataStream(visualizedVerticesStream).as(this.vertexFields);
		Table layoutedVerticesTable = this.fsTableEnv.fromDataStream(layoutedVerticesStream).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(layoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(visualizedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
				, wrapperRowTypeInfo)
			.union(fsTableEnv.toAppendStream(wrapperTable
				.join(visualizedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(layoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
				, wrapperRowTypeInfo));
		
		//filter out already visualized edges in wrapper stream
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return wrapperStream;
	}
	
	@Override
	public DataStream<Row> panLayoutFourthStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices, 
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, Float topOld, Float rightOld, Float bottomOld,
			Float leftOld){
		/*
		 * Fourth substep for pan operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * newly visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		
		DataStream<Row> newlyAddedInsideVertices = this.vertexStream.filter(new VertexFilterIsVisualized(newVertices))
				.filter(new VertexFilterNotInsideBefore(layoutedVertices, topOld, rightOld, bottomOld, leftOld));
		DataStream<Row> layoutedOutsideVertices = this.vertexStream.filter(new VertexFilterIsLayoutedOutside(layoutedVertices,
				topNew, rightNew, bottomNew, leftNew));
		Table newlyAddedInsideVerticesTable = this.fsTableEnv.fromDataStream(newlyAddedInsideVertices).as(this.vertexFields);
		Table layoutedOutsideVerticesTable = this.fsTableEnv.fromDataStream(layoutedOutsideVertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = this.fsTableEnv.toAppendStream(wrapperTable
				.join(newlyAddedInsideVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(layoutedOutsideVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), wrapperRowTypeInfo)
			.union(this.fsTableEnv.toAppendStream(wrapperTable
				.join(layoutedOutsideVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(newlyAddedInsideVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), wrapperRowTypeInfo));
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return wrapperStream;
	}
	
	@Override
	public DataStream<Row> zoomOutLayoutFirstStep(Map<String, VertexGVD> layoutedVertices, 
			Float topNew, Float rightNew, Float bottomNew, Float leftNew, 
			Float topOld, Float rightOld, Float bottomOld, Float leftOld){
		/*
		 * First substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * layouted inside the model space which was added by operation.
		 */
		
		zoomOutVertexFilter = new VertexFilterIsLayoutedInnerNewNotOld(layoutedVertices, leftNew, rightNew, topNew, 
				bottomNew, leftOld, rightOld, topOld, bottomOld);
		DataStream<Row> vertices = this.vertexStream.filter(zoomOutVertexFilter);
		Table verticesTable = fsTableEnv.fromDataStream(vertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(verticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(verticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	@Override
	public DataStream<Row> zoomOutLayoutSecondStep(Map<String, VertexGVD> layoutedVertices, Map<String, VertexGVD> newVertices, 
			Float top, Float right, Float bottom, Float left){
		/*
		 * Second substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the model space which was added by operation on the one hand, neighbour vertices that are layouted with 
		 * coordinates outside the current model window on the other hand.
		 */
		
		DataStream<Row> newlyVisualizedVertices = this.vertexStream
				.filter(new VertexFilterIsVisualized(newVertices))
				.filter(zoomOutVertexFilter);
		DataStream<Row> layoutedOutsideVertices = this.vertexStream
				.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, top, right, bottom, left));
		Table newlyVisualizedVerticesTable = this.fsTableEnv.fromDataStream(newlyVisualizedVertices).as(this.vertexFields);
		Table layoutedOutsideVerticesTable = this.fsTableEnv.fromDataStream(layoutedOutsideVertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = this.fsTableEnv.toAppendStream(wrapperTable
				.join(newlyVisualizedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(layoutedOutsideVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), 
				wrapperRowTypeInfo)
			.union(this.fsTableEnv.toAppendStream(wrapperTable
				.join(layoutedOutsideVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(newlyVisualizedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), 
				wrapperRowTypeInfo));
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return wrapperStream;
	}
}
