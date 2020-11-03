package aljoschaRydzyk.Gradoop_Flink_Prototype;

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

public class CSVGraphUtilJoin implements GraphUtil{
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
	public void initializeStreams(){
		
		//NOTE: Flink needs seperate instances of RowCsvInputFormat for each data import, although they might be identical		
		//initialize wrapper stream
		Path wrappersFilePath = Path.fromLocalFile(new File(this.inPath + "_wrappers"));
		RowCsvInputFormat wrappersFormatIdentity = new RowCsvInputFormat(wrappersFilePath, this.wrapperFormatTypeInfo);
		wrappersFormatIdentity.setFieldDelimiter(";");
		DataStream<Row> wrapperStreamIdentity = this.fsEnv.readFile(wrappersFormatIdentity, this.inPath + "_vertices").setParallelism(1);
		RowCsvInputFormat wrappersFormat = new RowCsvInputFormat(wrappersFilePath, this.wrapperFormatTypeInfo);
		wrappersFormat.setFieldDelimiter(";");
		this.wrapperStream = wrapperStreamIdentity.union(this.fsEnv.readFile(wrappersFormat, this.inPath + "_wrappers").setParallelism(1));

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
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable, wrapperRowTypeInfo);
		return wrapperStream;
	}

	@Override
	public DataStream<Row> zoom (Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		/*
		 * Zoom function for graphs with layout
		 */
		
		//vertex stream filter for in-view and out-view area and conversion to Flink Tables
		DataStream<Row> vertexStreamInner = this.vertexStream.filter(new VertexFilterInner(topModel, rightModel, bottomModel, leftModel));
		DataStream<Row> vertexStreamOuter = this.vertexStream.filter(new VertexFilterOuter(topModel, rightModel, bottomModel, leftModel));
		Table vertexTable = fsTableEnv.fromDataStream(vertexStreamInner).as(this.vertexFields);		
		Table vertexTableOuter = fsTableEnv.fromDataStream(vertexStreamOuter).as(this.vertexFields);

		//filter out already visualized edges in wrapper stream
		DataStream<Row> wrapperStream = this.wrapperStream;
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		
		//filter out already visualized vertices in wrapper stream (identity wrappers)
		Set<String> visualizedVertices = this.visualizedVertices;
		wrapperStream = wrapperStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !(visualizedVertices.contains(value.getField(2).toString()) && value.getField(14).equals("identityEdge"));
			}
		});
		
		//produce wrapper stream from in-view area to in-view area
		Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream).as(this.wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		//produce wrapper stream from in-view area to out-view area and vice versa
		Table wrapperTableInOut = fsTableEnv.fromDataStream(wrapperStream).as(this.wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTableOuter).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOutIn = fsTableEnv.fromDataStream(wrapperStream).as(this.wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTableOuter).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		//stream union
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		wrapperStream = fsTableEnv.toAppendStream(wrapperTable, wrapperRowTypeInfo).union(fsTableEnv.toAppendStream(wrapperTableInOut, wrapperRowTypeInfo))
				.union(fsTableEnv.toAppendStream(wrapperTableOutIn, wrapperRowTypeInfo));
		return wrapperStream;
	}
	
	@Override
	public DataStream<Row> pan(Float top, Float right, Float bottom, Float left, Float xModelDiff, Float yModelDiff){
		/*
		 * Pan function for graphs with layout
		 */
		
		//calculate previous model coordinate borders
		Float topOld = top - yModelDiff;
		Float rightOld = right - xModelDiff;
		Float bottomOld = bottom - yModelDiff;
		Float leftOld = left - xModelDiff;
		
		//vertex stream filter and conversion to Flink Tables for areas A, B and C
		DataStream<Row> vertexStreamInner = this.vertexStream.filter(new VertexFilterInner(top, right, bottom, left));
		DataStream<Row> vertexStreamInnerNewNotOld = vertexStreamInner.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld);
				}
			});
		DataStream<Row> vertexStreamOldOuterBoth = this.vertexStream.filter(new VertexFilterOuterBoth(left, right, top, bottom, leftOld, rightOld, topOld, bottomOld));
		DataStream<Row> vertexStreamOldInnerNotNewInner = this.vertexStream.filter(new VertexFilterInnerOldNotNew(left, right, top, bottom, leftOld, rightOld, topOld, bottomOld));
		Table vertexTableInnerNew = fsTableEnv.fromDataStream(vertexStreamInnerNewNotOld).as(this.vertexFields);
		Table vertexTableOldOuterExtend = fsTableEnv.fromDataStream(vertexStreamOldOuterBoth).as(this.vertexFields);
		Table vertexTableOldInNotNewIn = fsTableEnv.fromDataStream(vertexStreamOldInnerNotNewInner).as(this.vertexFields);
		Table vertexTableInner = fsTableEnv.fromDataStream(vertexStreamInner).as(this.vertexFields);
		
		//wrapper stream initialization
		DataStream<Row> wrapperStream = this.wrapperStream;
		Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream).as(this.wrapperFields);
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		
		//produce wrapperStream from A to B and vice versa
		Table wrapperTableInOut = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTableOldOuterExtend).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOutIn = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTableOldOuterExtend).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		
		//produce wrapperStream from A to A
		Table wrapperTableInIn = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableInIn = wrapperTableInIn.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
			//filter out redundant identity edges
			DataStream<Row> wrapperStreamInIn = fsTableEnv.toAppendStream(wrapperTableInIn, wrapperRowTypeInfo).filter(new WrapperFilterIdentity());
		
		//produce wrapperStream from A+C to D and vice versa
		Table wrapperTableOldInNewInInOut = wrapperTable.join(vertexTableInner)
				.where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOldInNewInInOut = wrapperTableOldInNewInInOut.join(vertexTableOldInNotNewIn)
				.where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOldInNewInOutIn = wrapperTable.join(vertexTableOldInNotNewIn)
				.where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOldInNewInOutIn = wrapperTableOldInNewInOutIn.join(vertexTableInner)
				.where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
			//filter out already visualized edges
			DataStream<Row> wrapperStreamOldInNewIn = fsTableEnv.toAppendStream(wrapperTableOldInNewInInOut, wrapperRowTypeInfo)
					.union(fsTableEnv.toAppendStream(wrapperTableOldInNewInOutIn, wrapperRowTypeInfo));	
			wrapperStreamOldInNewIn = wrapperStreamOldInNewIn.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
			
		//stream union
		wrapperStream = wrapperStreamInIn
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
	
	public DataStream<Row> zoomInLayoutFirstStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> innerVertices, 
			Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		/*
		 * First substep for zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that were
		 * layouted before and have their coordinates in the current model window but are not visualized yet.
		 */
		
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		DataStream<Row> vertices = this.vertexStream.filter(new VertexFilterIsLayoutedInside(layoutedVertices, topModel, rightModel, bottomModel, leftModel))
			.filter(new VertexFilterNotVisualized(innerVertices));
		Table verticesTable = fsTableEnv.fromDataStream(vertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(verticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(verticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields),
			wrapperRowTypeInfo);
		return wrapperStream;

		
//		//(3) produce wrapper identity stream of not visualized but layouted vertices inside model position
//		//    and also produce wrapper stream between those vertices
//		Set<String> notVisualizedButLayoutedNonNeighbourIds = new HashSet<String>();
//		for (Map.Entry<String, VertexCustom> layoutedVerticesEntry : layoutedVertices.entrySet()) {
//			String notVisualizedVertexId = layoutedVerticesEntry.getKey();
//			if (!innerVertices.containsKey(notVisualizedVertexId) && this.vertexIsInside(layoutedVerticesEntry.getValue(), 
//					topModel, rightModel, bottomModel, leftModel)) {
//					notVisualizedButLayoutedNonNeighbourIds.add(notVisualizedVertexId);
//				}
//			}
//		
//		//return to next step if set is empty
//		if (notVisualizedButLayoutedNonNeighbourIds.isEmpty()) return null;
//		
//		DataStream<String> notVisualizedButLayoutedNonNeighbours = fsEnv.fromCollection(notVisualizedButLayoutedNonNeighbourIds);
//		Table candidatesTable = fsTableEnv.fromDataStream(notVisualizedButLayoutedNonNeighbours).as("vertexIdGradoop");
//		wrapperTable = wrapperTable.join(candidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		wrapperTable = wrapperTable.join(candidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		
//		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable, wrapperRowTypeInfo);
//		return wrapperStream;
	}
	
	public DataStream<Row> zoomInLayoutSecondStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> innerVertices, 
			Map<String, VertexCustom> newVertices){
		/*
		 * Second substep for zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are not yet layouted on the
		 * other hand.
		 */

		//unite maps of already visualized vertices before this zoom-in operation and vertices added in this zoom-in operation
		Map<String,VertexCustom> unionMap = new HashMap<String,VertexCustom>(innerVertices);
		unionMap.putAll(newVertices);
		
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
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
		
//		//(1) produce wrapper stream from visualized vertices to neighbours that are not visualized but also not layouted outside model position
//		Set<String> notVisualizedNeighboursIds = new HashSet<String>();
//		for (Map.Entry<String, VertexCustom> innerVerticesEntry : innerVertices.entrySet()) {
//			for (Map.Entry<String, String> adjEntry : this.adjMatrix.get(innerVerticesEntry.getKey()).entrySet()) {
//				String notVisualizedVertexId = adjEntry.getKey();
//				if (!innerVertices.containsKey(notVisualizedVertexId)) {
//					if (layoutedVertices.containsKey(notVisualizedVertexId)) {
//						if (this.vertexIsInside(layoutedVertices.get(notVisualizedVertexId), topModel, rightModel, bottomModel, leftModel)) {
//							notVisualizedNeighboursIds.add(notVisualizedVertexId);
//						}
//					} else {
//						notVisualizedNeighboursIds.add(notVisualizedVertexId);
//					}
//				}
//			}
//		}
//		
//		//return to next step if set is empty
//		if (notVisualizedNeighboursIds.isEmpty()) return null;
//		
//		DataStream<String> visualizedVerticesStream = fsEnv.fromCollection(innerVertices.keySet());
//		DataStream<String> notVisualizedNeighboursStream = fsEnv.fromCollection(notVisualizedNeighboursIds);
//		Table visualizedVerticesTable = fsTableEnv.fromDataStream(visualizedVerticesStream).as("vertexIdGradoop");
//		Table neighbourCandidatesTable = fsTableEnv.fromDataStream(notVisualizedNeighboursStream).as("vertexIdGradoop");
//		Table wrapperTableNewOld = wrapperTable.join(neighbourCandidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		wrapperTableNewOld = wrapperTableNewOld.join(visualizedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		Table wrapperTableOldNew = wrapperTable.join(visualizedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		wrapperTableOldNew = wrapperTableOldNew.join(neighbourCandidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		
//		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTableOldNew, wrapperRowTypeInfo)
//				.union(fsTableEnv.toAppendStream(wrapperTableNewOld, wrapperRowTypeInfo));
//
//		return wrapperStream;
	}
	
	public DataStream<Row> zoomInLayoutThirdStep(Map<String, VertexCustom> layoutedVertices){		
		/*
		 * Third substep for zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * not yet layouted starting with highest degree.
		 */
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		DataStream<Row> notLayoutedVertices = this.vertexStream.filter(new VertexFilterNotLayouted(layoutedVertices));
		Table notLayoutedVerticesTable = fsTableEnv.fromDataStream(notLayoutedVertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(notLayoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(notLayoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
			, wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	public DataStream<Row> zoomInLayoutFourthStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> innerVertices, 
			Map<String, VertexCustom> newVertices, Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		/*
		 * Fourth substep for zoom-in operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		
		//unite maps of already visualized vertices before this zoom-in operation and vertices added in this zoom-in operation
		Map<String,VertexCustom> unionMap = new HashMap<String,VertexCustom>(innerVertices);
		unionMap.putAll(newVertices);
		
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		DataStream<Row> visualizedVerticesStream = this.vertexStream.filter(new VertexFilterIsVisualized(unionMap));
		DataStream<Row> layoutedVerticesStream = this.vertexStream.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, 
			topModel, rightModel, bottomModel, leftModel));
		Table visualizedVerticesTable = this.fsTableEnv.fromDataStream(visualizedVerticesStream).as(this.vertexFields);
		Table layoutedVerticesTable = this.fsTableEnv.fromDataStream(layoutedVerticesStream).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(layoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(visualizedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), wrapperRowTypeInfo)
			.union(fsTableEnv.toAppendStream(wrapperTable
				.join(visualizedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(layoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), wrapperRowTypeInfo));
		
		//filter out already visualized edges in wrapper stream
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return wrapperStream;

//		//(2) produce wrapper stream from visualized vertices to layouted vertices outside model position
//		Set<String> outsideNeighboursIds = new HashSet<String>();
//		for (Map.Entry<String, VertexCustom> innerVerticesEntry : innerVertices.entrySet()) {
//			for (Map.Entry<String, String> adjEntry : this.adjMatrix.get(innerVerticesEntry.getKey()).entrySet()) {
//				String neighbourVertexId = adjEntry.getKey();
//				if (!innerVertices.containsKey(neighbourVertexId) && layoutedVertices.containsKey(neighbourVertexId) 
//						&& !this.vertexIsInside(layoutedVertices.get(neighbourVertexId), topModel, rightModel, bottomModel, leftModel)) {
//					outsideNeighboursIds.add(neighbourVertexId);
//				}
//			}
//		}
//		
//		if (outsideNeighboursIds.isEmpty()) return null;
//		
//		DataStream<String> visualizedVerticesStream = fsEnv.fromCollection(innerVertices.keySet());
//		DataStream<String> outsideNeighboursStream = fsEnv.fromCollection(outsideNeighboursIds);
//		Table layoutedVerticesTable = fsTableEnv.fromDataStream(visualizedVerticesStream).as("vertexIdGradoop");
//		Table neighbourCandidatesTable = fsTableEnv.fromDataStream(outsideNeighboursStream).as("vertexIdGradoop");
//		Table wrapperTableNewOld = wrapperTable.join(neighbourCandidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		wrapperTableNewOld = wrapperTableNewOld.join(layoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		Table wrapperTableOldNew = wrapperTable.join(layoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		wrapperTableOldNew = wrapperTableOldNew.join(neighbourCandidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		
//		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTableOldNew, wrapperRowTypeInfo)
//				.union(fsTableEnv.toAppendStream(wrapperTableNewOld, wrapperRowTypeInfo));
//		
//		//filter out already visualized edges in wrapper stream
//		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
//		
//		return wrapperStream;
	}
	
	public DataStream<Row> panLayoutFirstStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
			Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		/*
		 * First substep for pan operation on graphs without layout. Returns a stream of wrappers including vertices that were
		 * layouted before and have their coordinates in the current model window but are not visualized yet.
		 */

		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		DataStream<Row> vertices = this.vertexStream.filter(new VertexFilterNotVisualized(newVertices))
				.filter(new VertexFilterIsLayoutedInside(layoutedVertices, topModel, rightModel, bottomModel, leftModel));
		Table verticesTable = fsTableEnv.fromDataStream(vertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(verticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(verticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), wrapperRowTypeInfo);
		return wrapperStream;
		
//		//(5) produce wrapper identity stream for layouted vertices within model position that are not visualized yet
//		//	  and also produce wrapper stream between those vertices
//		Set<String> notVisualizedButLayoutedNonNeighboursIds = new HashSet<String>();
//		for (String key : newVertices.keySet()) System.out.println("inner key " + key);
//		for (Map.Entry<String, VertexCustom> layoutedVerticesEntry : layoutedVertices.entrySet()) {
//			String vertexId = layoutedVerticesEntry.getKey();
//			System.out.println("boolean is inside innerVertices " + newVertices.containsKey(vertexId));
//			if (!newVertices.containsKey(vertexId) && this.vertexIsInside(layoutedVerticesEntry.getValue(), 
//					topModel, rightModel, bottomModel, leftModel)) {
//				System.out.println("panLayoutSecondStep " + vertexId);
//				notVisualizedButLayoutedNonNeighboursIds.add(vertexId);
//			}
//
//		}
//		
//		//return to next step if set is empty
//		System.out.println("notVisualizedButLayoutedNonNeighboursIds size in panLayoutSecondStep: " + notVisualizedButLayoutedNonNeighboursIds.size());
//		System.out.println(notVisualizedButLayoutedNonNeighboursIds.isEmpty());
//		if (notVisualizedButLayoutedNonNeighboursIds.isEmpty()) return null;
//		
//		DataStream<String> notVisualizedButLayoutedNonNeighbours = fsEnv.fromCollection(notVisualizedButLayoutedNonNeighboursIds);
//		Table candidatesTable = fsTableEnv.fromDataStream(notVisualizedButLayoutedNonNeighbours).as("vertexIdGradoop");
//		wrapperTable = wrapperTable.join(candidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		wrapperTable = wrapperTable.join(candidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		
//		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable, wrapperRowTypeInfo);
//		return wrapperStream;
	}
	
	public DataStream<Row> panLayoutSecondStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices){
		/*
		 * Second substep for pan operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the current model window on the one hand, and neighbour vertices that are not yet layouted on the other 
		 * hand.
		 */
		
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		DataStream<Row> visualizedVertices = this.vertexStream.filter(new VertexFilterIsVisualized(newVertices));
		DataStream<Row> notVisualizedNotLayoutedVertices = this.vertexStream
				.filter(new VertexFilterNotLayouted(layoutedVertices));
		Table visualizedVerticesTable = fsTableEnv.fromDataStream(visualizedVertices).as(this.vertexFields);
		Table notVisualizedNotLayoutedVerticesTable = fsTableEnv.fromDataStream(notVisualizedNotLayoutedVertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(visualizedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(notVisualizedNotLayoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
				, wrapperRowTypeInfo)
			.union(fsTableEnv.toAppendStream(wrapperTable
				.join(notVisualizedNotLayoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(visualizedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), 
				wrapperRowTypeInfo));
		return wrapperStream;
	
//		//(2 + 4) produce wrapper Stream from visualized vertices inside to neighbour vertices which are not layouted and not visualized
//		
//		System.out.println("in panLayoutThirdStep function");
//		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
//		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
//
//		Set<String> notVisualizedNotLayoutedNeighboursIds = new HashSet<String>();
//		for (Map.Entry<String, VertexCustom> innerVerticesEntry : newVertices.entrySet()) {
//			for (Map.Entry<String, String> adjEntry : this.adjMatrix.get(innerVerticesEntry.getKey()).entrySet()) {
//				String notLayoutedVertexId = adjEntry.getKey();
//				if (!newVertices.containsKey(notLayoutedVertexId) && !layoutedVertices.containsKey(notLayoutedVertexId)) {
//					notVisualizedNotLayoutedNeighboursIds.add(notLayoutedVertexId);
//				}
//			}
//		}
//		
//		//return to next step if set is empty
//		if (notVisualizedNotLayoutedNeighboursIds.isEmpty()) return null;
//		
//		DataStream<String> visualizedVerticesStream = fsEnv.fromCollection(newVertices.keySet());
//		DataStream<String> notVisualizedNotLayoutedNeighbours = fsEnv.fromCollection(notVisualizedNotLayoutedNeighboursIds);
//		Table layoutedVerticesTable = fsTableEnv.fromDataStream(visualizedVerticesStream).as("vertexIdGradoop");
//		Table neighbourCandidatesTable = fsTableEnv.fromDataStream(notVisualizedNotLayoutedNeighbours).as("vertexIdGradoop");
//		Table wrapperTableNewOld = wrapperTable.join(neighbourCandidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		wrapperTableNewOld = wrapperTableNewOld.join(layoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		Table wrapperTableOldNew = wrapperTable.join(layoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		wrapperTableOldNew = wrapperTableOldNew.join(neighbourCandidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		
//		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTableOldNew, wrapperRowTypeInfo)
//				.union(fsTableEnv.toAppendStream(wrapperTableNewOld, wrapperRowTypeInfo));
//		return wrapperStream;
	}
	
	public DataStream<Row> panLayoutThirdStep(Map<String, VertexCustom> layoutedVertices){
		/*
		 * Third substep for pan operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * not yet layouted starting with highest degree.
		 */
		
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		DataStream<Row> notLayoutedVertices = this.vertexStream.filter(new VertexFilterNotLayouted(layoutedVertices));
		Table notLayoutedVerticesTable = fsTableEnv.fromDataStream(notLayoutedVertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(notLayoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(notLayoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields)
			, wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	public DataStream<Row> panLayoutFourthStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
			Float top, Float right, Float bottom, Float left, Float xModelDiff, Float yModelDiff){
		/*
		 * Fourth substep for pan operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * newly visualized inside the current model window on the one hand, and neighbour vertices that are layouted with coordinates 
		 * outside the current model window on the other hand.
		 */
		
		//calculate previous model coordinate borders
		Float topOld = top - yModelDiff;
		Float rightOld = right - xModelDiff;
		Float bottomOld = bottom - yModelDiff;
		Float leftOld = left - xModelDiff;
		
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		DataStream<Row> newlyAddedInsideVertices = this.vertexStream.filter(new VertexFilterIsVisualized(newVertices))
				.filter(new VertexFilterNotInsideBefore(layoutedVertices, topOld, rightOld, bottomOld, leftOld));
		DataStream<Row> layoutedOutsideVertices = this.vertexStream.filter(new VertexFilterIsLayoutedOutside(layoutedVertices,
				top, right, bottom, left));
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
		
//		//(3) produce wrapperStream from visualized vertices that were newly added inside to layouted Vertices outside the model position
//		Set<String> newlyInsideVisualizedIds = new HashSet<String>();
//		for (String vertexId : newVertices.keySet()) {
//			VertexCustom vertex = layoutedVertices.get(vertexId);
//			System.out.println("panLayout5: " + vertex.getIdGradoop() + " " + vertex.getX());
//			Integer x = vertex.getX();
//			Integer y = vertex.getY();
//			if (this.vertexIsInside(vertex, top, right, bottom, left) && 
//					!(x >= leftOld && x <= rightOld && y >= topOld && y <= bottomOld)) newlyInsideVisualizedIds.add(vertex.getIdGradoop());
//		}
//		Set<String> outsideLayoutedNeighbourIds = new HashSet<String>();
//		for (String insideVertexId : newlyInsideVisualizedIds) {
//			for (Map.Entry<String, String> adjEntry : this.adjMatrix.get(insideVertexId).entrySet()) {
//				String neighbourVertexId = adjEntry.getKey();
//				if (!newVertices.containsKey(neighbourVertexId)) outsideLayoutedNeighbourIds.add(neighbourVertexId);
//			}
//		}
//		
//		if (newlyInsideVisualizedIds.isEmpty() || outsideLayoutedNeighbourIds.isEmpty()) return null;
//		
//		DataStream<String> newlyInsideVerticesStream = fsEnv.fromCollection(newlyInsideVisualizedIds);
//		DataStream<String> notInsideNeighbours = fsEnv.fromCollection(outsideLayoutedNeighbourIds);
//		Table newlyInsideVerticesTable = fsTableEnv.fromDataStream(newlyInsideVerticesStream).as("vertexIdGradoop");
//		Table neighbourCandidatesTable = fsTableEnv.fromDataStream(notInsideNeighbours).as("vertexIdGradoop");
//		Table wrapperTableNewOld = wrapperTable.join(neighbourCandidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		wrapperTableNewOld = wrapperTableNewOld.join(newlyInsideVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		Table wrapperTableOldNew = wrapperTable.join(newlyInsideVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
//		wrapperTableOldNew = wrapperTableOldNew.join(neighbourCandidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
//		
//		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTableOldNew, wrapperRowTypeInfo)
//				.union(fsTableEnv.toAppendStream(wrapperTableNewOld, wrapperRowTypeInfo));
//		return wrapperStream;
	}
	
	public DataStream<Row> zoomOutLayoutFirstStep(Map<String, VertexCustom> layoutedVertices, 
			Float topModelNew, Float rightModelNew, Float bottomModelNew, Float leftModelNew, 
			Float topModelOld, Float rightModelOld, Float bottomModelOld, Float leftModelOld){
		/*
		 * First substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * layouted inside the model space which was added by operation.
		 */
		
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		zoomOutVertexFilter = new VertexFilterIsLayoutedInnerNewNotOld(layoutedVertices, leftModelNew, rightModelNew, topModelNew, 
				bottomModelNew, leftModelOld, rightModelOld, topModelOld, bottomModelOld);
		DataStream<Row> vertices = this.vertexStream.filter(zoomOutVertexFilter);
		Table verticesTable = fsTableEnv.fromDataStream(vertices).as(this.vertexFields);
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable
				.join(verticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields)
				.join(verticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields), wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	public DataStream<Row> zoomOutLayoutSecondStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
			Float topModelNew, Float rightModelNew, Float bottomModelNew, Float leftModelNew, 
			Float topModelOld, Float rightModelOld, Float bottomModelOld, Float leftModelOld){
		/*
		 * Second substep for zoom-out operation on graphs without layout. Returns a stream of wrappers including vertices that are 
		 * visualized inside the model space which was added by operation on the one hand, neighbour vertices that are layouted with 
		 * coordinates outside the current model window on the other hand.
		 */
		
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		DataStream<Row> newlyVisualizedVertices = this.vertexStream
				.filter(new VertexFilterIsVisualized(newVertices))
				.filter(zoomOutVertexFilter);
		DataStream<Row> layoutedOutsideVertices = this.vertexStream
				.filter(new VertexFilterIsLayoutedOutside(layoutedVertices, topModelNew, rightModelNew, bottomModelNew, leftModelNew));
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
