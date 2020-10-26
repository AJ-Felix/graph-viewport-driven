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
	
	private boolean vertexIsInside(VertexCustom vertex,	Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
		Integer x = vertex.getX();
		Integer y = vertex.getY();
		return x >= leftModel && x <= rightModel && y >= topModel && y <= bottomModel;
	}
	
	//Prelayout functions
	
	public DataStream<Row> zoomInLayoutFirstStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> innerVertices, 
			Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		//Diese Funktion sollte solange wieder aufgerufen werden, bis die Kapazität erreicht ist. Wenn die Kapazität nicht erreich wird, sollten 
		//anschließend nicht gelayoutete Nachbarknoten zu den bisher visualisierten hinzugefügt werden. Wenn dann die Kapazität immer noch nicht erreicht ist, 
		//sollten nicht gelayoutete Knoten (beginnend mit größtem Grad?) hinzugefügt werden. Anschließend müssen noch die Kanten von den eben hinzugefügten
		//Knoten zu Nachbarn außerhalb des Viewports hinzugefügt werden.
		
		System.out.println("in ZoomInLayoutFirstStep function");
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		
		//(3) produce wrapper identity stream of not visualized but layouted vertices inside model position
		//    and also produce wrapper stream between those vertices
		Set<String> notVisualizedButLayoutedNonNeighbourIds = new HashSet<String>();
		for (Map.Entry<String, VertexCustom> layoutedVerticesEntry : layoutedVertices.entrySet()) {
			String notVisualizedVertexId = layoutedVerticesEntry.getKey();
			if (!innerVertices.containsKey(notVisualizedVertexId) && this.vertexIsInside(layoutedVerticesEntry.getValue(), 
					topModel, rightModel, bottomModel, leftModel)) {
					notVisualizedButLayoutedNonNeighbourIds.add(notVisualizedVertexId);
				}
			}
		
		//return to next step if set is empty
		if (notVisualizedButLayoutedNonNeighbourIds.isEmpty()) return null;
		
		DataStream<String> notVisualizedButLayoutedNonNeighbours = fsEnv.fromCollection(notVisualizedButLayoutedNonNeighbourIds);
		Table candidatesTable = fsTableEnv.fromDataStream(notVisualizedButLayoutedNonNeighbours).as("vertexIdGradoop");
		wrapperTable = wrapperTable.join(candidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTable = wrapperTable.join(candidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable, wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	public DataStream<Row> zoomInLayoutSecondStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> innerVertices, 
			Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		//Diese Funktion sollte solange wieder aufgerufen werden, bis die Kapazität erreicht ist. Wenn die Kapazität nicht erreicht ist, 
		//sollten nicht gelayoutete Knoten (beginnend mit größtem Grad?) hinzugefügt werden. Anschließend müssen noch die Kanten von den eben hinzugefügten
		//Knoten zu Nachbarn außerhalb des Viewports hinzugefügt werden.
		
		System.out.println("in ZoomInLayoutSecondStep function");
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		
		//DIFFERENT APPROACH
		//(1) produce wrapper stream from visualized vertices to neighbours that are not visualized but also not layouted outside model position
		DataStream<Row> visualizedVertices = this.vertexStream.filter(new VertexFilterIsVisualized(innerVertices));
		DataStream<Row> neighbours = this.vertexStream.filter(new VertexFilterNotVisualized(innerVertices))
				.filter(new VertexFilterNotLayoutedOutside(layoutedVertices, topModel, rightModel, bottomModel, leftModel));
//		visualizedVertices.addSink(new FlinkRowStreamPrintSink());
//		neighbours.addSink(new FlinkRowStreamPrintSink());
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
		System.out.println("sdfgsdfg");
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
		System.out.println("in ZoomInLayoutThirdStep function");
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		
		//(4) produce wrapper identity stream for vertices which are not yet layouted starting with highest degree
		//    and also produce wrapper stream between those vertices
		DataStream<Row> notLayoutedVertices = this.vertexStream.filter(new VertexFilterLayouted(layoutedVertices));
		Table notLayoutedVerticesTable = fsTableEnv.fromDataStream(notLayoutedVertices).as(this.vertexFields);
		wrapperTable = wrapperTable.join(notLayoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTable = wrapperTable.join(notLayoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable, wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	public DataStream<Row> zoomInLayoutFourthStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> innerVertices, 
			Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		
		//(2) produce wrapper stream from visualized vertices to layouted vertices outside model position
		System.out.println("in ZoomInLayoutFourthStep function");
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		
		Set<String> outsideNeighboursIds = new HashSet<String>();
		for (Map.Entry<String, VertexCustom> innerVerticesEntry : innerVertices.entrySet()) {
			for (Map.Entry<String, String> adjEntry : this.adjMatrix.get(innerVerticesEntry.getKey()).entrySet()) {
				String neighbourVertexId = adjEntry.getKey();
				if (!innerVertices.containsKey(neighbourVertexId) && layoutedVertices.containsKey(neighbourVertexId) 
						&& !this.vertexIsInside(layoutedVertices.get(neighbourVertexId), topModel, rightModel, bottomModel, leftModel)) {
					outsideNeighboursIds.add(neighbourVertexId);
				}
			}
		}
		
		if (outsideNeighboursIds.isEmpty()) return null;
		
		DataStream<String> visualizedVerticesStream = fsEnv.fromCollection(innerVertices.keySet());
		DataStream<String> outsideNeighboursStream = fsEnv.fromCollection(outsideNeighboursIds);
		Table layoutedVerticesTable = fsTableEnv.fromDataStream(visualizedVerticesStream).as("vertexIdGradoop");
		Table neighbourCandidatesTable = fsTableEnv.fromDataStream(outsideNeighboursStream).as("vertexIdGradoop");
		Table wrapperTableNewOld = wrapperTable.join(neighbourCandidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableNewOld = wrapperTableNewOld.join(layoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOldNew = wrapperTable.join(layoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOldNew = wrapperTableOldNew.join(neighbourCandidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTableOldNew, wrapperRowTypeInfo)
				.union(fsTableEnv.toAppendStream(wrapperTableNewOld, wrapperRowTypeInfo));
		
		//filter out already visualized edges in wrapper stream
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		
		return wrapperStream;
	}
	
	public DataStream<Row> panLayoutFirstStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
			Float topModel, Float rightModel, Float bottomModel, Float leftModel) {
		
		//Diese Funktion sollte solange wieder aufgerufen werden, bis die Kapazität erreicht ist. Wenn die Kapazität nicht erreicht wird, sollten Nachbarknoten,
		//welche noch nicht gelayoutet sind hinzugefügt werden. Wenn die Kapazität weiterhinisolierte
		//Knoten nichtzugefügt werden, bis die Kapazität erreich ist. Anschließend müssen noch die Kanten von den eben hinzugefügten
		//Knoten zu Nachbarn außerhalb des Viewports hinzugefügt werden.
		
		System.out.println("in panLayoutFirstStep function");
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);

		// (1) produce wrapper stream from visualized Vertices to neighbours that are not visualized inside but are layouted already
		Set<String> notVisualizedButLayoutedNeighboursIds = new HashSet<String>();
		for (Map.Entry<String, VertexCustom> innerVerticesEntry : newVertices.entrySet()) {
			for (Map.Entry<String, String> adjEntry : this.adjMatrix.get(innerVerticesEntry.getKey()).entrySet()) {
				String notVisualizedVertexId = adjEntry.getKey();
				if (!newVertices.containsKey(notVisualizedVertexId) && layoutedVertices.containsKey(notVisualizedVertexId) 
					&& this.vertexIsInside(layoutedVertices.get(notVisualizedVertexId), topModel, rightModel, bottomModel, leftModel)) {
					notVisualizedButLayoutedNeighboursIds.add(notVisualizedVertexId);
				}
			}
		}
		
		//return to next step if set is empty
		if (notVisualizedButLayoutedNeighboursIds.isEmpty()) return null;
		
		DataStream<String> visualizedVerticesStream = fsEnv.fromCollection(newVertices.keySet());
		DataStream<String> notVisualizedButLayoutedNeighbours = fsEnv.fromCollection(notVisualizedButLayoutedNeighboursIds);
		Table layoutedVerticesTable = fsTableEnv.fromDataStream(visualizedVerticesStream).as("vertexIdGradoop");
		Table neighbourCandidatesTable = fsTableEnv.fromDataStream(notVisualizedButLayoutedNeighbours).as("vertexIdGradoop");
		Table wrapperTableNewOld = wrapperTable.join(neighbourCandidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableNewOld = wrapperTableNewOld.join(layoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOldNew = wrapperTable.join(layoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOldNew = wrapperTableOldNew.join(neighbourCandidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
				
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTableOldNew, wrapperRowTypeInfo)
				.union(fsTableEnv.toAppendStream(wrapperTableNewOld, wrapperRowTypeInfo));
		
		//filter out already visualized edges in wrapper stream
		wrapperStream = wrapperStream.filter(new WrapperFilterVisualizedWrappers(this.visualizedWrappers));
		return wrapperStream;
	}
	
	public DataStream<Row> panLayoutSecondStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
			Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		
		//(5) produce wrapper identity stream for layouted vertices within model position that are not visualized yet
		//	  and also produce wrapper stream between those vertices
		System.out.println("in panLayoutSecondStep function");
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);

		Set<String> notVisualizedButLayoutedNonNeighboursIds = new HashSet<String>();
		for (String key : newVertices.keySet()) System.out.println("inner key " + key);
		for (Map.Entry<String, VertexCustom> layoutedVerticesEntry : layoutedVertices.entrySet()) {
			String vertexId = layoutedVerticesEntry.getKey();
			System.out.println("boolean is inside innerVertices " + newVertices.containsKey(vertexId));
			if (!newVertices.containsKey(vertexId) && this.vertexIsInside(layoutedVerticesEntry.getValue(), 
					topModel, rightModel, bottomModel, leftModel)) {
				System.out.println("panLayoutSecondStep " + vertexId);
				notVisualizedButLayoutedNonNeighboursIds.add(vertexId);
			}

		}
		
		//return to next step if set is empty
		System.out.println("notVisualizedButLayoutedNonNeighboursIds size in panLayoutSecondStep: " + notVisualizedButLayoutedNonNeighboursIds.size());
		System.out.println(notVisualizedButLayoutedNonNeighboursIds.isEmpty());
		if (notVisualizedButLayoutedNonNeighboursIds.isEmpty()) return null;
		
		DataStream<String> notVisualizedButLayoutedNonNeighbours = fsEnv.fromCollection(notVisualizedButLayoutedNonNeighboursIds);
		Table candidatesTable = fsTableEnv.fromDataStream(notVisualizedButLayoutedNonNeighbours).as("vertexIdGradoop");
		wrapperTable = wrapperTable.join(candidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTable = wrapperTable.join(candidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable, wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	public DataStream<Row> panLayoutThirdStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices){
		//Diese Funktion sollte solange wieder aufgerufen werden, bis die Kapazität erreicht ist.  Wenn die Kapazität weiterhin nicht erreicht ist, 
		//sollten isolierte Knoten nichtzugefügt werden, bis die Kapazität erreich ist. Anschließend müssen noch die Kanten von den eben hinzugefügten
		//Knoten zu Nachbarn außerhalb des Viewports hinzugefügt werden.
		
		//(2 + 4) produce wrapper Stream from visualized vertices inside to neighbour vertices which are not layouted and not visualized
		
		System.out.println("in panLayoutThirdStep function");
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);

		Set<String> notVisualizedNotLayoutedNeighboursIds = new HashSet<String>();
		for (Map.Entry<String, VertexCustom> innerVerticesEntry : newVertices.entrySet()) {
			for (Map.Entry<String, String> adjEntry : this.adjMatrix.get(innerVerticesEntry.getKey()).entrySet()) {
				String notLayoutedVertexId = adjEntry.getKey();
				if (!newVertices.containsKey(notLayoutedVertexId) && !layoutedVertices.containsKey(notLayoutedVertexId)) {
					notVisualizedNotLayoutedNeighboursIds.add(notLayoutedVertexId);
				}
			}
		}
		
		//return to next step if set is empty
		if (notVisualizedNotLayoutedNeighboursIds.isEmpty()) return null;
		
		DataStream<String> visualizedVerticesStream = fsEnv.fromCollection(newVertices.keySet());
		DataStream<String> notVisualizedNotLayoutedNeighbours = fsEnv.fromCollection(notVisualizedNotLayoutedNeighboursIds);
		Table layoutedVerticesTable = fsTableEnv.fromDataStream(visualizedVerticesStream).as("vertexIdGradoop");
		Table neighbourCandidatesTable = fsTableEnv.fromDataStream(notVisualizedNotLayoutedNeighbours).as("vertexIdGradoop");
		Table wrapperTableNewOld = wrapperTable.join(neighbourCandidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableNewOld = wrapperTableNewOld.join(layoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOldNew = wrapperTable.join(layoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOldNew = wrapperTableOldNew.join(neighbourCandidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTableOldNew, wrapperRowTypeInfo)
				.union(fsTableEnv.toAppendStream(wrapperTableNewOld, wrapperRowTypeInfo));
		return wrapperStream;
	}
	
	public DataStream<Row> panLayoutFourthStep(Map<String, VertexCustom> layoutedVertices){
		//Anschließend müssen noch die Kanten von den eben hinzugefügten
		//Knoten zu Nachbarn außerhalb des Viewports hinzugefügt werden.
		
		
		
		System.out.println("in panLayoutFourthStep function");
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		
		//(4) produce wrapper identity stream for vertices which are not yet layouted starting with highest degree
		//    and also produce wrapper stream between those vertices
		DataStream<Row> notLayoutedVertices = this.vertexStream.filter(new VertexFilterLayouted(layoutedVertices));
		Table notLayoutedVerticesTable = fsTableEnv.fromDataStream(notLayoutedVertices).as(this.vertexFields);
		wrapperTable = wrapperTable.join(notLayoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTable = wrapperTable.join(notLayoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable, wrapperRowTypeInfo);
		return wrapperStream;
	}
	
	public DataStream<Row> panLayoutFifthStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
			Float top, Float right, Float bottom, Float left, Float xModelDiff, Float yModelDiff){
		
		Float topOld = top - yModelDiff;
		Float rightOld = right - xModelDiff;
		Float bottomOld = bottom - yModelDiff;
		Float leftOld = left - xModelDiff;
		
		//(3) produce wrapperStream from visualized vertices that were newly added inside to layouted Vertices outside the model position
		System.out.println("in panLayoutFifthStep function");
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);

		Set<String> newlyInsideVisualizedIds = new HashSet<String>();
		for (String vertexId : newVertices.keySet()) {
			VertexCustom vertex = layoutedVertices.get(vertexId);
			System.out.println("panLayout5: " + vertex.getIdGradoop() + " " + vertex.getX());
			Integer x = vertex.getX();
			Integer y = vertex.getY();
			if (this.vertexIsInside(vertex, top, right, bottom, left) && 
					!(x >= leftOld && x <= rightOld && y >= topOld && y <= bottomOld)) newlyInsideVisualizedIds.add(vertex.getIdGradoop());
		}
		Set<String> outsideLayoutedNeighbourIds = new HashSet<String>();
		for (String insideVertexId : newlyInsideVisualizedIds) {
			for (Map.Entry<String, String> adjEntry : this.adjMatrix.get(insideVertexId).entrySet()) {
				String neighbourVertexId = adjEntry.getKey();
				if (!newVertices.containsKey(neighbourVertexId)) outsideLayoutedNeighbourIds.add(neighbourVertexId);
			}
		}
		
		if (newlyInsideVisualizedIds.isEmpty() || outsideLayoutedNeighbourIds.isEmpty()) return null;
		
		DataStream<String> newlyInsideVerticesStream = fsEnv.fromCollection(newlyInsideVisualizedIds);
		DataStream<String> notInsideNeighbours = fsEnv.fromCollection(outsideLayoutedNeighbourIds);
		Table newlyInsideVerticesTable = fsTableEnv.fromDataStream(newlyInsideVerticesStream).as("vertexIdGradoop");
		Table neighbourCandidatesTable = fsTableEnv.fromDataStream(notInsideNeighbours).as("vertexIdGradoop");
		Table wrapperTableNewOld = wrapperTable.join(neighbourCandidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableNewOld = wrapperTableNewOld.join(newlyInsideVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOldNew = wrapperTable.join(newlyInsideVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOldNew = wrapperTableOldNew.join(neighbourCandidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTableOldNew, wrapperRowTypeInfo)
				.union(fsTableEnv.toAppendStream(wrapperTableNewOld, wrapperRowTypeInfo));
		return wrapperStream;
	}
	
	public DataStream<Row> zoomOutLayout() {
		//set of visualized vertices and their position is given
		//new position/zoomLevel is sent by client
		//produce wrapper stream of max degree vertices that have already been layouted
		//produce wrapper stream of max degree vertices that have not been layouted. These could be added if they are neighbours of visualized vertices which
			//are close to a sparse region of the viewport (how can this be defined?) AND if their degree is higher than at least one of the current visualized
			//vertices (think about this again later...)
		
		// (1) produce wrapper identity stream for in-new-area layouted, not visualized, non-neighbour vertices until vertices are replaced no more 
				//(all following have lesser degree)
		// (2) produce wrapper Stream from newly inside and visualized vertices to in-new-area layouted but not visualized neighbour vertices and vice versa
		// (3) produce wrapper from newlynew inside and visualized vertices to out-area layouted but not visualized neighbour vertices and vice versa
		
		return null;
	}
	
	public DataStream<Row> zoomOutLayoutFirstStep(Float topModelNew, Float rightModelNew, Float bottomModelNew, Float leftModelNew, 
			Float topModelOld, Float rightModelOld, Float bottomModelOld, Float leftModelOld){
		// (1) produce wrapper identity stream for in-new-area layouted, not visualized, non-neighbour vertices until vertices are replaced no more 
		//(all following have lesser degree)
		
		System.out.println("in zoomOutLayoutFirstStep function");
		DataStream<Row> vertices = this.vertexStream.filter(new VertexFilterInnerNewNotOld(leftModelNew, rightModelNew, topModelNew, bottomModelNew,
				leftModelOld, rightModelOld, topModelOld, bottomModelOld));
		DataStream<Row> wrapperStream = vertices.map(new VertexMapIdentityWrapper());
		return wrapperStream;
		
		//This function needs to be controlled differently (see above)
	}
	
//	public DataStream<Row> zoomOutLayoutSecondStep(Map<String, VertexCustom> layoutedVertices, Map<String, VertexCustom> newVertices, 
//			Float topModelNew, Float rightModelNew, Float bottomModelNew, Float leftModelNew, 
//			Float topModelOld, Float rightModelOld, Float bottomModelOld, Float leftModelOld){
//		// (2) produce wrapper Stream from newly inside and visualized vertices to in-new-area layouted but not visualized neighbour vertices and vice versa
//
//		FilterFunction<Row> vertexFilterInnerNewNotOld = new VertexFilterInnerNewNotOld(leftModelNew, rightModelNew, topModelNew, bottomModelNew,
//				leftModelOld, rightModelOld, topModelOld, bottomModelOld);
//		DataStream<Row> innerNewNotOldVertices = this.vertexStream.filter(vertexFilterInnerNewNotOld);
//		DataStream<Row> newlyVisualizedVertices = innerNewNotOldVertices.filter(new VertexFilterIsVisualized(newVertices));
//		DataStream<Row> neighbourVertices = innerNewNotOldVertices.filter(new VertexFilterLayouted(layoutedVertices)).filter(new VertexFilterNotVisualized);
//		
//		
//		return vertices;
//	}
	
	public DataStream<Row> panLayout() {
		// (1) produce wrapper stream from C to not-visualized but layouted vertices with coordinates newly inside (A) and vice versa
					//!!! PROBABLY THIS IS NOT NECESSARY
//				DataStream<Row> vertexStreamA = this.getVertexStream()
//						.filter(new VertexFilterInnerNewNotOld(null, null, null, null, null, null, null, null))
//						.filter(new VertexFilterLayouted(layoutedVertices));
//				DataStream<String> vertexStreamC = fsEnv.fromCollection(innerVertices.keySet());
//				Table vertexTableA = fsTableEnv.fromDataStream(vertexStreamA).as(this.vertexFields);
//				Table vertexTableC = fsTableEnv.fromDataStream(vertexStreamC).as("vertexIdGradoop");
//				Table wrapperTableSourceA = wrapperTable.join(vertexTableA).where("sourceVertexIdGradoop = vertexIdGradoop").select(this.wrapperFields);
//				Table wrapperTableAToC = wrapperTableSourceA.join(vertexTableC).where("targetVertexIdGradoop = vertexIdGradoop").select(this.wrapperFields);
//				Table wrapperTableSourceC = wrapperTable.join(vertexTableC).where("sourceVertexIdGradoop = vertexIdGradoop").select(this.wrapperFields);
//				Table wrapperTableCToA = wrapperTableSourceC.join(vertexTableA).where("targetVertexIdGradoop = vertexIdGradoop").select(this.wrapperFields);
//					//hier muss noch einmal identity rausgefiltert werden
//				
//				// (2) produce wrapper stream from A to not-Visualized non-layouted neighbours (n1) and vice versa
//				DataStream<String> vertexIdsN1 = vertexStreamA.flatMap(new vertexFlatMapNeighbour(this.adjMatrix))
//						.filter(new VertexIdFilterNotLayouted(layoutedVertices))
//						.filter(new VertexIdFilterNotVisualized(innerVertices.keySet()));
//				Table vertexTableN1 = fsTableEnv.fromDataStream(vertexIdsN1).as("vertexIdGradoop");
//				Table wrapperTableAToN1 = wrapperTableSourceA.join(vertexTableN1).where("targetVertexIdGradoop = vertexIdGradoop").select(this.wrapperFields);
//				Table wrapperTableSourceN1 = wrapperTable.join(vertexTableN1).where("sourceVertexIdGradoop = vertexIdGradoop").select(this.wrapperFields);
//				Table wrapperTableN1ToA = wrapperTableSourceN1.join(vertexTableA).where("targetVertexIdGradoop = vertexIdGradoop").select(this.wrapperFields);
				
				// (3) produce wrapper stream from A to visualized neighbours outside (n2) and vice versa
				
				// (4) produce wrapper stream from n1 to n2 and vice versa
		//set of visualized vertices and their position is given
		//new position is sent by client
		//produce wrapper stream of max degree vertices that have already been layouted in the new region
		//produce wrapper stream of max degree vertices that have not been layouted which are neighbours of those vertices close to the new region
		//produce wrapper stream of max degree vertices that have not been layouted which are neighbours of higher degree vertices that have been layouted
			//close to the new region
		return null;
	}	
}
