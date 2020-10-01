package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
	public DataStream<Row> pan(Float topOld, Float rightOld, Float bottomOld, Float leftOld, Float xModelDiff, Float yModelDiff){
		Float topNew = topOld + yModelDiff;
		Float rightNew = rightOld + xModelDiff;
		Float bottomNew = bottomOld + yModelDiff;
		Float leftNew = leftOld + xModelDiff;
		
		//vertex stream filter and conversion to Flink Tables for areas A, B, C and D
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
		
		//produce wrapperStream from B to D and vice versa
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
	
	//Prelayout functions
	public DataStream<Row> zoomInLayout(Set<String> layoutedVertices, Map<String, VertexCustom> innerVertices) {
		System.out.println("in ZoomInLayout function");
		RowTypeInfo wrapperRowTypeInfo = new RowTypeInfo(this.wrapperFormatTypeInfo);
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(this.wrapperFields);
		
		Set<String> NotVisualizedNeighboursIds = new HashSet<String>();
		for (Map.Entry<String, VertexCustom> innerVerticesEntry : innerVertices.entrySet()) {
			for (Map.Entry<String, String> adjEntry : this.adjMatrix.get(innerVerticesEntry.getKey()).entrySet()) {
				String notVisualizedVertexId = adjEntry.getKey();
				if (!innerVertices.containsKey(notVisualizedVertexId)) NotVisualizedNeighboursIds.add(notVisualizedVertexId);
			}
		}
		DataStream<String> layoutedVerticesStream = fsEnv.fromCollection(layoutedVertices);
		DataStream<String> neighbourCandidates = fsEnv.fromCollection(NotVisualizedNeighboursIds);
		Table layoutedVerticesTable = fsTableEnv.fromDataStream(layoutedVerticesStream).as("vertexIdGradoop");
		Table neighbourCandidatesTable = fsTableEnv.fromDataStream(neighbourCandidates).as("vertexIdGradoop");
		Table wrapperTableNewOld = wrapperTable.join(neighbourCandidatesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableNewOld = wrapperTableNewOld.join(layoutedVerticesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		Table wrapperTableOldNew = wrapperTable.join(layoutedVerticesTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(this.wrapperFields);
		wrapperTableOldNew = wrapperTableOldNew.join(neighbourCandidatesTable).where("vertexIdGradoop = targetVertexIdGradoop").select(this.wrapperFields);
		
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTableOldNew, wrapperRowTypeInfo)
				.union(fsTableEnv.toAppendStream(wrapperTableNewOld, wrapperRowTypeInfo));
		return wrapperStream;
		
		
		//map of innerVertices and their position is given
		//set of layouted vertices and their position is given
		//new position/zoomLevel is sent by client
		//delete those vertices from the innerVertices map that are newly outside the viewport, this is done in prepareOperation()
		//after prepareOperation() this function
			//produce vertex stream of neighbour vertices from those still/already visualized
				//produce wrapper stream of candidates to vertices already layouted 
			//when there is still space for visualization produce wrapper stream from the neighbours neighbours and so on, 
			//hopefully this can be controlled by consecutive execution of flink jobs
	}
	
	public DataStream<Row> zoomOutLayout() {
		//set of visualized vertices and their position is given
		//new position/zoomLevel is sent by client
		//produce wrapper stream of max degree vertices that have already been layouted
		//produce wrapper stream of max degree vertices that have not been layouted. These could be added if they are neighbours of visualized vertices which
			//are close to a sparse region of the viewport (how can this be defined?) AND if their degree is higher than at least one of the current visualized
			//vertices (think about this again later...)
		return null;
	}
	
	public DataStream<Row> panLayout() {
		//set of visualized vertices and their position is given
		//new position is sent by client
		//produce wrapper stream of max degree vertices that have already been layouted in the new region
		//produce wrapper stream of max degree vertices that have not been layouted which are neighbours of those vertices close to the new region
		//produce wrapper stream of max degree vertices that have not been layouted which are neighbours of higher degree vertices that have been layouted
			//close to the new region
		return null;
	}	
}
