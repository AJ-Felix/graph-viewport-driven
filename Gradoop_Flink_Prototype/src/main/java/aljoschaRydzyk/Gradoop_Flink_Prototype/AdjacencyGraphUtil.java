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

public class AdjacencyGraphUtil implements GraphUtil{
	private StreamExecutionEnvironment fsEnv;
	private StreamTableEnvironment fsTableEnv;
	private String inPath;
	private DataStream<Row> wrapperStream = null;
	private DataStreamSource<Row> vertexStream = null;
	private Map<String,Map<String,Boolean>> adjMatrix;
	
	public AdjacencyGraphUtil(StreamExecutionEnvironment fsEnv, StreamTableEnvironment fsTableEnv, String inPath) {
		this.fsEnv = fsEnv;
		this.fsTableEnv = fsTableEnv;
		this.inPath = inPath;
	}

	@Override
	public DataStream<Row> produceWrapperStream() throws Exception {
		Path wrappersFilePath = Path.fromLocalFile(new File(this.inPath + "_wrappers"));
		RowCsvInputFormat wrappersFormatIdentity = new RowCsvInputFormat(wrappersFilePath, new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		wrappersFormatIdentity.setFieldDelimiter(";");
		RowCsvInputFormat wrappersFormat = new RowCsvInputFormat(wrappersFilePath, new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		wrappersFormat.setFieldDelimiter(";");
		DataStream<Row> wrapperStreamIdentity = this.fsEnv.readFile(wrappersFormatIdentity, this.inPath + "_vertices").setParallelism(1);
		this.wrapperStream = wrapperStreamIdentity.union(this.fsEnv.readFile(wrappersFormat, this.inPath + "_wrappers").setParallelism(1));
		Path verticesFilePath = Path.fromLocalFile(new File(this.inPath + "_vertices"));
		RowCsvInputFormat verticesFormat = new RowCsvInputFormat(verticesFilePath, new TypeInformation[] {Types.STRING, Types.STRING, Types.INT, Types.STRING, 
				Types.INT, Types.INT, Types.LONG});
		verticesFormat.setFieldDelimiter(";");
		this.vertexStream = this.fsEnv.readFile(verticesFormat, this.inPath + "_vertices").setParallelism(1);
		return this.wrapperStream;
	}
	
	public DataStream<Row> getMaxDegreeSubset(Integer numberVertices){
		DataStream<Row> filteredVertices = this.vertexStream.filter(new FilterFunction<Row>(){
			@Override
			public boolean filter(Row value) throws Exception {
				if ((Integer) value.getField(2) < numberVertices) return true;
				else return false;
			}
		});
		String vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, vertexX, vertexY, vertexDegree";
		Table vertexTable = fsTableEnv.fromDataStream(filteredVertices).as(vertexFields);
		String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
				+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
				+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
		Table wrapperTable = fsTableEnv.fromDataStream(this.wrapperStream).as(wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG
				, Types.STRING, Types.STRING});
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable, typeInfo);
		return wrapperStream;
	}
	
	public Map<String,Map<String,Boolean>> buildAdjacencyMatrix() throws IOException {
		this.adjMatrix = new HashMap<String, Map<String,Boolean>>();
		BufferedReader csvReader = new BufferedReader(new FileReader(this.inPath + "_adjacency"));
		String row;
		while ((row = csvReader.readLine()) != null) {
		    String[] arr = row.split(";");
		    String vertexIdRow = arr[0];
		    String[] vertexRows = Arrays.copyOfRange(arr, 1, arr.length);
		    Map<String,Boolean> map = new HashMap<String,Boolean>();
		    for (String column: vertexRows) {
		    	String[] entry = column.split(","); 
		    	if (entry[1].equals("0")) {
			    	map.put(entry[0], false);
		    	} else {
		    		map.put(entry[0], true);
		    	}
		    }
		    adjMatrix.put(vertexIdRow, map);
		}
		csvReader.close();
		return adjMatrix;
	}
	
	public DataStream<Row> zoom(Float topModel, Float rightModel, Float bottomModel, Float leftModel, Set<String> visualizedWrappers, Set<String> visualizedVertices) {
		DataStream<Row> vertexStreamInner = this.getVertexStream().filter(new FilterFunction<Row>(){
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (leftModel <= x) &&  (x <= rightModel) && (topModel <= y) && (y <= bottomModel);
			}
		});
		DataStream<Row> wrapperStream = this.getWrapperStream();
		String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
				+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
				+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
		String vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree";
		Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream, wrapperFields);
		Table vertexInnerTable = fsTableEnv.fromDataStream(vertexStreamInner, vertexFields);
		Table wrapperTableInOut = wrapperTable.join(vertexInnerTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		Table wrapperTableOutIn = wrapperTable.join(vertexInnerTable).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		wrapperStream = fsTableEnv.toAppendStream(wrapperTableInOut, typeInfo).union(fsTableEnv.toAppendStream(wrapperTableOutIn, typeInfo));
		Map<String, Map<String, Boolean>> adjMatrix;
		try {
			adjMatrix = this.buildAdjacencyMatrix();
		} catch (IOException e) {
			e.printStackTrace();
		}
		wrapperStream = wrapperStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return adjMatrix.get(value.getField(1)).get(value.getField(7));
			}
		});
		wrapperStream = wrapperStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !(visualizedVertices.contains(value.getField(2).toString()) && value.getField(14).equals("identityEdge"));
			}
		});
		return wrapperStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !(visualizedWrappers.contains(value.getField(13)));
			}
		});
	}

	@Override
	public DataStream<Row> getWrapperStream() {
		return this.wrapperStream;
	}

	@Override
	public DataStream<Row> getVertexStream() {
		return this.vertexStream;
	}

}
