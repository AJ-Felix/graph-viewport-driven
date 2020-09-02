package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.File;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

//graphIdGradoop ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; edgeIdGradoop ; edgeLabel

public class CSVGraphUtilJoin implements GraphUtil{
	private StreamExecutionEnvironment fsEnv;
	private StreamTableEnvironment fsTableEnv;
	private String inPath;
	private DataStream<Row> wrapperStream = null;
	private DataStreamSource<Row> vertexStream = null;
	private Set<String> visualizedWrappers;
	private Set<String> visualizedVertices;
	
	public CSVGraphUtilJoin(StreamExecutionEnvironment fsEnv, StreamTableEnvironment fsTableEnv, String inPath) {
		this.fsEnv = fsEnv;
		this.fsTableEnv = fsTableEnv;
		this.inPath = inPath;
	}
	
	public void setVisualizedWrappers(Set<String> visualizedWrappers) {
		this.visualizedWrappers = visualizedWrappers;
	}
	
	public void setVisualizedVertices(Set<String> visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	@Override
	public DataStream<Row> initializeStreams(){
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
	
	public DataStream<Row> getWrapperStream() {
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

	public DataStream<Row> getVertexStream() {
		return this.vertexStream;
	}
	
	public DataStream<Row> zoom (Float topModel, Float rightModel, Float bottomModel, Float leftModel){
		DataStream<Row> vertexStreamInner = this.vertexStream
			.filter(new FilterFunction<Row>(){
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (leftModel <= x) &&  (x <= rightModel) && (topModel <= y) && (y <= bottomModel);
			}
		});
		String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
					+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
					+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
		String vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree";
		Table vertexTable = fsTableEnv.fromDataStream(vertexStreamInner).as(vertexFields);
		DataStream<Row> wrapperStream = this.wrapperStream;
		Set<String> visualizedWrappers = this.visualizedWrappers;
		wrapperStream = wrapperStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !visualizedWrappers.contains(value.getField(13)); 
			}
		});
		Set<String> visualizedVertices = this.visualizedVertices;
		wrapperStream = wrapperStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !(visualizedVertices.contains(value.getField(2).toString()) && value.getField(14).equals("identityEdge"));
			}
		});
		Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		DataStream<Row> vertexStreamOuter = this.vertexStream
			.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftModel > x) || (x > rightModel) || (topModel > y) || (y > bottomModel);
				}
			});
		Table vertexTableOuter = fsTableEnv.fromDataStream(vertexStreamOuter).as(vertexFields);
		Table wrapperTableInOut = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTableOuter).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		Table wrapperTableOutIn = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTableOuter).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		wrapperStream = fsTableEnv.toAppendStream(wrapperTable, typeInfo).union(fsTableEnv.toAppendStream(wrapperTableInOut, typeInfo))
				.union(fsTableEnv.toAppendStream(wrapperTableOutIn, typeInfo));
		return wrapperStream;
	}
	
	public DataStream<Row> pan(Float topOld, Float rightOld, Float bottomOld, Float leftOld, Float xModelDiff, Float yModelDiff){
		Float topNew = topOld + yModelDiff;
		Float rightNew = rightOld + xModelDiff;
		Float bottomNew = bottomOld + yModelDiff;
		Float leftNew = leftOld + xModelDiff;
		DataStream<Row> vertexStreamInner = this.vertexStream
			.filter(new FilterFunction<Row>(){
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftNew <= x) &&  (x <= rightNew) && (topNew <= y) && (y <= bottomNew);
				}
			});
		DataStream<Row> vertexStreamInnerNew = vertexStreamInner.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld);
				}
			});
		DataStream<Row> vertexStreamOldOuterExtend = this.vertexStream
			.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return ((leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld)) && 
							((leftNew > x) || (x > rightNew) || (topNew > y) || (y > bottomNew));
				}
			});
		DataStream<Row> vertexStreamOldInnerNotNewInner = this.vertexStream
			.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftOld <= x) && (x <= rightOld) && (topOld <= y) && (y <= bottomOld) && 
							((leftNew > x) || (x > rightNew) || (topNew > y) || (y > bottomNew));
				}
			});
		String vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree";
		String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
				+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
				+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
		Table vertexTableInnerNew = fsTableEnv.fromDataStream(vertexStreamInnerNew).as(vertexFields);
		Table vertexTableOldOuterExtend = fsTableEnv.fromDataStream(vertexStreamOldOuterExtend).as(vertexFields);
		Table vertexTableOldInNotNewIn = fsTableEnv.fromDataStream(vertexStreamOldInnerNotNewInner).as(vertexFields);
		Table vertexTableInner = fsTableEnv.fromDataStream(vertexStreamInner).as(vertexFields);
		DataStream<Row> wrapperStream = this.wrapperStream;
		Set<String> visualizedWrappers = this.visualizedWrappers;
		Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
		Table wrapperTableInOut = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTableOldOuterExtend).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		Table wrapperTableOutIn = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTableOldOuterExtend).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		Table wrapperTableInIn = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableInIn = wrapperTableInIn.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		Table wrapperTableOldInNewInInOut = wrapperTable.join(vertexTableInner)
				.where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableOldInNewInInOut = wrapperTableOldInNewInInOut.join(vertexTableOldInNotNewIn)
				.where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		Table wrapperTableOldInNewInOutIn = wrapperTable.join(vertexTableOldInNotNewIn)
				.where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableOldInNewInOutIn = wrapperTableOldInNewInOutIn.join(vertexTableInner)
				.where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		
		//filter out redundant identity edges
		DataStream<Row> wrapperStreamInIn = fsTableEnv.toAppendStream(wrapperTableInIn, typeInfo)
			.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					return !(value.getField(14).equals("identityEdge"));
				}
			});
		
		//filter out already visualized edges
		DataStream<Row> wrapperStreamOldInNewIn = fsTableEnv.toAppendStream(wrapperTableOldInNewInInOut, typeInfo)
				.union(fsTableEnv.toAppendStream(wrapperTableOldInNewInOutIn, typeInfo));	
		wrapperStreamOldInNewIn = wrapperStreamOldInNewIn.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !(visualizedWrappers.contains(value.getField(13)));
			}
		});

		wrapperStream = wrapperStreamInIn
				.union(fsTableEnv.toAppendStream(wrapperTableOutIn, typeInfo))
				.union(wrapperStreamOldInNewIn)
				.union(fsTableEnv.toAppendStream(wrapperTableInOut, typeInfo));
		return wrapperStream;
	}
}
