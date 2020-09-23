package Temporary;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import aljoschaRydzyk.Gradoop_Flink_Prototype.GraphUtil;

//graphIdGradoop ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; sourceDegree
//targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY ; targetDegree ; edgeIdGradoop ; edgeLabel

public class CSVGraphUtilMap implements GraphUtil{
	private StreamExecutionEnvironment fsEnv;
	private String inPath;
	private DataStream<Row> wrapperStream = null;
	
	public CSVGraphUtilMap(StreamExecutionEnvironment fsEnv, String inPath) {
		this.fsEnv = fsEnv;
		this.inPath = inPath;
	}
	
	@Override
	public void initializeStreams() {
		Path wrappersFilePath = Path.fromLocalFile(new File(this.inPath + "_wrappers"));
		RowCsvInputFormat wrappersFormat = new RowCsvInputFormat(wrappersFilePath, new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		wrappersFormat.setFieldDelimiter(";");
		this.wrapperStream = this.fsEnv.readFile(wrappersFormat, this.inPath + "_wrappers").setParallelism(1);

	}

	@Override
	public DataStream<Row> getVertexStream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataStream<Row> zoom(Float topModel, Float rightModel, Float bottomModel, Float leftModel)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataStream<Row> pan(Float topOld, Float rightOld, Float bottomOld, Float leftOld, Float xModelDiff,
			Float yModelDiff) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setVisualizedVertices(Set<String> visualizedVertices) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setVisualizedWrappers(Set<String> visualizedWrappers) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Map<String, String>> buildAdjacencyMatrix() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Map<String, String>> getAdjMatrix() {
		// TODO Auto-generated method stub
		return null;
	}

}
