package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.File;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

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
	public DataStream<Row> produceWrapperStream() {
		Path wrappersFilePath = Path.fromLocalFile(new File(this.inPath + "_wrappers"));
		RowCsvInputFormat wrappersFormat = new RowCsvInputFormat(wrappersFilePath, new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		wrappersFormat.setFieldDelimiter(";");
		this.wrapperStream = this.fsEnv.readFile(wrappersFormat, this.inPath + "_wrappers").setParallelism(1);
		return this.wrapperStream;
	}

	@Override
	public DataStream<Row> getWrapperStream() {
		return this.wrapperStream;
	}

}
