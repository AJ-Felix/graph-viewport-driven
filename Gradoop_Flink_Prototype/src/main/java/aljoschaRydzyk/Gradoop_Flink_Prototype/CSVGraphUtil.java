package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.File;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

//graphIdGradoop ; edgeIdGradoop ; edgeLabel ; sourceIdGradoop ; sourceIdNumeric ; sourceLabel ; sourceX ; sourceY ; targetIdGradoop ; targetIdNumeric ; targetLabel ; targetX ; targetY

public class CSVGraphUtil implements GraphUtil{
	public static DataStream<Row> produceWrapperStream(StreamExecutionEnvironment fsEnv, String inPath){
		Path filePath = Path.fromLocalFile(new File(inPath));
		RowCsvInputFormat format = new RowCsvInputFormat(filePath, new TypeInformation[] {Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT, 
				Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG});
		format.setFieldDelimiter(";");
		DataStreamSource<Row> stream = fsEnv.readFile(format, inPath).setParallelism(1);
		return stream;
	}

	@Override
	public DataStream<VVEdgeWrapper> produceWrapperStream() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataStream<VVEdgeWrapper> getWrapperStream() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
