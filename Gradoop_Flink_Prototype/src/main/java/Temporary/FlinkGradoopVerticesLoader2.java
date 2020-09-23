package Temporary;


import java.nio.ByteBuffer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

public class FlinkGradoopVerticesLoader2 {
	public static DataStream<Tuple2<Boolean, Row>> load(
			StreamTableEnvironment fsTableEnv,
			Integer top_view_number) {
		fsTableEnv.sqlUpdate(
				"CREATE TABLE loadTempTable (" + 
				"  id BYTES," + 
				"  p_value ROW<degree BYTES>" +  
				") WITH (" + 
				"  'connector.type' = 'hbase'," + 
				"  'connector.version' = '1.4.3'," + 
				"  'connector.table-name' = 'vertices'," +
				"  'connector.zookeeper.quorum' = 'localhost:2181'" +
				")");
		String query = "SELECT * from loadTempTable ORDER BY p_value.degree DESC LIMIT " + top_view_number.toString();
		Table table = fsTableEnv.sqlQuery(query);
		RowTypeInfo adjListInfo = new RowTypeInfo(new TypeInformation[]{LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(DataTypes.BYTES()), 
				Types.ROW(LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(DataTypes.BYTES()))}, 
				new String[] {"rowkey", "row"});
		DataStream<Tuple2<Boolean,Row>> vertexStream = fsTableEnv.toRetractStream(table, adjListInfo);
		TupleTypeInfo<Tuple2<Boolean, Row>> tupleInfo = 
				new TupleTypeInfo<Tuple2<Boolean,Row>>(new TypeInformation[] {Types.BOOLEAN, Types.ROW(Types.STRING, Types.LONG)});
		DataStream<Tuple2<Boolean,Row>> vertexStreamOutput = vertexStream.map(new MapFunction<Tuple2<Boolean, Row>, Tuple2<Boolean, Row>>(){
			@Override
			public Tuple2<Boolean, Row> map(Tuple2<Boolean, Row> value) throws Exception {
				String id = StringUtils.byteToHexString((byte[]) value.f1.getField(0));
				byte[] arr = (byte[]) ((Row) value.f1.getField(1)).getField(0);
				ByteBuffer wrapped = ByteBuffer.wrap(arr);
				long degree = wrapped.getLong();
				return value;
			}
		}).returns(tupleInfo).setParallelism(1);
		return vertexStreamOutput;
	}
}
