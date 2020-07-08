package Temporary;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DegreeMatrixLoader {
	public static DataStream<Tuple2<Boolean, Row>> load(
			StreamTableEnvironment fsTableEnv,
			String hbase_table_name,
			Integer top_view_number) {
		fsTableEnv.sqlUpdate(
				"CREATE TABLE loadTempTable (" + 
				"  id STRING," + 
				"  cf ROW<degree INT>" +  
				") WITH (" + 
				"  'connector.type' = 'hbase'," + 
				"  'connector.version' = '1.4.3'," + 
				"  'connector.table-name' = '" + hbase_table_name + "'," +
				"  'connector.zookeeper.quorum' = 'localhost:2181'" +
				")");
		String query = "SELECT * from loadTempTable ORDER BY cf.degree DESC LIMIT " + top_view_number.toString();
		Table table = fsTableEnv.sqlQuery(query);
		RowTypeInfo adjListInfo = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.ROW(Types.INT)}, 
				new String[] {"rowkey", "row"});
		DataStream<Tuple2<Boolean,Row>> ds_tuple_row = fsTableEnv.toRetractStream(table, adjListInfo);
//		DataStream<Row> dsRow = fsTableEnv.toAppendStream(table, adjListInfo);		//only works without ORDER BY
		return ds_tuple_row;
		
	}
	
}
