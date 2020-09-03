package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.types.Row;

public class WrapperFilterIdentity implements FilterFunction<Row> {
	@Override
	public boolean filter(Row value) throws Exception {
		return !(value.getField(14).equals("identityEdge"));
	}
}
