package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.DataTransformation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class UnitedIDsGroup implements KeySelector<Tuple3<String, String, String>,String>{

	@Override
	public String getKey(Tuple3<String, String, String> value) throws Exception {
		return value.f0.toString();
	}
}
