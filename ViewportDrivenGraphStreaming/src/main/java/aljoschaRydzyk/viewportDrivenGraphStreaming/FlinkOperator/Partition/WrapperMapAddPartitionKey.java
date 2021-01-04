package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Partition;

import java.math.BigInteger;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple17;
import org.apache.flink.api.java.tuple.Tuple18;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class WrapperMapAddPartitionKey implements FlatMapFunction<
	Tuple17<
		String,
		String,Long,String,Integer,Integer,Long,Integer,
		String,Long,String,Integer,Integer,Long,Integer,
		String,String>,
	Tuple2<BigInteger,Row>>{

	@Override
	public void flatMap(
			Tuple17<
				String,
				String,Long,String,Integer,Integer,Long,Integer,
				String,Long,String,Integer,Integer,Long,Integer,
				String,String> value,
			Collector<Tuple2<BigInteger, Row>> out) throws Exception {
		BigInteger gradoopSourceIntegerId = new BigInteger(value.f1, 16);
		BigInteger gradoopTargetIntegerId = new BigInteger(value.f8, 16);
		out.collect(Tuple2.of(gradoopSourceIntegerId, Row.of(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, value.f7, value.f8, 
				value.f9, value.f10, value.f11, value.f12, value.f13, value.f14, value.f15, value.f16)));
		out.collect(Tuple2.of(gradoopTargetIntegerId, Row.of(value.f0, value.f1, value.f2, value.f3, value.f4, value.f5, value.f6, value.f7, value.f8, 
				value.f9, value.f10, value.f11, value.f12, value.f13, value.f14, value.f15, value.f16)));	
	}		
}
