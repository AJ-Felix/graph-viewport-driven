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
		out.collect(Tuple2.of(gradoopSourceIntegerId, Row.of(value)));
		out.collect(Tuple2.of(gradoopTargetIntegerId, Row.of(value)));	
	}		
}
