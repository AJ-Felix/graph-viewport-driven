package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Partition;

import java.math.BigInteger;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

public class WrapperMapPartitioner implements Partitioner<BigInteger>{

//	@Override
//	public int partition(Tuple2<BigInteger,Row> key, int numPartitions) {
//		int partitionKey = key.f0.remainder(new BigInteger(String.valueOf(numPartitions), 10)).intValue();
//		return partitionKey;
//	}

	@Override
	public int partition(BigInteger key, int numPartitions) {
		int partitionKey = key.remainder(new BigInteger(String.valueOf(numPartitions), 10)).intValue();
		return partitionKey;
	}

}
