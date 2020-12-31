package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Partition;

import java.math.BigInteger;
import java.util.Map;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;

public class AdjacencyMatrixPartitioner implements Partitioner<String>{

//	@Override
//	public int partition(Tuple2<String, Map<String, String>> key, int numPartitions) {
//		BigInteger gradoopIntegerId = new BigInteger(key.f0, 16);
//		int partitionKey = gradoopIntegerId.remainder(new BigInteger(String.valueOf(numPartitions), 10)).intValue();
//		return partitionKey;
//	}

	@Override
	public int partition(String key, int numPartitions) {
		BigInteger gradoopIntegerId = new BigInteger(key, 16);
		int partitionKey = gradoopIntegerId.remainder(new BigInteger(String.valueOf(numPartitions), 10)).intValue();
		return partitionKey;
	}

}
