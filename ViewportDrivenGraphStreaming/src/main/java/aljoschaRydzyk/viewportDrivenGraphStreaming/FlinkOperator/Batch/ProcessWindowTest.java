package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.Batch;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


public class ProcessWindowTest extends ProcessWindowFunction<Row,Row,Integer,GlobalWindow>{

	@Override
	public void process(Integer arg0, ProcessWindowFunction<Row, Row, Integer, GlobalWindow>.Context arg1,
			Iterable<Row> arg2, Collector<Row> arg3) throws Exception {
		for (Row row : arg2) arg3.collect(row);
	}

}
