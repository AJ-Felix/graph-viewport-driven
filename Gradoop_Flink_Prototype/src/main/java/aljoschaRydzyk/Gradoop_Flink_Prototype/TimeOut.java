package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TimeOut extends KeyedProcessFunction<Object,Row, Boolean> {

	  @Override
	  public void open(Configuration conf) throws Exception {
		  super.open(conf);
	  }

	  @Override
	  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Boolean> out) throws Exception {
	    System.out.println("onTimer: " + timestamp);
		if (ctx.timerService().currentWatermark() == Long.MAX_VALUE) System.out.println("time is now!");
	  }

		@Override
		public void processElement(Row arg0, KeyedProcessFunction<Object, Row, Boolean>.Context arg1,
				Collector<Boolean> arg2) throws Exception {
			// TODO Auto-generated method stub
			System.out.println("item processd");
			arg1.timerService().registerEventTimeTimer(100);
		}
	}