package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TimeOut extends KeyedProcessFunction<Object,Row, Boolean> {
	 
	  // delay after which an alert flag is thrown
	  private final long timeOut;
	  // state to remember the last timer set
	  private transient ValueState<Long> lastTimer;

	  public TimeOut(long timeOut) {
	    this.timeOut = timeOut;
	  }

	  @Override
	  public void open(Configuration conf) {
	    // setup timer state
	    ValueStateDescriptor<Long> lastTimerDesc = 
	      new ValueStateDescriptor<Long>("lastTimer", Long.class);
	    lastTimer = getRuntimeContext().getState(lastTimerDesc);
	  }

	  @Override
	  public void onTimer(long timestamp, OnTimerContext ctx, Collector<Boolean> out) throws Exception {
	    System.out.println("onTimer: " + timestamp);	    
//	    System.out.println("onTimer, currentWatermark: " + ctx.timerService().currentWatermark());
	    // check if this was the last timer we registered
	    if (timestamp == lastTimer.value()) {
	      // it was, so no data was received afterwards.
	      // fire an alert.
	      System.out.println("firing value is: " + timestamp);
	      out.collect(true);
	    }
//		if (ctx.timerService().currentWatermark() == Long.MAX_VALUE) System.out.println("time is now!");
	  }

		@Override
		public void processElement(Row arg0, KeyedProcessFunction<Object, Row, Boolean>.Context arg1,
				Collector<Boolean> arg2) throws Exception {
			// get current time and compute timeout time
		    long currentTime = arg1.timerService().currentProcessingTime();
		    long timeoutTime = currentTime + timeOut;
		    // register timer for timeout time
		    arg1.timerService().registerEventTimeTimer(timeoutTime);
		    // remember timeout time
		    lastTimer.update(timeoutTime);
			System.out.println("item processed at processing time: " + arg1.timerService().currentProcessingTime());
//			System.out.println(arg1.timestamp());
//			System.out.println("process function, current watermark: " + arg1.timerService().currentWatermark());
//			arg1.timerService().registerEventTimeTimer(arg1.timestamp() + 100);
			arg2.collect(false);
		}
	}