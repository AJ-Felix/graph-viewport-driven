package Temporary;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.log4j.BasicConfigurator;

public class testRun {
	public static void main(String[] args) {
//		BasicConfigurator.configure();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "test");
		DataStream<String> stream = env
			.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties));
		stream.print();

		try {
			env.execute();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
