package Temporary;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.HBaseEPGMStore;
import org.gradoop.storage.hbase.impl.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.hbase.impl.io.HBaseDataSink;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class Gradoop_GraphBuilder {
	
	public static void main(String[] args) {
		
//		System.setProperty("hadoop.home.dir", "/home/aljoscha/hadoop-2.7.7");
//		System.setProperty("HADOOP_HOME", "/home/aljoscha/hadoop-2.7.7");
//		System.setProperty("mapred.output.dir", "/home/aljoscha/hadoop-2.7.7");
		BasicConfigurator.configure();
//		System.out.println(System.getenv("HADOOP_HOME"));
//		
//
//		RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
//		List<String> arguments = runtimeMxBean.getInputArguments();
//		for (int i = 0; i < arguments.size(); i++) {
//			System.out.println(arguments.get(i));
//		}

		//create gradoop Flink configuration
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);
			
		//build Graph
			String graph = "g1:graph[" + 
					  "(p1:Person {name: \"Bob\", age: 24})-[:friendsWith]->" +
					  "(p2:Person{name: \"Alice\", age: 30})-[:friendsWith]->(p1)" +
					  "(p2)-[:friendsWith]->(p3:Person {name: \"Jacob\", age: 27})-[:friendsWith]->(p2) " +
					  "(p3)-[:friendsWith]->(p4:Person{name: \"Marc\", age: 40})-[:friendsWith]->(p3) " +
					  "(p4)-[:friendsWith]->(p5:Person{name: \"Sara\", age: 33})-[:friendsWith]->(p4) " +
					  "(c1:Company {name: \"Acme Corp\"}) " +
					  "(c2:Company {name: \"Globex Inc.\"}) " +
					  "(p2)-[:worksAt]->(c1) " +
					  "(p4)-[:worksAt]->(c1) " +
					  "(p5)-[:worksAt]->(c1) " +
					  "(p1)-[:worksAt]->(c2) " +
					  "(p3)-[:worksAt]->(c2) " + "] " +
					  "g2:graph[" +
					  "(p4)-[:friendsWith]->(p6:Person {name: \"Paul\", age: 37})-[:friendsWith]->(p4) " +
					  "(p6)-[:friendsWith]->(p7:Person {name: \"Mike\", age: 23})-[:friendsWith]->(p6) " +
					  "(p8:Person {name: \"Jil\", age: 32})-[:friendsWith]->(p7)-[:friendsWith]->(p8) " +
					  "(p6)-[:worksAt]->(c2) " +
					  "(p7)-[:worksAt]->(c2) " +
					  "(p8)-[:worksAt]->(c1) " + "]";
			
			FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(cfg);
			loader.initDatabaseFromString(graph);
			
			
			// create gradoop HBase configuration
			GradoopHBaseConfig config = GradoopHBaseConfig.getDefaultConfig();
			Configuration hbconfig = HBaseConfiguration.create();
		
			
//			try {
//				System.out.println(hbconfig.getResource("hbase-default.xml").getPath());
//				System.out.println(hbconfig.getResource("core-default.xml").getPath());
//				Configuration.dumpConfiguration(hbconfig, new PrintWriter(System.out));
//				System.out.println();
//			} catch (IOException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
//			
//			hbconfig.addResource(new Path("/home/aljoscha/hadoop-2.7.7/etc/hadoop/core-site.xml"));
//			hbconfig.addResource(new Path("/home/aljoscha/hadoop-2.7.7/etc/hadoop/hdfs-site.xml"));
//			hbconfig.addResource(new Path("/home/aljoscha/hadoop-2.7.7/etc/hadoop/mapred-site.xml"));
//			hbconfig.addResource(new Path("/home/aljoscha/hadoop-2.7.7/etc/hadoop/yarn-site.xml"));
//			hbconfig.addResource(new Path("/home/aljoscha/hbase-1.4.3/conf/hbase-site.xml"));
//			System.out.println();
//			System.out.println(hbconfig.get("mapreduce.output.fileoutputformat.outputdir"));
//			System.out.println(hbconfig.get("mapred.output.dir"));
//			System.out.println(hbconfig.get("hadoop.tmp.dir"));
//			
//
//			hbconfig.reloadConfiguration();
//			String[] ar = hbconfig.getPropertySources("hadoop.tmp.dir");
//			for (int i = 0; i < ar.length; i++) {
//				System.out.println(ar[i]);
//			}
//			try {
//					Configuration.dumpConfiguration(hbconfig, new PrintWriter(System.out));
//				} catch (IOException e1) {
//					// TODO Auto-generated catch block
//					e1.printStackTrace();
//				}
				
			// create store
			HBaseEPGMStore graphStore = null;
			try {
				graphStore = HBaseEPGMStoreFactory.createOrOpenEPGMStore(hbconfig, config);
			} catch (IOException e) {
				e.printStackTrace();
			}
			DataSink datasink = new HBaseDataSink(graphStore, cfg);
			
			
			Collection<EPGMVertex> vertices = loader.getVertices();
			Iterator<EPGMVertex> iter = vertices.iterator();
			while (iter.hasNext()) {
				System.out.print(iter.next().toString());
				System.out.print("\n");
			}
			
			try {
				datasink.write(loader.getLogicalGraphByVariable("g1"));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				env.execute("test");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
}
