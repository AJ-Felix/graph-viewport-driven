package Temporary;

import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.BasicConfigurator;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMVertexFactory;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.HBaseEPGMStore;
import org.gradoop.storage.hbase.impl.factory.HBaseEPGMStoreFactory;

import com.google.protobuf.ServiceException;

public class Gradoop_HBase {
public static void main(String[] args) {
		
		BasicConfigurator.configure();
	
		// create gradoop HBase configuration
		GradoopHBaseConfig config = GradoopHBaseConfig.getDefaultConfig();

		// create HBase configuration
		System.out.print("Step zero");

		Configuration hbconfig = HBaseConfiguration.create();

		try {
			Configuration.dumpConfiguration(hbconfig, new PrintWriter(System.out));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.print(hbconfig.getResource("hbase-default.xml") + "\n"); 
		System.out.print(hbconfig.get("hbase.zookeeper.quorum") + "\n");
		System.out.print(hbconfig.get("hbase.zookeeper.property.clientPort") + "\n");
		//hbconfig.clear();
		hbconfig.set("hbase.zookeeper.quorum", "localhost");
		System.out.print(hbconfig.get("hbase.zookeeper.quorum")+ "\n");
		System.out.print(hbconfig.get("hbase.rootdir")+ "\n");
		System.out.print(hbconfig.get("hbase.master.port"));
		try {
			HBaseAdmin.checkHBaseAvailable(hbconfig);
		} catch (MasterNotRunningException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ZooKeeperConnectionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ServiceException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.print("First step");
		// create store
		HBaseEPGMStore graphStore = null;
		try {
			System.out.print("2");
			graphStore = HBaseEPGMStoreFactory
			    .createOrOpenEPGMStore(hbconfig, config);
			System.out.print("3");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//add Vertex
		EPGMVertexFactory factory = new EPGMVertexFactory();
		EPGMVertex vertex = factory.createVertex("test_label");
		try {
			graphStore.writeVertex(vertex);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			graphStore.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
