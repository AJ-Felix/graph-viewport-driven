package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.HBaseEPGMStore;
import org.gradoop.storage.hbase.impl.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.hbase.impl.io.HBaseDataSink;

public class Gradoop_HBase_Sink {
	GradoopHBaseConfig gra_hbase_cfg;
	Configuration hbase_cfg;
	GradoopFlinkConfig gra_flink_cfg;
	DataSink datasink;
	
	public Gradoop_HBase_Sink(
			GradoopHBaseConfig gra_hbase_cfg,
			Configuration hbase_cfg,
			GradoopFlinkConfig gra_flink_cfg) {
		this.gra_hbase_cfg = gra_hbase_cfg;
		this.hbase_cfg = hbase_cfg;
		this.gra_flink_cfg = gra_flink_cfg;
		
		// create store
		HBaseEPGMStore graphStore = null;
		try {
			graphStore = HBaseEPGMStoreFactory.createOrOpenEPGMStore(this.hbase_cfg, this.gra_hbase_cfg);
		} catch (IOException e) {
			e.printStackTrace();
		}
		this.datasink = new HBaseDataSink(graphStore, gra_flink_cfg);
	}
	
	public void sink(LogicalGraph graph) throws IOException {
		this.datasink.write(graph);
		return;
	}
	
}
