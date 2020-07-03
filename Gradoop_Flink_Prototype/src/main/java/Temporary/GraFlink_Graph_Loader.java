package Temporary;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.HBaseEPGMStore;
import org.gradoop.storage.hbase.impl.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.hbase.impl.io.HBaseDataSource;

public class GraFlink_Graph_Loader {
	GradoopFlinkConfig gra_flink_cfg;
	GradoopHBaseConfig gra_hbase_cfg;
	Configuration hbase_cfg;

	public GraFlink_Graph_Loader(
		GradoopFlinkConfig gra_flink_cfg,
		GradoopHBaseConfig gra_hbase_cfg,
		Configuration hbase_cfg) {
		
		this.gra_flink_cfg = gra_flink_cfg;
		this.gra_hbase_cfg = gra_hbase_cfg;
		this.hbase_cfg = hbase_cfg;
	
	}
	
	public GraphCollection getGraphCollection() throws IOException {
		HBaseEPGMStore graph_store = HBaseEPGMStoreFactory.createOrOpenEPGMStore(hbase_cfg, gra_hbase_cfg);
		DataSource hbase_data_source = new HBaseDataSource(graph_store, gra_flink_cfg);
		GraphCollection col = hbase_data_source.getGraphCollection();
		return col;
	}
	
	public LogicalGraph getLogicalGraph(String graph_id) throws IOException {
		HBaseEPGMStore graph_store = HBaseEPGMStoreFactory.createOrOpenEPGMStore(hbase_cfg, gra_hbase_cfg);
		DataSource hbase_data_source = new HBaseDataSource(graph_store, gra_flink_cfg);
		LogicalGraph log_gra = hbase_data_source.getGraphCollection().getGraph(GradoopId.fromString(graph_id));
		return log_gra;
	}
}
