package aljoschaRydzyk.Gradoop_Flink_Prototype;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;

public class DegMatrix_Builder {
	GradoopFlinkConfig gra_flink_cfg;
	GradoopHBaseConfig gra_hbase_cfg;
	Configuration hbase_cfg;
	StreamExecutionEnvironment fsEnv;
	StreamTableEnvironment fsTableEnv;
	
	public DegMatrix_Builder(
			GradoopFlinkConfig gra_flink_cfg,
			GradoopHBaseConfig gra_hbase_cfg,
			Configuration hbase_cfg,
			StreamExecutionEnvironment fsEnv,
			StreamTableEnvironment fsTableEnv) {
		this.gra_flink_cfg = gra_flink_cfg;
		this.gra_hbase_cfg = gra_hbase_cfg;
		this.hbase_cfg = hbase_cfg;
		this.fsEnv = fsEnv;
		this.fsTableEnv =  fsTableEnv;
		
	}
	
	public void build(
			LogicalGraph log,
			String hbase_degree_table_name) {

		Table table_vertices = null;
		try {				
			EPGMtoDegMatrix edm = new EPGMtoDegMatrix(this.fsEnv, this.fsTableEnv, log);
			table_vertices = edm.getDegMatrix();
			table_vertices.printSchema();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		fsTableEnv.sqlUpdate(
				"CREATE TABLE buildDegreeTempTable (" + 
				"  id STRING," + 
				"  cf ROW<degree INT>" +  
				") WITH (" + 
				"  'connector.type' = 'hbase'," + 
				"  'connector.version' = '1.4.3'," + 
				"  'connector.table-name' = '" + hbase_degree_table_name + "'," +
				"  'connector.zookeeper.quorum' = 'localhost:2181'" +
				")");
		table_vertices.insertInto("buildDegreeTempTable");
	}
}
