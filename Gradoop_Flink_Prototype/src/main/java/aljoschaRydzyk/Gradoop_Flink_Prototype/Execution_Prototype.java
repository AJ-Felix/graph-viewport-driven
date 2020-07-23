package aljoschaRydzyk.Gradoop_Flink_Prototype;


import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.log4j.BasicConfigurator;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.BySameId;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.model.impl.operators.layouting.CentroidFRLayouter;
import org.gradoop.flink.model.impl.operators.sampling.RandomLimitedDegreeVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomNonUniformVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexNeighborhoodSampling;
import org.gradoop.flink.model.impl.operators.sampling.RandomVertexSampling;
import org.gradoop.flink.model.impl.operators.sampling.functions.Neighborhood;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.flink.model.impl.operators.transformation.functions.TransformGraphHead;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;

import Temporary.GraFlink_Graph_Loader;

public class Execution_Prototype {
	
	public static void main(String[] args) {
		
		//set Flink Logging
//		BasicConfigurator.configure();
		
		//create gradoop Flink configuration
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		GradoopFlinkConfig gra_flink_cfg = GradoopFlinkConfig.createConfig(env);

		//create gradoop HBase configuration
		GradoopHBaseConfig gra_hbase_cfg = GradoopHBaseConfig.getDefaultConfig();
		Configuration hbase_cfg = HBaseConfiguration.create();
		
		//Flink 10.0
		
		//create Flink Table Stream Configuration
		EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamExecutionEnvironment fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
		
		//enable checkpointing (for StreamingFileSink)
		fsEnv.enableCheckpointing(1000);
		
		//create Flink Table Batch Configuration
//		BatchTableEnvironment fbTableEnv = BatchTableEnvironment.create(env);
		
		//load graph from CSV to HBase
//		DataSource source = new CSVDataSource("some_path", gra_flink_cfg);
//		GradoopId id = GradoopId.fromString("id_of_graph");
//		LogicalGraph log = source.getGraphCollection().getGraph(id);
//		Gradoop_HBase_Sink sink = new Gradoop_HBase_Sink(gra_hbase_cfg, hbase_cfg, gra_flink_cfg);
//		sink.sink(log_result);
		
		//load graph from CSV, sample, and sink sample graph to CSV
//		DataSource source = new CSVDataSource("some_path", gra_flink_cfg);
//		GradoopId id = GradoopId.fromString("id_of_graph");
//		LogicalGraph log = source.getGraphCollection().getGraph(id);
//		log = new RandomVertexNeighborhoodSampling((float) 0.0001, 3, Neighborhood.BOTH).sample(log);
//		DataSink csvDataSink = new CSVDataSink("some_path", gra_flink_cfg);
//		csvDataSink.write(log, true);

		//load graph from HBase, calculate vertex degree and sink to HBase
//		GraFlink_Graph_Loader loader = new GraFlink_Graph_Loader(gra_flink_cfg, gra_hbase_cfg, hbase_cfg);
//		GradoopId id = GradoopId.fromString("id_of_graph");
//		LogicalGraph log = loader.getGraphCollection().getGraph(id);
//		LogicalGraph log_result = new EPGMtoDegMatrix(log).getDegMatrix();
//		Gradoop_HBase_Sink sink = new Gradoop_HBase_Sink(gra_hbase_cfg, hbase_cfg, gra_flink_cfg);
//		sink.sink(log_result);
		
		//write vertex properties of graph to file for debugging
//		org.apache.flink.api.java.operators.DataSink<EPGMVertex> sink = log.getVertices().writeAsText("/home/aljoscha/debug/tmp");

		//load graph from HBase, calculate vertex degree, transform to DegreeMatrix and sink to HBase
//		GraFlink_Graph_Loader loader = new GraFlink_Graph_Loader(gra_flink_cfg, gra_hbase_cfg, hbase_cfg);
//		LogicalGraph log = loader.getLogicalGraph("graph_id");
//		log.getGraphHead().print();
//		DegMatrix_Builder degMatrix_builder = new DegMatrix_Builder(gra_flink_cfg, gra_hbase_cfg, hbase_cfg, fsEnv, fsTableEnv);
//		degMatrix_builder.build(log, "degree_vertices", "graph_id");
		
		//load degree matrix from HBase and filter for vertices
//		DataStream<Tuple2<Boolean, Row>> ds_row = DegreeMatrix_Loader.load(fsTableEnv, "degree_table_name_in_hbase", 10);
//		TupleTypeInfo<Tuple3<String, String,Integer>> ds_tuple_typeInfo = new TupleTypeInfo<Tuple3<String, String, Integer>>(new TypeInformation[]{Types.STRING,Types.STRING, Types.INT});
//		DataStream<Tuple3<String,String,Integer>> ds_tuple = ds_row.map(new MapFunction<Tuple2<Boolean,Row>, Tuple3<String,String,Integer>>() {
//			private static final long serialVersionUID = -7026515741245426370L;
//
//			@Override
//			public Tuple3<String,String, Integer> map(Tuple2<Boolean, Row> value) throws Exception {
//				String s = (String) value.f1.getField(0);
//				Integer i = (Integer) ((Row) value.f1.getField(1)).getField(0);
//				// TODO Auto-generated method stub
//				return new Tuple3<String, String, Integer>("count",s, i);
//			}
//		}).returns(ds_tuple_typeInfo).setParallelism(1);
//		ds_tuple.countWindowAll(3).maxBy(2).setParallelism(1).print().setParallelism(1);
//		ds_row.print().setParallelism(1);
		
		//load graph from HBase, compute layout and build CSV file for Cytoscape coordinates visualization
//		GraFlink_Graph_Loader loader = new GraFlink_Graph_Loader(gra_flink_cfg, gra_hbase_cfg, hbase_cfg);
//		LogicalGraph log = loader.getLogicalGraph("graph_id");
//		LogicalGraph log_layout = new CentroidFRLayouter(5, 2000).execute(log);
//		Cyto_CSV_Builder.build(fsEnv, log_layout);
		try {
//			PrintStream fileOut = new PrintStream("/home/aljoscha/out.txt");
//			System.setOut(fileOut);
//
			GraFlink_Graph_Loader loader = new GraFlink_Graph_Loader(gra_flink_cfg, gra_hbase_cfg, hbase_cfg);
			LogicalGraph log = loader.getLogicalGraph("5ebe6813a7986cc7bd77f9c2"); 			//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
			GradoopToCSV.parseGradoopToCSV(log, "/home/aljoscha/graph-viewport-driven/csvGraphs/one10thousand_sample_2_third_degrees_layout");
//			
//			DataStream<Tuple2<Boolean, Row>> ds_degree = FlinkGradoopVerticesLoader.load(fsTableEnv, 10);
//			ds_degree.print().setParallelism(1);
			
//			GraFlink_Graph_Loader loader = new GraFlink_Graph_Loader(gra_flink_cfg, gra_hbase_cfg, hbase_cfg);
//			LogicalGraph log = loader.getLogicalGraph("5ebe6813a7986cc7bd77f9c2"); 			//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
//			log.getVertices().print();
//			MatchDegreeChoice.match(fsEnv, fsTableEnv, ds_degree, log);
			
//			RetractCsvToCytoscapeCsv.convert("/home/aljoscha/debug/tmp/tables/test_run5/3", "/home/aljoscha/debug/tmp/tables/test_run5/cyto");
			
//			TupleTypeInfo<Tuple3<String, String,Integer>> ds_tuple_typeInfo = new TupleTypeInfo<Tuple3<String, String, Integer>>(new TypeInformation[]{Types.STRING,Types.STRING, Types.INT});
//			DataStream<Tuple3<String,String,Integer>> ds_tuple = ds_degree.map(new MapFunction<Tuple2<Boolean,Row>, Tuple3<String,String,Integer>>() {
//				private static final long serialVersionUID = -7026515741245426370L;
//	
//				@Override
//				public Tuple3<String,String, Integer> map(Tuple2<Boolean, Row> value) throws Exception {
//					String s = (String) value.f1.getField(0);
//					Integer i = (Integer) ((Row) value.f1.getField(1)).getField(0);
//					// TODO Auto-generated method stub
//					return new Tuple3<String, String, Integer>("count",s, i);
//				}
//			}).returns(ds_tuple_typeInfo).setParallelism(1);
//			ds_tuple.countWindowAll(3).maxBy(2).setParallelism(1).print().setParallelism(1);
//			ds_degree.print().setParallelism(1);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		//actions
//		String hbase_degree_table_name = "degree_begin_may";
//		DegMatrix_Builder.build(gra_flink_cfg, gra_hbase_cfg, hbase_cfg, fsEnv, fsTableEnv, hbase_degree_table_name);
//		DataStream<Row> ds_row = DegreeMatrix_Loader.load(fsTableEnv, hbase_degree_table_name, 0);
//		ds_row.print().setParallelism(1);
		
		// execute
		try {
			fsTableEnv.execute("test");
//			env.execute("test");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//ARCHIVE
		
			//load EPGMGraph from HBase and call SQL query on it
				//VertexIDs and Properties are returned in unreadable format when using STRING
				//conversion might be possible but tedious (using BYTES, see notes)
				
			//		fsTableEnv.sqlUpdate(
			//		"CREATE TABLE tempTable (" + 
			//		"  id BYTES," + 
			//		"  m ROW<l STRING,g STRING>," +  
			//		"  p_type ROW<degree STRING, label STRING, inDegree STRING, birthday STRING>," +  
			//		"  p_value ROW<degree STRING, label STRING, inDegree STRING, birthday STRING>" +
			//		") WITH (" + 
			//		"  'connector.type' = 'hbase'," + 
			//		"  'connector.version' = '1.4.3'," + 
			//		"  'connector.table-name' = 'vertices'," +
			//		"  'connector.zookeeper.quorum' = 'localhost:2181'" +
			//		")");
			//Table source = fsTableEnv.from("tempTable");
			//String query = "SELECT * from tempTable ORDER BY p_value.degree DESC LIMIT 1";
			//Table source2 = fsTableEnv.sqlQuery(query);
			//TypeInformation<?> info3 = LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(DataTypes.BYTES());
			//RowTypeInfo adjListInfo = new RowTypeInfo(new TypeInformation[]{info3, 
			//		Types.ROW(Types.STRING, Types.STRING), 
			//		Types.ROW(Types.STRING, Types.STRING, Types.STRING, Types.STRING), Types.ROW(Types.STRING, Types.STRING, Types.STRING, Types.STRING)}, 
			//		new String[] {"id", "m", "p_type", "p_value"});
			//DataStream<Tuple2<Boolean,Row>> ds_tuple_row = fsTableEnv.toRetractStream(source2, adjListInfo);
			//ds_tuple_row.print();
	}
}

