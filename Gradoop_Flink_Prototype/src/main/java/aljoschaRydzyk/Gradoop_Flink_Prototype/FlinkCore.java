package aljoschaRydzyk.Gradoop_Flink_Prototype; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.hbase.impl.io.HBaseDataSource;

public class FlinkCore {
	  private ExecutionEnvironment env;
	  private GradoopFlinkConfig graflink_cfg;
	  private GradoopHBaseConfig gra_hbase_cfg;
	  private Configuration hbase_cfg;
	  private EnvironmentSettings fsSettings;
	  private StreamExecutionEnvironment fsEnv;
	  private StreamTableEnvironment fsTableEnv;
	  
	  private GraphUtil graphUtil;
	  private Integer topBoundary;
	  private Integer bottomBoundary;
	  private Integer leftBoundary;
	  private Integer rightBoundary;
	  
	public  FlinkCore () {
		this.env = ExecutionEnvironment.getExecutionEnvironment();
	    this.graflink_cfg = GradoopFlinkConfig.createConfig(env);
		this.gra_hbase_cfg = GradoopHBaseConfig.getDefaultConfig();
		this.hbase_cfg = HBaseConfiguration.create();
		this.fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		this.fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		this.fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
	}
	
	public StreamExecutionEnvironment getFsEnv() {
		return this.fsEnv;
	}
	
	public LogicalGraph getLogicalGraph(String gradoopGraphID) throws IOException {
		DataSource hbaseDataSource = new HBaseDataSource(HBaseEPGMStoreFactory.createOrOpenEPGMStore(hbase_cfg, gra_hbase_cfg), graflink_cfg);
		LogicalGraph graph = hbaseDataSource.getGraphCollection().getGraph(GradoopId.fromString(gradoopGraphID));
		return graph;
	}
	
	public List<DataStream<Tuple2<Boolean, Row>>> buildTopView (){
		DataStream<Tuple2<Boolean, Row>> dataStreamDegree = FlinkGradoopVerticesLoader.load(fsTableEnv, 50);
		List<DataStream<Tuple2<Boolean, Row>>> datastreams = new ArrayList<DataStream<Tuple2<Boolean, Row>>>();
		try {
			LogicalGraph graph = this.getLogicalGraph("5ebe6813a7986cc7bd77f9c2");	//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
			this.graphUtil = new GraphUtil(graph, fsEnv, fsTableEnv);
			datastreams.addAll(graphUtil.getMaxDegreeSubset(dataStreamDegree));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return datastreams;
		
	}
	
	@SuppressWarnings("rawtypes")
	public List<DataStream> zoomIn (Integer top, Integer right, Integer bottom, Integer left){
		List<DataStream> datastreams = new ArrayList<DataStream>();
		DataStream<Tuple5<String, String, String, String, String>> vertexStream = this.graphUtil.getVertexStream()
			.filter(new FilterFunction<Tuple5<String, String, String, String, String>>(){
			@Override
			public boolean filter(Tuple5<String, String, String, String, String> value) throws Exception {
				Integer x = Integer.parseInt(value.f3);
				Integer y = Integer.parseInt(value.f4);
				return (left < x) &&  (x < right) && (top < y) && (y < bottom);
			}
		});
		datastreams.add(vertexStream);
		Table vertexTable = fsTableEnv.fromDataStream(vertexStream).as("vertexIdCompare, vertexLabel, vertexIdLayout, x, y");
		Table edgeTable = fsTableEnv.fromDataStream(this.graphUtil.getEdgeStream())
				.as("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
		edgeTable = edgeTable.join(vertexTable).where("vertexIdCompare = vertexIdSourceOld")
				.select("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
		edgeTable = edgeTable.join(vertexTable).where("vertexIdCompare = vertexIdTargetOld")
				.select("edgeId, vertexIdSourceNew, vertexIdTargetNew");
		RowTypeInfo rowTypeInfoEdges = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING}, 
				new String[] {"edgeId", "vertexIdSourceNew", "vertexIdTargetNew"});
		DataStream<Tuple2<Boolean, Row>> edgeStream = fsTableEnv.toRetractStream(edgeTable, rowTypeInfoEdges);
		datastreams.add(edgeStream);
		return datastreams;
	}
	
	@SuppressWarnings("rawtypes")
	public List<DataStream> panRight(Integer topOld, Integer rightOld, Integer bottomOld, Integer leftOld, Integer topNew, Integer rightNew, 
			Integer bottomNew, Integer leftNew){
		List<DataStream> datastreams = new ArrayList<DataStream>();
		DataStream<Tuple5<String, String, String, String, String>> vertexStreamAll = this.graphUtil.getVertexStream()
				.filter(new FilterFunction<Tuple5<String, String, String, String, String>>(){
				@Override
				public boolean filter(Tuple5<String, String, String, String, String> value) throws Exception {
					Integer x = Integer.parseInt(value.f3);
					Integer y = Integer.parseInt(value.f4);
					return (leftNew < x) &&  (x < rightNew) && (topNew < y) && (y < bottomNew);
				}
			});
		DataStream<Tuple5<String, String, String, String, String>> vertexStreamNew = vertexStreamAll
				.filter(new FilterFunction<Tuple5<String, String, String, String, String>>(){
					@Override
					public boolean filter(Tuple5<String, String, String, String, String> value) throws Exception {
						Integer x = Integer.parseInt(value.f3);
						Integer y = Integer.parseInt(value.f4);
						return (rightOld < x) &&  (x < rightNew) && (topNew < y) && (y < bottomNew);
				}
			});
		DataStream<Tuple5<String, String, String, String, String>> vertexStreamOld = vertexStreamAll
				.filter(new FilterFunction<Tuple5<String, String, String, String, String>>(){
					@Override
					public boolean filter(Tuple5<String, String, String, String, String> value) throws Exception {
						Integer x = Integer.parseInt(value.f3);
						Integer y = Integer.parseInt(value.f4);
						return (leftOld < x) &&  (x < leftNew) && (topNew < y) && (y < bottomNew);
				}
			});
		
		//edge stream for all connections within new vertices
		Table vertexTableNew = fsTableEnv.fromDataStream(vertexStreamNew).as("vertexIdCompare, vertexLabel, vertexIdLayout, x, y");
		Table edgeTableNew = fsTableEnv.fromDataStream(this.graphUtil.getEdgeStream())
				.as("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
		edgeTableNew = edgeTableNew.join(vertexTableNew).where("vertexIdCompare = vertexIdSourceOld")
				.select("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
		edgeTableNew = edgeTableNew.join(vertexTableNew).where("vertexIdCompare = vertexIdTargetOld")
				.select("edgeId, vertexIdSourceNew, vertexIdTargetNew");

		
		//edge stream for all connection between old and new vertices in one direction
		Table vertexTableOld = fsTableEnv.fromDataStream(vertexStreamOld).as("vertexIdCompare, vertexLabel, vertexIdLayout, x, y");
		Table edgeTableOld1 = fsTableEnv.fromDataStream(this.graphUtil.getEdgeStream())
				.as("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
		edgeTableOld1 = edgeTableOld1.join(vertexTableOld).where("vertexIdCompare = vertexIdSourceOld")
				.select("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
		edgeTableOld1 = edgeTableOld1.join(vertexTableNew).where("vertexIdCompare = vertexIdTargetOld")
				.select("edgeId, vertexIdSourceNew, vertexIdTargetNew");
			
		//edge stream for all connection between old and new vertices in the respective other direction
		Table edgeTableOld2 = fsTableEnv.fromDataStream(this.graphUtil.getEdgeStream())
				.as("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
		edgeTableOld2 = edgeTableOld2.join(vertexTableNew).where("vertexIdCompare = vertexIdSourceOld")
				.select("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
		edgeTableOld2 = edgeTableOld2.join(vertexTableOld).where("vertexIdCompare = vertexIdTargetOld")
				.select("edgeId, vertexIdSourceNew, vertexIdTargetNew");
		
		RowTypeInfo rowTypeInfoEdges = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING}, 
				new String[] {"edgeId", "vertexIdSourceNew", "vertexIdTargetNew"});
		DataStream<Tuple2<Boolean, Row>> edgeStreamNew = fsTableEnv.toRetractStream(edgeTableNew, rowTypeInfoEdges);
		DataStream<Tuple2<Boolean, Row>> edgeStreamOldToNew = fsTableEnv.toRetractStream(edgeTableOld1, rowTypeInfoEdges);
		DataStream<Tuple2<Boolean, Row>> edgeStreamNewToOld = fsTableEnv.toRetractStream(edgeTableOld2, rowTypeInfoEdges);
		DataStream<Tuple2<Boolean, Row>> edgeStream = edgeStreamNew.union(edgeStreamOldToNew).union(edgeStreamNewToOld);
		datastreams.add(vertexStreamNew);
		datastreams.add(edgeStream);
		return datastreams;
	}
	
	@SuppressWarnings("rawtypes")
	public List<DataStream> displayAll() {
		LogicalGraph graph = null;
		try {
			graph = this.getLogicalGraph("5ebe6813a7986cc7bd77f9c2");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
		this.graphUtil = new GraphUtil(graph, fsEnv, fsTableEnv);
		List<DataStream> streams = new ArrayList<DataStream>();
		DataStream<Tuple2<Boolean, Row>> dataStreamDegree = FlinkGradoopVerticesLoader.load(fsTableEnv, 50);
		try {
			List<DataStream<Tuple2<Boolean, Row>>> otherStreams = graphUtil.getMaxDegreeSubset(dataStreamDegree);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		streams.add(this.graphUtil.getVertexStream());
		streams.add(this.graphUtil.getEdgeStream());
		return streams;
	}
	
	public List<DataStream> pan(Integer xMouseMovement, Integer yMouseMovement){
		
		return null;
	}
}
