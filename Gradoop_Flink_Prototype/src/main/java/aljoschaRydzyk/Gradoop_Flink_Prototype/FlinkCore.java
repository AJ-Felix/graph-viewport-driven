package aljoschaRydzyk.Gradoop_Flink_Prototype; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.hbase.config.GradoopHBaseConfig;
import org.gradoop.storage.hbase.impl.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.hbase.impl.io.HBaseDataSource;

import Temporary.CSVGraphUtilMap;

public class FlinkCore {
	  private ExecutionEnvironment env;
	  private GradoopFlinkConfig graflink_cfg;
	  private GradoopHBaseConfig gra_hbase_cfg;
	  private org.apache.hadoop.conf.Configuration hbase_cfg;
	  private EnvironmentSettings fsSettings;
	  private StreamExecutionEnvironment fsEnv;
	  private StreamTableEnvironment fsTableEnv;
	  
	  private GraphUtil graphUtil;
	  private Float topModelPos;
	  private Float bottomModelPos;
	  private Float leftModelPos;
	  private Float rightModelPos;
	  private Set<String> visualizedWrappers;
	  private Set<String> visualizedVertices;
	  
	public  FlinkCore () {
		this.env = ExecutionEnvironment.getExecutionEnvironment();
	    this.graflink_cfg = GradoopFlinkConfig.createConfig(env);
		this.gra_hbase_cfg = GradoopHBaseConfig.getDefaultConfig();
		this.hbase_cfg = HBaseConfiguration.create();
		this.fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		this.fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		org.apache.flink.configuration.Configuration conf = new Configuration();
		this.fsEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
		this.fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
//		TestThread thread = new TestThread("prototype", fsEnv, this);
//		thread.start();
		System.out.println("initiated Flink.");

	}
	
	public void setTopModelPos(Float topModelPos2) {
		this.topModelPos = topModelPos2;
	}
	
	public Float gettopModelPos() {
		return this.topModelPos;
	}
	
	public void setBottomModelPos(Float bottomModelPos) {
		this.bottomModelPos = bottomModelPos;
	}
	
	public Float getBottomModelPos() {
		return this.bottomModelPos;
	}
	
	public void setRightModelPos(Float rightModelPos) {
		this.rightModelPos = rightModelPos;
	}
	
	public Float getRightModelPos() {
		return this.rightModelPos;
	}
	
	public void setLeftModelPos(Float leftModelPos) {
		this.leftModelPos = leftModelPos;
	}
	
	public Float getLeftModelPos() {
		return this.leftModelPos;
	}
	
	public StreamExecutionEnvironment getFsEnv() {
		return this.fsEnv;
	}
	
	public void setVisualizedWrappers(Set<String> visualizedWrappers) {
		this.visualizedWrappers = visualizedWrappers;
	}
	
	public Set<String> getVisualizedWrappers(){
		return this.visualizedWrappers;
	}
	
	public void setVisualizedVertices(Set<String> visualizedVertices) {
		this.visualizedVertices = visualizedVertices;
	}
	
	public Set<String> getVisualizedVertices(){
		return this.visualizedVertices;
	}
	
	public LogicalGraph getLogicalGraph(String gradoopGraphID) throws IOException {
		DataSource hbaseDataSource = new HBaseDataSource(HBaseEPGMStoreFactory.createOrOpenEPGMStore(hbase_cfg, gra_hbase_cfg), graflink_cfg);
		LogicalGraph graph = hbaseDataSource.getGraphCollection().getGraph(GradoopId.fromString(gradoopGraphID));
		return graph;
	}
	
	public GraphUtil initializeGradoopGraphUtil() {
		LogicalGraph graph;
		try {
			graph = this.getLogicalGraph("5ebe6813a7986cc7bd77f9c2");	//5ebe6813a7986cc7bd77f9c2 is one10thousand_sample_2_third_degrees_layout
			this.graphUtil = new GradoopGraphUtil(graph, this.fsEnv, this.fsTableEnv);
		} catch (IOException e) {
			e.printStackTrace();
		}	
		return this.graphUtil;
	}
	
	public GraphUtil initializeCSVGraphUtilJoin() {
		this.graphUtil = new CSVGraphUtilJoin(this.fsEnv, this.fsTableEnv, "/home/aljoscha/graph-viewport-driven/csvGraphs/one10thousand_sample_2_third_degrees_layout");
		return this.graphUtil;
	}
	
	public GraphUtil initializeCSVGraphUtilMap() {
		this.graphUtil = new CSVGraphUtilMap(this.fsEnv, "/home/aljoscha/graph-viewport-driven/csvGraphs/one10thousand_sample_2_third_degrees_layout");
		return this.graphUtil;
	}
	
	public DataStream<Tuple2<Boolean, Row>> buildTopViewRetract(){
		DataStream<Row> dataStreamDegree = FlinkGradoopVerticesLoader.load(fsTableEnv, 10);
		DataStream<Tuple2<Boolean, Row>> wrapperStream = null;
		try {
			GradoopGraphUtil graphUtil = ((GradoopGraphUtil) this.graphUtil);
			graphUtil.produceWrapperStream();
			wrapperStream = graphUtil.getMaxDegreeSubset(dataStreamDegree);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return wrapperStream;
	}
	
	public DataStream<Row> buildTopViewAppendJoin(Integer maxVertices){
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
		graphUtil.produceWrapperStream();
		return graphUtil.getMaxDegreeSubset(maxVertices);
	}
	
	public DataStream<Row> buildTopViewAppendMap(){
		CSVGraphUtilMap graphUtil = ((CSVGraphUtilMap) this.graphUtil);
		return graphUtil.produceWrapperStream();	
	}
	
	public DataStream<Row> zoom (Float topModel, Float rightModel, Float bottomModel, Float leftModel){
			DataStream<Row> vertexStreamInner = graphUtil.getVertexStream()
				.filter(new FilterFunction<Row>(){
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftModel <= x) &&  (x <= rightModel) && (topModel <= y) && (y <= bottomModel);
				}
			});
			String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
						+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
						+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
			String vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree";
			Table vertexTable = fsTableEnv.fromDataStream(vertexStreamInner).as(vertexFields);
			DataStream<Row> wrapperStream = this.graphUtil.getWrapperStream();
			
			//Diese Funktionalität kann alternativ mit einer Adjazenzmatrix umgesetzt werden
			Set<String> visualizedWrappers = this.getVisualizedWrappers();
			wrapperStream = wrapperStream.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					return !visualizedWrappers.contains(value.getField(13)); 
				}
			});
			//
			
			Set<String> visualizedVertices = this.getVisualizedVertices();
			wrapperStream = wrapperStream.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					return !(visualizedVertices.contains(value.getField(2).toString()) && value.getField(14).equals("identityEdge"));
				}
			});
			Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
			wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
			wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		//add vertices and edges that have neighbours in the zoomed in view to show edges
			DataStream<Row> vertexStreamOuter = this.graphUtil.getVertexStream()
				.filter(new FilterFunction<Row>() {
					@Override
					public boolean filter(Row value) throws Exception {
						Integer x = (Integer) value.getField(4);
						Integer y = (Integer) value.getField(5);
						return (leftModel > x) || (x > rightModel) || (topModel > y) || (y > bottomModel);
					}
				});
			Table vertexTableOuter = fsTableEnv.fromDataStream(vertexStreamOuter).as(vertexFields);
			Table wrapperTableInOut = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
			wrapperTableInOut = wrapperTableInOut.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
			wrapperTableInOut = wrapperTableInOut.join(vertexTableOuter).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
			Table wrapperTableOutIn = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
			wrapperTableOutIn = wrapperTableOutIn.join(vertexTableOuter).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
			wrapperTableOutIn = wrapperTableOutIn.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
			RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[] {Types.STRING, Types.STRING, 
					Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
					Types.STRING, Types.STRING});
			wrapperStream = fsTableEnv.toAppendStream(wrapperTable, typeInfo).union(fsTableEnv.toAppendStream(wrapperTableInOut, typeInfo))
					.union(fsTableEnv.toAppendStream(wrapperTableOutIn, typeInfo));
//			wrapperStream.addSink(new SinkFunction<Row>() {
//				@Override
//				public void invoke(Row element) {
//					System.out.println(element);
//				}
//			});
			return wrapperStream;
	}
		
	public DataStream<Row> pan(Float topOld, Float rightOld, Float bottomOld, Float leftOld,Float xModelDiff, Float yModelDiff){
		Float topNew = topOld + yModelDiff;
		Float rightNew = rightOld + xModelDiff;
		Float bottomNew = bottomOld + yModelDiff;
		Float leftNew = leftOld + xModelDiff;
		DataStream<Row> vertexStreamInner = this.graphUtil.getVertexStream()
			.filter(new FilterFunction<Row>(){
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftNew <= x) &&  (x <= rightNew) && (topNew <= y) && (y <= bottomNew);
				}
			});
		DataStream<Row> vertexStreamInnerNew = vertexStreamInner.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld);
				}
			});
		DataStream<Row> vertexStreamOldOuterExtend = this.graphUtil.getVertexStream()
			.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return ((leftOld > x) || (x > rightOld) || (topOld > y) || (y > bottomOld)) && 
							((leftNew > x) || (x > rightNew) || (topNew > y) || (y > bottomNew));
				}
			});
		DataStream<Row> vertexStreamOldInnerNotNewInner = graphUtil.getVertexStream()
			.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftOld <= x) && (x <= rightOld) && (topOld <= y) && (y <= bottomOld) && 
							((leftNew > x) || (x > rightNew) || (topNew > y) || (y > bottomNew));
				}
			});
		String vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree";
		String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
				+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
				+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
		Table vertexTableInnerNew = fsTableEnv.fromDataStream(vertexStreamInnerNew).as(vertexFields);
		Table vertexTableOldOuterExtend = fsTableEnv.fromDataStream(vertexStreamOldOuterExtend).as(vertexFields);
		Table vertexTableOldInNotNewIn = fsTableEnv.fromDataStream(vertexStreamOldInnerNotNewInner).as(vertexFields);
		Table vertexTableInner = fsTableEnv.fromDataStream(vertexStreamInner).as(vertexFields);
		DataStream<Row> wrapperStream = this.graphUtil.getWrapperStream();
		Set<String> visualizedWrappers = this.getVisualizedWrappers();
		Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
		Table wrapperTableInOut = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTableOldOuterExtend).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		Table wrapperTableOutIn = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTableOldOuterExtend).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		Table wrapperTableInIn = wrapperTable.join(vertexTableInnerNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableInIn = wrapperTableInIn.join(vertexTableInnerNew).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		Table wrapperTableOldInNewInInOut = wrapperTable.join(vertexTableInner)
				.where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableOldInNewInInOut = wrapperTableOldInNewInInOut.join(vertexTableOldInNotNewIn)
				.where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		Table wrapperTableOldInNewInOutIn = wrapperTable.join(vertexTableOldInNotNewIn)
				.where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableOldInNewInOutIn = wrapperTableOldInNewInOutIn.join(vertexTableInner)
				.where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		
		//filter out redundant identity edges
		DataStream<Row> wrapperStreamInIn = fsTableEnv.toAppendStream(wrapperTableInIn, typeInfo)
			.filter(new FilterFunction<Row>() {
				@Override
				public boolean filter(Row value) throws Exception {
					return !(value.getField(14).equals("identityEdge"));
				}
			});
		
		//Diese Funktionalität kann alternativ mit einer Adjazenzmatrix umgesetzt werden
		//filter out already visualized edges
		DataStream<Row> wrapperStreamOldInNewIn = fsTableEnv.toAppendStream(wrapperTableOldInNewInInOut, typeInfo)
				.union(fsTableEnv.toAppendStream(wrapperTableOldInNewInOutIn, typeInfo));	
		wrapperStreamOldInNewIn = wrapperStreamOldInNewIn.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return !(visualizedWrappers.contains(value.getField(13)));
			}
		});
		wrapperStreamOldInNewIn.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				return false;
			}
		});
		//

		wrapperStream = wrapperStreamInIn
				.union(fsTableEnv.toAppendStream(wrapperTableOutIn, typeInfo))
				.union(wrapperStreamOldInNewIn)
				.union(fsTableEnv.toAppendStream(wrapperTableInOut, typeInfo));
		return wrapperStream;
	}
		
	public DataStream<Row> displayAll() {
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
		return graphUtil.produceWrapperStream();
	}
}
