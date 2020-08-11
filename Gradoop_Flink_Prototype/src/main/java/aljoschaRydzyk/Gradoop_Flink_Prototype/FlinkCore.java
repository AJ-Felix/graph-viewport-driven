package aljoschaRydzyk.Gradoop_Flink_Prototype; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
	  private Integer topModelPos;
	  private Integer bottomModelPos;
	  private Integer leftModelPos;
	  private Integer rightModelPos;
	  
	public  FlinkCore () {
		this.env = ExecutionEnvironment.getExecutionEnvironment();
	    this.graflink_cfg = GradoopFlinkConfig.createConfig(env);
		this.gra_hbase_cfg = GradoopHBaseConfig.getDefaultConfig();
		this.hbase_cfg = HBaseConfiguration.create();
		this.fsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		this.fsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		this.fsTableEnv = StreamTableEnvironment.create(fsEnv, fsSettings);
	}
	
	public void setTopModelPos(Integer topModelPos) {
		this.topModelPos = topModelPos;
	}
	
	public Integer gettopModelPos() {
		return this.topModelPos;
	}
	
	public void setBottomModelPos(Integer bottomModelPos) {
		this.bottomModelPos = bottomModelPos;
	}
	
	public Integer getBottomModelPos() {
		return this.bottomModelPos;
	}
	
	public void setRightModelPos(Integer rightModelPos) {
		this.rightModelPos = rightModelPos;
	}
	
	public Integer getRightModelPos() {
		return this.rightModelPos;
	}
	
	public void setLeftModelPos(Integer leftModelPos) {
		this.leftModelPos = leftModelPos;
	}
	
	public Integer getLeftModelPos() {
		return this.leftModelPos;
	}
	
	public StreamExecutionEnvironment getFsEnv() {
		return this.fsEnv;
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
		DataStream<Row> dataStreamDegree = FlinkGradoopVerticesLoader.load(fsTableEnv, 50);
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
	
	public DataStream<Row> buildTopViewAppendJoin(){
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
		graphUtil.produceWrapperStream();
		return graphUtil.getMaxDegreeSubset(100);
	}
	
	public DataStream<Row> buildTopViewAppendMap(){
		CSVGraphUtilMap graphUtil = ((CSVGraphUtilMap) this.graphUtil);
		return graphUtil.produceWrapperStream();	
	}
	
	public DataStream<Row> zoomIn (Integer topModel, Integer rightModel, Integer bottomModel, Integer leftModel){
			CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
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
			DataStream<Row> wrapperStream = graphUtil.getWrapperStream();
			Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
			wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
			wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		//add vertices and edges that have neighbours in the zoomed in view to show edges
			DataStream<Row> vertexStreamOuter = graphUtil.getVertexStream()
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
			return wrapperStream;
	}
	
	public DataStream<Row> pan(Integer topOld, Integer rightOld, Integer bottomOld, Integer leftOld, Integer xModelDiff, Integer yModelDiff){
		Integer topNew = topOld + yModelDiff;
		Integer rightNew = rightOld + xModelDiff;
		Integer bottomNew = bottomOld + yModelDiff;
		Integer leftNew = leftOld + xModelDiff;
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
		DataStream<Row> vertexStreamInner = graphUtil.getVertexStream()
			.filter(new FilterFunction<Row>(){
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftNew < x) &&  (x < rightNew) && (topNew < y) && (y < bottomNew);
				}
			});
		List<DataStream<Row>> lStream;
		if ((xModelDiff == 0) && (yModelDiff < 0)) { //stattdessen könnte auch ein switch-case auf der pan...-String-Endung verwendet werden!!
			lStream = panTop(vertexStreamInner, topOld);
		} else if ((xModelDiff > 0) && (yModelDiff < 0)) {
			lStream = panTopRight(vertexStreamInner, topOld, rightOld);
		} else if ((xModelDiff > 0) && (yModelDiff == 0)) {
			lStream = panRight(vertexStreamInner, rightOld);
		} else if ((xModelDiff > 0) && (yModelDiff > 0)) {
			lStream = panBottomRight(vertexStreamInner, bottomOld, rightOld);
		} else if ((xModelDiff == 0) && (yModelDiff > 0)) {
			lStream = panBottom(vertexStreamInner, bottomOld);
		} else if ((xModelDiff < 0) && (yModelDiff > 0)) {
			lStream = panBottomLeft(vertexStreamInner, bottomOld, leftOld);
		} else if ((xModelDiff < 0) && (yModelDiff == 0)) {
			lStream = panLeft(vertexStreamInner, leftOld);
		} else {
			lStream = panTopLeft(vertexStreamInner, topOld, leftOld);
		}
		
		DataStream<Row> vertexStreamInnerOld = lStream.get(0);
		DataStream<Row> vertexStreamInnerNew = lStream.get(1);
		String vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree";
		String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
				+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
				+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
		
		//edge stream for all connections within new vertices
		Table vertexTableNew = fsTableEnv.fromDataStream(vertexStreamInnerNew).as(vertexFields);
		DataStream<Row> wrapperStream = graphUtil.getWrapperStream();
		Table wrapperTableNew = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
		wrapperTableNew = wrapperTableNew.join(vertexTableNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableNew = wrapperTableNew.join(vertexTableNew).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);

		
		//edge stream for all connection between old and new vertices in one direction
		Table vertexTableOld = fsTableEnv.fromDataStream(vertexStreamInnerOld).as(vertexFields);
		Table wrapperTableOld1 = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
		wrapperTableOld1 = wrapperTableOld1.join(vertexTableOld).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableOld1 = wrapperTableOld1.join(vertexTableNew).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
			
		//edge stream for all connection between old and new vertices in the respective other direction
		Table wrapperTableOld2 = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
		wrapperTableOld2 = wrapperTableOld2.join(vertexTableNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableOld2 = wrapperTableOld2.join(vertexTableOld).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		
		RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		DataStream<Row> wrapperStreamNew = fsTableEnv.toAppendStream(wrapperTableNew, typeInfo);
		DataStream<Row> wrapperStreamOldToNew = fsTableEnv.toAppendStream(wrapperTableOld1, typeInfo);
		DataStream<Row> wrapperStreamNewToOld = fsTableEnv.toAppendStream(wrapperTableOld2, typeInfo);
		DataStream<Row> wrapperStreamInner = wrapperStreamNew.union(wrapperStreamOldToNew).union(wrapperStreamNewToOld);
		
		//add vertices and edges that have neighbours in the view to show edges
		DataStream<Row> vertexStreamOuter = graphUtil.getVertexStream()
				.filter(new FilterFunction<Row>(){
					@Override
					public boolean filter(Row value) throws Exception {
						Integer x = (Integer) value.getField(4);
						Integer y = (Integer) value.getField(5);
						return (leftNew > x) ||  (x > rightNew) || (topNew > y) || (y > bottomNew);
					}
				});
		Table vertexTableInner = fsTableEnv.fromDataStream(vertexStreamInner).as(vertexFields);
		Table vertexTableOuter = fsTableEnv.fromDataStream(vertexStreamOuter).as(vertexFields);
		Table wrapperTableInOut = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTableInner).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableInOut = wrapperTableInOut.join(vertexTableOuter).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		Table wrapperTableOutIn = fsTableEnv.fromDataStream(wrapperStream).as(wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTableOuter).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableOutIn = wrapperTableOutIn.join(vertexTableInner).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		DataStream<Row> wrapperStreamOuter = fsTableEnv.toAppendStream(wrapperTableInOut, typeInfo)
				.union(fsTableEnv.toAppendStream(wrapperTableOutIn, typeInfo));
		wrapperStream = wrapperStreamInner.union(wrapperStreamOuter);
		return wrapperStream;
	}
		
	
	public List<DataStream<Row>> panTop(DataStream<Row> vertexStream, Integer topOld) {
		DataStream<Row> vertexStreamNew = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer y = (Integer) value.getField(5);
				return (y < topOld);
			}
		});
		DataStream<Row> vertexStreamOld = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer y = (Integer) value.getField(5);
				return (y > topOld);
			}
		});
		List<DataStream<Row>> lStream = new ArrayList<DataStream<Row>>();
		lStream.add(vertexStreamOld);
		lStream.add(vertexStreamNew);
		return lStream;
	}
	
	public List<DataStream<Row>> panTopRight(DataStream<Row> vertexStream, Integer topOld, Integer rightOld) {
		DataStream<Row> vertexStreamOld = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (x < rightOld) && (y > topOld);
			}
		});
		DataStream<Row> vertexStreamNew = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (y < topOld) || (x > rightOld);
			}
		});
		List<DataStream<Row>> lStream = new ArrayList<DataStream<Row>>();
		lStream.add(vertexStreamOld);
		lStream.add(vertexStreamNew);
		return lStream;
	}
	
	public List<DataStream<Row>> panRight(DataStream<Row> vertexStream, Integer rightOld) {
		DataStream<Row> vertexStreamOld = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				return (x < rightOld) ;
			}
		});
		DataStream<Row> vertexStreamNew = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				return (x > rightOld);
			}
		});
		List<DataStream<Row>> lStream = new ArrayList<DataStream<Row>>();
		lStream.add(vertexStreamOld);
		lStream.add(vertexStreamNew);
		return lStream;
	}
	
	public List<DataStream<Row>> panBottomRight(DataStream<Row> vertexStream, Integer bottomOld, Integer rightOld) {
		DataStream<Row> vertexStreamOld = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (x < rightOld) && (y < bottomOld);
			}
		});
		DataStream<Row> vertexStreamNew = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (y > bottomOld) || (x > rightOld);
			}
		});
		List<DataStream<Row>> lStream = new ArrayList<DataStream<Row>>();
		lStream.add(vertexStreamOld);
		lStream.add(vertexStreamNew);
		return lStream;
	}
	
	public List<DataStream<Row>> panBottom (DataStream<Row> vertexStream, Integer bottomOld) {
		DataStream<Row> vertexStreamOld = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer y = (Integer) value.getField(5);
				return (y < bottomOld);
			}
		});
		DataStream<Row> vertexStreamNew = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer y = (Integer) value.getField(5);
				return (y > bottomOld);
			}
		});
		List<DataStream<Row>> lStream = new ArrayList<DataStream<Row>>();
		lStream.add(vertexStreamOld);
		lStream.add(vertexStreamNew);
		return lStream;
	}
	
	public List<DataStream<Row>> panBottomLeft (DataStream<Row> vertexStream, Integer bottomOld, Integer leftOld) {
		DataStream<Row> vertexStreamOld = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (y < bottomOld) && (x > leftOld);
			}
		});
		DataStream<Row> vertexStreamNew = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (y > bottomOld) || (x < leftOld);
			}
		});
		List<DataStream<Row>> lStream = new ArrayList<DataStream<Row>>();
		lStream.add(vertexStreamOld);
		lStream.add(vertexStreamNew);
		return lStream;
	}
	
	public List<DataStream<Row>> panLeft (DataStream<Row> vertexStream, Integer leftOld) {
		DataStream<Row> vertexStreamOld = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				return (x > leftOld);
			}
		});
		DataStream<Row> vertexStreamNew = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (x < leftOld);
			}
		});
		List<DataStream<Row>> lStream = new ArrayList<DataStream<Row>>();
		lStream.add(vertexStreamOld);
		lStream.add(vertexStreamNew);
		return lStream;
	}
	
	public List<DataStream<Row>> panTopLeft (DataStream<Row> vertexStream, Integer topOld, Integer leftOld) {
		DataStream<Row> vertexStreamOld = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (y > topOld) && (x > leftOld);
			}
		});
		DataStream<Row> vertexStreamNew = vertexStream.filter(new FilterFunction<Row>() {
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (y < topOld) || (x < leftOld);
			}
		});
		List<DataStream<Row>> lStream = new ArrayList<DataStream<Row>>();
		lStream.add(vertexStreamOld);
		lStream.add(vertexStreamNew);
		return lStream;
	}
		
	public DataStream<Row> displayAll() {
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
		return graphUtil.produceWrapperStream();
	}
}
