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
		return graphUtil.getMaxDegreeSubset(50);
	}
	
	public DataStream<Row> buildTopViewAppendMap(){
		CSVGraphUtilMap graphUtil = ((CSVGraphUtilMap) this.graphUtil);
		return graphUtil.produceWrapperStream();	
	}
	
	public DataStream<Row> zoomIn (Integer top, Integer right, Integer bottom, Integer left){
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
		DataStream<Row> vertexStream = graphUtil.getVertexStream()
			.filter(new FilterFunction<Row>(){
			@Override
			public boolean filter(Row value) throws Exception {
				Integer x = (Integer) value.getField(4);
				Integer y = (Integer) value.getField(5);
				return (left < x) &&  (x < right) && (top < y) && (y < bottom);
			}
		});
		String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
					+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
					+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
		Table vertexTable = fsTableEnv.fromDataStream(vertexStream).as("graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree");
		Table wrapperTable = fsTableEnv.fromDataStream(graphUtil.getWrapperStream()).as(wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTable = wrapperTable.join(vertexTable).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		DataStream<Row> wrapperStream = fsTableEnv.toAppendStream(wrapperTable, typeInfo);
		//filter out all wrappers whose edges are already in previous view!!!
		return wrapperStream;
	}
	
	public DataStream<Row> pan(Integer topOld, Integer rightOld, Integer bottomOld, Integer leftOld, Integer xDiff, Integer yDiff){
		Integer topNew = topOld + yDiff;
		Integer rightNew = rightOld + xDiff;
		Integer bottomNew = bottomOld + yDiff;
		Integer leftNew = leftOld + xDiff;
		CSVGraphUtilJoin graphUtil = ((CSVGraphUtilJoin) this.graphUtil);
		DataStream<Row> vertexStreamAll = graphUtil.getVertexStream()
			.filter(new FilterFunction<Row>(){
				@Override
				public boolean filter(Row value) throws Exception {
					Integer x = (Integer) value.getField(4);
					Integer y = (Integer) value.getField(5);
					return (leftNew < x) &&  (x < rightNew) && (topNew < y) && (y < bottomNew);
				}
			});
		List<DataStream<Row>> lStream;
		if ((xDiff == 0) && (yDiff < 0)) {
			lStream = panTop(vertexStreamAll, topOld);
		} else if ((xDiff > 0) && (yDiff < 0)) {
			lStream = panTopRight(vertexStreamAll, topOld, rightOld);
		} else if ((xDiff > 0) && (yDiff == 0)) {
			lStream = panRight(vertexStreamAll, rightOld);
		} else if ((xDiff > 0) && (yDiff > 0)) {
			lStream = panBottomRight(vertexStreamAll, bottomOld, rightOld);
		} else if ((xDiff == 0) && (yDiff > 0)) {
			lStream = panBottom(vertexStreamAll, bottomOld);
		} else if ((xDiff < 0) && (yDiff > 0)) {
			lStream = panBottomLeft(vertexStreamAll, bottomOld, leftOld);
		} else if ((xDiff < 0) && (yDiff == 0)) {
			lStream = panLeft(vertexStreamAll, leftOld);
		} else {
			lStream = panTopLeft(vertexStreamAll, topOld, leftOld);
		}
		
		DataStream<Row> vertexStreamOld = lStream.get(0);
		DataStream<Row> vertexStreamNew = lStream.get(1);
		String vertexFields = "graphId2, vertexIdGradoop, vertexIdNumeric, vertexLabel, x, y, vertexDegree";
		String wrapperFields = "graphId, sourceVertexIdGradoop, sourceVertexIdNumeric, sourceVertexLabel, sourceVertexX, "
				+ "sourceVertexY, sourceVertexDegree, targetVertexIdGradoop, targetVertexIdNumeric, targetVertexLabel, targetVertexX, targetVertexY, "
				+ "targetVertexDegree, edgeIdGradoop, edgeLabel";
		
		//edge stream for all connections within new vertices
		Table vertexTableNew = fsTableEnv.fromDataStream(vertexStreamNew).as(vertexFields);
		Table wrapperTableNew = fsTableEnv.fromDataStream(graphUtil.getWrapperStream()).as(wrapperFields);
		wrapperTableNew = wrapperTableNew.join(vertexTableNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		wrapperTableNew = wrapperTableNew.join(vertexTableNew).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);

		
		//edge stream for all connection between old and new vertices in one direction
		Table vertexTableOld = fsTableEnv.fromDataStream(vertexStreamOld).as(vertexFields);
		Table edgeTableOld1 = fsTableEnv.fromDataStream(graphUtil.getWrapperStream()).as(wrapperFields);
		edgeTableOld1 = edgeTableOld1.join(vertexTableOld).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		edgeTableOld1 = edgeTableOld1.join(vertexTableNew).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
			
		//edge stream for all connection between old and new vertices in the respective other direction
		Table edgeTableOld2 = fsTableEnv.fromDataStream(graphUtil.getWrapperStream()).as(wrapperFields);
		edgeTableOld2 = edgeTableOld2.join(vertexTableNew).where("vertexIdGradoop = sourceVertexIdGradoop").select(wrapperFields);
		edgeTableOld2 = edgeTableOld2.join(vertexTableOld).where("vertexIdGradoop = targetVertexIdGradoop").select(wrapperFields);
		
		RowTypeInfo typeInfo = new RowTypeInfo(new TypeInformation[] {Types.STRING, Types.STRING, 
				Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, Types.INT, Types.INT, Types.LONG,
				Types.STRING, Types.STRING});
		DataStream<Row> wrapperStreamNew = fsTableEnv.toAppendStream(wrapperTableNew, typeInfo);
		DataStream<Row> wrapperStreamOldToNew = fsTableEnv.toAppendStream(edgeTableOld1, typeInfo);
		DataStream<Row> wrapperStreamNewToOld = fsTableEnv.toAppendStream(edgeTableOld2, typeInfo);
		DataStream<Row> wrapperStream = wrapperStreamNew.union(wrapperStreamOldToNew).union(wrapperStreamNewToOld);
		wrapperStream.print().setParallelism(1);
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
