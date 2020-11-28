package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.algorithms.gelly.vertexdegrees.DistinctVertexDegrees;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

public class EPGMtoDegMatrix {
	StreamExecutionEnvironment fsEnv;
	StreamTableEnvironment fsTableEnv;
	LogicalGraph logical_graph;
	boolean sorting = false;
	
	public EPGMtoDegMatrix(
			StreamExecutionEnvironment fsEnv,
			StreamTableEnvironment fsTableEnv,
			LogicalGraph logical_graph) {
		this.fsEnv = fsEnv;
		this.fsTableEnv = fsTableEnv;
		this.logical_graph = logical_graph;
	}
	
	public Table getDegMatrix() throws Exception{
		String propertyKeyDegree = "degree";
		String propertyKeyInDegree = "inDegree";
		String propertyKeyOutDegree = "outDegree";
		boolean includeZeroDegreeVertices = true;
		
		Table table = null;
		
		//invoke Gradoop vertex degree operation
		LogicalGraph result_graph = logical_graph.callForGraph(new DistinctVertexDegrees(propertyKeyDegree, propertyKeyInDegree, propertyKeyOutDegree, includeZeroDegreeVertices));
		DataSet<EPGMVertex> ds_vertices = result_graph.getVertices();
		

		//sorting is disabled by default since HBase does not allow sorted entries
		if (sorting) {
			KeySelector<EPGMVertex, Long> keySelector = new KeySelector<EPGMVertex, Long>(){
				private static final long serialVersionUID = 1L;
				
				@Override
				public Long getKey(EPGMVertex vertex) throws Exception {
					return vertex.getPropertyValue("degree").getLong();
				}
			};
			ds_vertices = ds_vertices.sortPartition(keySelector, Order.DESCENDING).setParallelism(1);
		};
		
		// transform DataSet<EPGMVertex> to DataSet<Row> to allow sinking in HBase
		RowTypeInfo ds_row_typeInfo = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.ROW(Types.INT)}, 
				new String[] {"rowkey", "row"});
		DataSet<Row> ds_row = ds_vertices.map(new MapFunction<EPGMVertex, Row>() {
			private static final long serialVersionUID = -7026515741245426370L;

			@Override
			public Row map(EPGMVertex vertex) throws Exception {
				String vertex_id = vertex.getId().toString();
				int vertex_degree = (int) vertex.getPropertyValue("degree").getLong();
				Row row = Row.of(vertex_id, Row.of(vertex_degree));
				return row;
			}
		}).returns(ds_row_typeInfo);
		
		//Collecting Dataset into List and back into DataStreams since BatchTableEnvironment does not allow sinking tables "BatchTableSink [...] required"
		//and direct conversion from DataSet to DataStream is not possible
		List<Row> list_ds_row = ds_row.collect();
		DataStream<Row> dstream_vertices = fsEnv.fromCollection(list_ds_row, ds_row_typeInfo);
		table = fsTableEnv.fromDataStream(dstream_vertices);
		return table;	
	}

	public LogicalGraph getDegMatrixLogicalGraph() throws Exception{
		String propertyKeyDegree = "degree";
		String propertyKeyInDegree = "inDegree";
		String propertyKeyOutDegree = "outDegree";
		boolean includeZeroDegreeVertices = true;
		
		//invoke Gradoop vertex degree operation
		LogicalGraph result_graph = logical_graph.callForGraph(new DistinctVertexDegrees(propertyKeyDegree, propertyKeyInDegree, propertyKeyOutDegree, includeZeroDegreeVertices));
		return result_graph;
	}
		
		
}
