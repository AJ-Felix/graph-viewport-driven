package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.ArrayList;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;



public class MaxDegreeSubset {
	
	//define Vertex Accumulator Object
	public static class VertexAccum {
		String bool;
		String v_id;
		String degree;
	}
	
	//define Vertex AggregateFunction Object to make downstream join on single rows possible
	public static class CurrentVertex extends AggregateFunction<Tuple2<String, String>, VertexAccum>{

		private static final long serialVersionUID = 1L;

		@Override
		public VertexAccum createAccumulator() {
			return new VertexAccum();
		}

		@Override
		public Tuple2<String, String> getValue(VertexAccum accumulator) {
			return new Tuple2<String, String>(accumulator.bool, accumulator.degree);
		}
		
		public void accumulate(VertexAccum accumulator, String bool,  String v_id, String degree) {
			accumulator.bool = bool;
			accumulator.v_id = v_id;
			accumulator.degree = degree;
		}
	}
	
	public static ArrayList<DataStream<Tuple2<Boolean, Row>>> getStreams(
			StreamExecutionEnvironment fsEnv,
			StreamTableEnvironment fsTableEnv, 
			DataStreamSource<Tuple5<String, String, String, String, String>> vertexStreamInput,
			DataStreamSource<Tuple5<String, String, String, String, String>> edgeStreamInput,
			DataStream<Tuple2<Boolean, Row>> datastreamDegree,
			LogicalGraph log) throws Exception {
		
		//create flink table from degree hbase table
		TupleTypeInfo<Tuple3<String, String,String>> datastreamDegreeConvertedInfo = 
			new TupleTypeInfo<Tuple3<String, String, String>>(new TypeInformation[]{Types.STRING,Types.STRING, Types.STRING});
		
			//convert DataStream to enable attribute comparison in table join
			DataStream<Tuple3<String,String,String>> datastreamDegreeConverted = datastreamDegree.map(new MapFunction<Tuple2<Boolean,Row>, Tuple3<String,String,String>>() {
				private static final long serialVersionUID = -7026515741245426370L;
	
				@Override
				public Tuple3<String, String, String> map(Tuple2<Boolean, Row> value) throws Exception {
					String bool = value.f0.toString();
					String vertexId = value.f1.getField(0).toString();
					String degree = ((Row) value.f1.getField(1)).getField(0).toString();
					return new Tuple3<String, String, String>(bool, vertexId, degree);
				}
			}).returns(datastreamDegreeConvertedInfo).setParallelism(1);
			Table degreeTable = fsTableEnv.fromDataStream(datastreamDegreeConverted).as("bool, vertexId, degree");
			fsTableEnv.registerFunction("currentVertex", new CurrentVertex());
			degreeTable = degreeTable
					.groupBy("vertexId")
					.aggregate("currentVertex(bool, vertexId, degree) as (bool, degree)")
					.select("vertexId, degree, bool")
					.filter("bool = 'true'");		
				//sammeln von bool verändert Verhalten kritisch!
		
		//create flink vertices table from vertex stream
		Table vertexTable = fsTableEnv.fromDataStream(vertexStreamInput).as("vertexIdCompare, vertexLabel, vertexIdLayout, x, y");
		
		//table joins for vertex table
		Table vertex_result_table = degreeTable.join(vertexTable).where("vertexId = vertexIdCompare").select("vertexIdLayout, x, y, degree");
		
		//convert joined vertex table to data stream
		RowTypeInfo rowTypeInfoVertices = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING, Types.STRING}, 
				new String[] {"vertexIdLayout", "x", "y", "degree"});
		DataStream<Tuple2<Boolean, Row>> vertexStream = fsTableEnv.toRetractStream(vertex_result_table, rowTypeInfoVertices);
		
		//create flink edge table from edge stream
		Table edgeTable = fsTableEnv.fromDataStream(edgeStreamInput).as("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");	
		
		//table joins for edges table
		edgeTable = degreeTable.join(edgeTable).where("vertexId = vertexIdSourceOld")
				.select("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
		edgeTable = degreeTable.join(edgeTable).where("vertexId = vertexIdTargetOld")
				.select("edgeId, vertexIdSourceNew, vertexIdTargetNew");
		
		//convert joined edge table to data stream
		RowTypeInfo rowTypeInfoEdges = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING}, 
				new String[] {"edgeId", "vertexIdSourceNew", "vertexIdTargetNew"});
		DataStream<Tuple2<Boolean, Row>> edgeStream = fsTableEnv.toRetractStream(edgeTable, rowTypeInfoEdges);
		
		ArrayList<DataStream<Tuple2<Boolean, Row>>> streams = new ArrayList<DataStream<Tuple2<Boolean, Row>>>();
		streams.add(vertexStream);
		streams.add(edgeStream);
		return streams;
	}
}
