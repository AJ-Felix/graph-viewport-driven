package Temporary;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;

import aljoschaRydzyk.Gradoop_Flink_Prototype.EdgeCustom;
import aljoschaRydzyk.Gradoop_Flink_Prototype.VertexCustom;



public class MaxDegreeSubset {
	
	//define Vertex Accumulator Object
	public static class VertexAccum {
		String bool;
		String v_id;
		Long degree;
	}
	
	//define Vertex AggregateFunction Object to make downstream join on single rows possible
	public static class CurrentVertex extends AggregateFunction<Tuple2<String, Long>, VertexAccum>{

		private static final long serialVersionUID = 1L;

		@Override
		public VertexAccum createAccumulator() {
			return new VertexAccum();
		}

		@Override
		public Tuple2<String, Long> getValue(VertexAccum accumulator) {
			return new Tuple2<String, Long>(accumulator.bool, accumulator.degree);
		}
		
		public void accumulate(VertexAccum accumulator, String bool,  String v_id, Long degree) {
			accumulator.bool = bool;
			accumulator.v_id = v_id;
			accumulator.degree = degree;
		}
	}
	
	public static DataStream<Tuple2<Boolean, Row>> getWrapperStream(
			StreamExecutionEnvironment fsEnv,
			StreamTableEnvironment fsTableEnv, 
			DataStreamSource<VertexCustom> vertexStreamInput,
			DataStreamSource<EdgeCustom> edgeStreamInput,
			DataStreamSource<Row> wrapperStream2,
			DataStream<Row> vertexStreamDegree,
			LogicalGraph log) throws Exception {
		
			Table degreeTable = fsTableEnv.fromDataStream(vertexStreamDegree).as("bool, vertexId, degree");
			fsTableEnv.registerFunction("currentVertex", new CurrentVertex());
			degreeTable = degreeTable
					.groupBy("vertexId")
					.aggregate("currentVertex(bool, vertexId, degree) as (bool, degree)")
					.select("bool, vertexId, degree")
					.filter("bool = 'true'")
					.select("vertexId, degree");		
				//sammeln von bool verändert Verhalten kritisch!

//			RowTypeInfo rowInfo = new RowTypeInfo(new TypeInformation[] {Types.STRING, Types.LONG}, new String[] {"vertexId", "degree"});
//			DataStream<Tuple2<Boolean, Row>> streamDegreeTest = 
//					fsTableEnv.toRetractStream(degreeTable, rowInfo);
//			streamDegreeTest.print();
//			
//			Table degreeTable2 = fsTableEnv.fromDataStream(dataStreamDegree2).as("bool, vertexId, degree");
//			RowTypeInfo info2 = new RowTypeInfo(new TypeInformation[] {Types.BOOLEAN, Types.ROW(Types.STRING, Types.INT)});
//			DataStream<Tuple2<Boolean,Row>> degreeTest = fsTableEnv.toRetractStream(degreeTable2, info2);
//			degreeTest.print();
//			datastreamDegreeConverted.print();
		//create flink vertices table from vertex stream
//		Table vertexTable = fsTableEnv.fromDataStream(vertexStreamInput).as("vertexIdCompare, vertexLabel, vertexIdLayout, x, y");
//		Table vertexTable = fsTableEnv.fromDataStream(vertexStreamInput).as("vertexInput");
		
		//table joins for vertex table
//		Table vertex_result_table = degreeTable.join(vertexTable).where("vertexId = vertexIdCompare").select("vertexIdLayout, x, y, degree");
//		Table vertex_result_table = degreeTable.join(vertexTable).where("vertexId = vertexInput.idGradoop").select("vertexInput");

		
		//convert joined vertex table to data stream
//		RowTypeInfo rowTypeInfoVertices = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.INT, Types.INT, Types.STRING}, 
//				new String[] {"vertexIdLayout", "x", "y", "degree"});
//		RowTypeInfo rowTypeInfoVertices = new RowTypeInfo(new TypeInformation[]{Types.POJO(VertexCustom.class)}, 
//				new String[] {"vertexInput"});
//		DataStream<Tuple2<Boolean, Row>> vertexStream = fsTableEnv.toRetractStream(vertex_result_table, rowTypeInfoVertices);
//		vertexStream.print().setParallelism(1);
		
		//create flink edge table from edge stream
//		Table edgeTable = fsTableEnv.fromDataStream(edgeStreamInput).as("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
//		Table edgeTable = fsTableEnv.fromDataStream(edgeStreamInput).as("edgeInput");	

		
		//table joins for edges table
//		edgeTable = edgeTable.join(degreeTable).where("vertexId = vertexIdSourceOld")
//				.select("edgeId, vertexIdSourceOld, vertexIdTargetOld, vertexIdSourceNew, vertexIdTargetNew");
//		edgeTable = edgeTable.join(degreeTable).where("vertexId = vertexIdTargetOld")
//				.select("edgeId, vertexIdSourceNew, vertexIdTargetNew");
		
		//convert joined edge table to data stream
//		RowTypeInfo rowTypeInfoEdges = new RowTypeInfo(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING}, 
//				new String[] {"edgeId", "vertexIdSourceNew", "vertexIdTargetNew"});
//		DataStream<Tuple2<Boolean, Row>> edgeStream = fsTableEnv.toRetractStream(edgeTable, rowTypeInfoEdges);
		
		//wrapper stream
		String fieldNames = "graphId, edgeIdGradoop, edgeLabel, sourceIdGradoop, sourceIdNumeric, sourceLabel, sourceX, sourceY, sourceDegree, targetIdGradoop,"
				+ "targetIdNumeric, targetLabel, targetX, targetY, targetDegree";
		Table wrapperTable = fsTableEnv.fromDataStream(wrapperStream2).as(fieldNames);
		wrapperTable = wrapperTable.join(degreeTable).where("sourceIdGradoop = vertexId").select(fieldNames);
		wrapperTable = wrapperTable.join(degreeTable).where("targetIdGradoop = vertexId").select(fieldNames);
		RowTypeInfo rowTypeInfoWrappers = new RowTypeInfo(new TypeInformation[] {
				Types.STRING,
				Types.STRING, Types.STRING, Types.STRING, Types.INT, Types.STRING, 
				Types.INT, Types.INT, Types.LONG, Types.STRING, Types.INT, Types.STRING, 
				Types.INT, Types.INT, Types.LONG
				}, new String[] {"graphId", "edgeIdGradoop", "edgeLabel", "sourceIdGradoop", "sourceIdNumeric", "sourceLabel", "sourceX", 
						"sourceY", "sourceDegree", "targetIdGradoop", "targetIdNumeric", "targetLabel", "targetX", "targetY", "targetDegree"});
		DataStream<Tuple2<Boolean, Row>> wrapperStream = fsTableEnv.toRetractStream(wrapperTable, rowTypeInfoWrappers);	
		return wrapperStream;
	}
}
