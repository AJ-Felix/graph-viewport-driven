package aljoschaRydzyk.viewportDrivenGraphStreaming;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.s1ck.ldbc.LDBCToFlink;
import org.s1ck.ldbc.tuples.LDBCEdge;
import org.s1ck.ldbc.tuples.LDBCVertex;

public class LdbcImporter {
	private String sourcePath;
	private ExecutionEnvironment env;
	
	public LdbcImporter(String sourcePath, ExecutionEnvironment env) {
		this.sourcePath = sourcePath;
		this.env = env;
	}
	
	public void parseToLogicalGraph() {
		LDBCToFlink ldbcToFlink = new LDBCToFlink(sourcePath, env);
		DataSet<LDBCVertex> vertices = ldbcToFlink.getVertices();
		DataSet<LDBCEdge> edges = ldbcToFlink.getEdges();
		try {
			vertices.print();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
		
}
