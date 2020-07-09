package aljoschaRydzyk.Gradoop_Flink_Prototype;

public class VVEdgeWrapper {
	private VertexCustom sourceVertex;
	private VertexCustom targetVertex;
	private EdgeCustom edge;
	
	public VVEdgeWrapper(VertexCustom sourceVertex, VertexCustom targetVertex, EdgeCustom edge) {
		this.sourceVertex = sourceVertex;
		this.targetVertex = targetVertex;
		this.edge = edge;
	}
	
	public VertexCustom getSourceVertex() {
		return this.sourceVertex;
	}
	
	public VertexCustom getTargetVertex() {
		return this.targetVertex;
	}
	
	public EdgeCustom getEdge() {
		return this.edge;
	}
}
