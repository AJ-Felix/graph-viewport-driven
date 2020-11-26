package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject;

public class WrapperGVD {
	private VertexGVD sourceVertex;
	private VertexGVD targetVertex;
	private EdgeGVD edge;
	private String edgeIdGradoop;
	private String edgeLabel;
	private String sourceIdGradoop;
	private String sourceLabel;
	private int sourceX;
	private int sourceY;
	private long sourceIdNumeric;
	private long sourceDegree;
	private String targetIdGradoop;
	private String targetLabel;
	private int targetX;
	private int targetY;
	private long targetIdNumeric;
	private long targetDegree;
	
	
	public WrapperGVD() {
		
	}
	
	public WrapperGVD (VertexGVD sourceVertex, VertexGVD targetVertex, EdgeGVD edge) {
		this.sourceVertex = sourceVertex;
		this.targetVertex = targetVertex;
		this.edge = edge;
		this.edgeIdGradoop = edge.getIdGradoop();
		this.edgeLabel = edge.getLabel();
		this.sourceIdGradoop = sourceVertex.getIdGradoop();
		this.sourceLabel = sourceVertex.getLabel();
		this.sourceIdNumeric = sourceVertex.getIdNumeric();
		this.sourceX = sourceVertex.getX();
		this.sourceY = sourceVertex.getY();
		this.sourceDegree = sourceVertex.getDegree();
		this.targetIdGradoop = targetVertex.getIdGradoop();
		this.targetLabel = targetVertex.getLabel();
		this.targetIdNumeric = targetVertex.getIdNumeric();
		this.targetX = targetVertex.getX();
		this.targetY = targetVertex.getY();
		this.targetDegree = targetVertex.getDegree();
	}
	
	public void setSourceVertex(VertexGVD sourceVertex) {
		this.sourceVertex = sourceVertex;
	}
	
	public VertexGVD getSourceVertex() {
		return this.sourceVertex;
	}
	
	public void setTargetVertex(VertexGVD targetVertex) {
		this.targetVertex = targetVertex;
	}
	
	public VertexGVD getTargetVertex() {
		return this.targetVertex;
	}
	
	public void setEdge(EdgeGVD edge) {
		this.edge = edge;
	}
	
	public EdgeGVD getEdge() {
		return this.edge;
	}
	
	public void setEdgeIdGradoop(String edgeIdGradoop) {
		this.edgeIdGradoop = edgeIdGradoop;
	}
	
	public String getEdgeIdGradoop() {
		return this.edgeIdGradoop;
	}
	
	public void setEdgeLabel(String edgeLabel) {
		this.edgeLabel = edgeLabel;
	}
	
	public String getEdgeLabel() {
		return this.edgeLabel;
	}
	
	public void setSourceIdGradoop(String sourceIdGradoop) {
		this.sourceIdGradoop = sourceIdGradoop;
	}
	
	public String getSourceIdGradoop() {
		return this.sourceIdGradoop;
	}
	
	public void setSourceLabel(String sourceLabel) {
		this.sourceLabel = sourceLabel;
	}
	
	public String getSourceLabel() {
		return this.sourceLabel;
	}
	
	public void setSourceIdNumeric(long sourceIdNumeric) {
		this.sourceIdNumeric = sourceIdNumeric;
	}
	
	public long getSourceIdNumeric() {
		return this.sourceIdNumeric;
	}
	
	public void setSourceX(int sourceX) {
		this.sourceX = sourceX;
	}
	
	public int getSourceX() {
		return this.sourceX;
	}
	
	public void setSourceY(int sourceY) {
		this.sourceY = sourceY;
	}
	
	public int getSourceY() {
		return this.sourceY;
	}
	
	public void setSourceDegree(long sourceDegree) {
		this.sourceDegree = sourceDegree;
	}
	
	public long getSourceDegree() {
		return this.sourceDegree;
	}
	
	public void setTargetIdGradoop(String targetIdGradoop) {
		this.targetIdGradoop = targetIdGradoop;
	}
	
	public String getTargetIdGradoop() {
		return this.targetIdGradoop;
	}
	
	public void setTargetLabel(String targetLabel) {
		this.targetLabel = targetLabel;
	}
	
	public String getTargetLabel() {
		return this.targetLabel;
	}
	
	public void setTargetIdNumeric(long targetIdNumeric) {
		this.targetIdNumeric = targetIdNumeric;
	}
	
	public long getTargetIdNumeric() {
		return this.targetIdNumeric;
	}
	
	public void setTargetX(int targetX) {
		this.targetX = targetX;
	}
	
	public int getTargetX() {
		return this.targetX;
	}
	
	public void setTargetY(int targetY) {
		this.targetY = targetY;
	}
	
	public int getTargetY() {
		return this.targetY;
	}
	
	public void setTargetDegree(long targetDegree){
		this.targetDegree = targetDegree;
	}
	
	public long getTargetDegree() {
		return this.targetDegree;
	}
}
