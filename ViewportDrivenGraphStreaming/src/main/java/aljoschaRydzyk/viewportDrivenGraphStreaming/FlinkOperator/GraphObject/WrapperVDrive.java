package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject;

public class WrapperVDrive {
	private VertexVDrive sourceVertex;
	private VertexVDrive targetVertex;
	private EdgeVDrive edge;
	private String edgeIdGradoop;
	private String edgeLabel;
	private String sourceIdGradoop;
	private String sourceLabel;
	private int sourceX;
	private int sourceY;
	private long sourceIdNumeric;
	private long sourceDegree;
	private int sourceZoomLevel;
	private String targetIdGradoop;
	private String targetLabel;
	private int targetX;
	private int targetY;
	private long targetIdNumeric;
	private long targetDegree;
	private int targetZoomLevel;
	
	public WrapperVDrive (VertexVDrive sourceVertex, VertexVDrive targetVertex, EdgeVDrive edge) {
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
		this.sourceZoomLevel = sourceVertex.getZoomLevel();
		this.targetIdGradoop = targetVertex.getIdGradoop();
		this.targetLabel = targetVertex.getLabel();
		this.targetIdNumeric = targetVertex.getIdNumeric();
		this.targetX = targetVertex.getX();
		this.targetY = targetVertex.getY();
		this.targetDegree = targetVertex.getDegree();
		this.targetZoomLevel = targetVertex.getZoomLevel();
	}
	
	public void setSourceVertex(VertexVDrive sourceVertex) {
		this.sourceVertex = sourceVertex;
	}
	
	public VertexVDrive getSourceVertex() {
		return this.sourceVertex;
	}
	
	public void setTargetVertex(VertexVDrive targetVertex) {
		this.targetVertex = targetVertex;
	}
	
	public VertexVDrive getTargetVertex() {
		return this.targetVertex;
	}
	
	public void setEdge(EdgeVDrive edge) {
		this.edge = edge;
	}
	
	public EdgeVDrive getEdge() {
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
	
	public int getSourceZoomLevel() {
		return this.sourceZoomLevel;
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
	
	public int getTargetZoomLevel() {
		return this.targetZoomLevel;
	}
}
