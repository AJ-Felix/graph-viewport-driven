package aljoschaRydzyk.Gradoop_Flink_Prototype;

import Temporary.VertexCustom;

public class VVEdgeWrapper {
	private VertexCustom sourceVertex;
	private VertexCustom targetVertex;
	private EdgeCustom edge;
	private String edgeIdGradoop;
	private String edgeLabel;
	private String sourceIdGradoop;
	private String sourceLabel;
	private Integer sourceX;
	private Integer sourceY;
	private Integer sourceIdNumeric;
	private Long sourceDegree;
	private String targetIdGradoop;
	private String targetLabel;
	private Integer targetX;
	private Integer targetY;
	private Integer targetIdNumeric;
	private Long targetDegree;
	
	
	public VVEdgeWrapper() {
		
	}
	
	public VVEdgeWrapper (VertexCustom sourceVertex, VertexCustom targetVertex, EdgeCustom edge) {
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
	
	public void setSourceVertex(VertexCustom sourceVertex) {
		this.sourceVertex = sourceVertex;
	}
	
	public VertexCustom getSourceVertex() {
		return this.sourceVertex;
	}
	
	public void setTargetVertex(VertexCustom targetVertex) {
		this.targetVertex = targetVertex;
	}
	
	public VertexCustom getTargetVertex() {
		return this.targetVertex;
	}
	
	public void setEdge(EdgeCustom edge) {
		this.edge = edge;
	}
	
	public EdgeCustom getEdge() {
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
	
	public void setSourceIdNumeric(Integer sourceIdNumeric) {
		this.sourceIdNumeric = sourceIdNumeric;
	}
	
	public Integer getSourceIdNumeric() {
		return this.sourceIdNumeric;
	}
	
	public void setSourceX(Integer sourceX) {
		this.sourceX = sourceX;
	}
	
	public Integer getSourceX() {
		return this.sourceX;
	}
	
	public void setSourceY(Integer sourceY) {
		this.sourceY = sourceY;
	}
	
	public Integer getSourceY() {
		return this.sourceY;
	}
	
	public void setSourceDegree(Long sourceDegree) {
		this.sourceDegree = sourceDegree;
	}
	
	public Long getSourceDegree() {
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
	
	public void setTargetIdNumeric(Integer targetIdNumeric) {
		this.targetIdNumeric = targetIdNumeric;
	}
	
	public Integer getTargetIdNumeric() {
		return this.targetIdNumeric;
	}
	
	public void setTargetX(Integer targetX) {
		this.targetX = targetX;
	}
	
	public Integer getTargetX() {
		return this.targetX;
	}
	
	public void setTargetY(Integer targetY) {
		this.targetY = targetY;
	}
	
	public Integer getTargetY() {
		return this.targetY;
	}
	
	public void setTargetDegree(Long targetDegree){
		this.targetDegree = targetDegree;
	}
	
	public Long getTargetDegree() {
		return this.targetDegree;
	}
}
