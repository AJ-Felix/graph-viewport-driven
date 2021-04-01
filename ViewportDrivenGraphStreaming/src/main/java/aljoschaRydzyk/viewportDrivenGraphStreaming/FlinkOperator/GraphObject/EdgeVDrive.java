package aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject;

public class EdgeVDrive {
	private String idGradoop;
	private String label;
	private String sourceIdGradoop;
	private String targetIdGradoop;
	
	public EdgeVDrive(String idGradoop, String label, String sourceIdGradoop, String targetIdGradoop) {
		this.idGradoop = idGradoop;
		this.label = label;
		this.sourceIdGradoop = sourceIdGradoop;
		this.targetIdGradoop = targetIdGradoop;
	}
	
	public String getIdGradoop() {
		return this.idGradoop;
	}
	
	public String getLabel() {
		return this.label;
	}
	
	public String getSourceIdGradoop() {
		return this.sourceIdGradoop;
	}
	
	public String getTargetIdGradoop() {
		return this.targetIdGradoop;
	}
	
	public void setIdGradoop(String idGradoop) {
		this.idGradoop = idGradoop;
	}
	
	public void setLabel(String label) {
		this.label = label;
	}
	
	public void setSourceIdGradoop(String sourceIdGradoop) {
		this.sourceIdGradoop = sourceIdGradoop;
	}
	
	public void setTargetIdGradoop(String targetIdGradoop) {
		this.targetIdGradoop = targetIdGradoop;
	}
}
