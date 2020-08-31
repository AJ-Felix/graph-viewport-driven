package Temporary;

public class VertexCustom {
	public String idGradoop;
	public String label;
	public Integer x;
	public Integer y;
	public Integer idNumeric;
	public Long degree;
	
	public VertexCustom() {
		
	}
	
	public VertexCustom(String idGradoop, String label, Integer idNumeric, Integer x, Integer y, Long degree) {
		this.idGradoop = idGradoop;
		this.label = label;
		this.x = x;
		this.y = y;
		this.idNumeric = idNumeric;
		this.degree = degree;
	}
	
	public String getIdGradoop() {
		return this.idGradoop;
	}
	
	public String getLabel() {
		return this.label;
	}
	
	public Integer getX() {
		return this.x;
	}
	
	public Integer getY() {
		return this.y;
	}
	
	public Integer getIdNumeric() {
		return this.idNumeric;
	}
	
	public void setIdGradoop(String idGradoop) {
		this.idGradoop = idGradoop;
	}
	
	public void setLabel(String label) {
		this.label = label;
	}
	
	public void setX(Integer x) {
		this.x = x;
	}
	
	public void setY(Integer y) {
		this.y = y;
	}
	
	public void setIdNumeric(Integer idNumeric) {
		this.idNumeric = idNumeric;
	}
	
	public void setDegree(Long degree) {
		this.degree = degree;
	}
	
	public Long getDegree() {
		return this.degree;
	}
}