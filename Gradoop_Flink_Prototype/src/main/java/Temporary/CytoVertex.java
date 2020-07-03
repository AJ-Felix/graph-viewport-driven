package Temporary;

public class CytoVertex{
	
	public String data;
	private Integer x;
	private Integer y;
	
	
	public CytoVertex(String data, Integer x, Integer y) {
		this.data = data;
		this.x = x;
		this.y = y;
	}
	
	public String getData() {
		return this.data;
	}
	
	public void setData(String data) {
		this.data = data;
	}
	
	public Integer getX() {
		return this.x;
	}
	
	public void setX(Integer x) {
		this.x = x;
	}
	
	public Integer getY() {
		return this.y;
	}
	
	public void setY(Integer y) {
		this.y = y;
	}
	
	@Override
	public String toString() {
		return "{ id: '" + 
			this.getData() + "' }, position: { x: " + 
			this.getX() + ", y: " +
			this.getY() + " }";
	}
}
