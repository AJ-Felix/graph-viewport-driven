package Temporary;

public class EdgeObject {
	private String bool;
	private String id;
	private String source;
	private String target;
	
	public EdgeObject() {
	}
	
	public EdgeObject(String bool, String id, String source, String target) {
		super();
		this.bool = bool;
		this.id = id;
		this.source = source;
		this.target = target;
	}
	
	public String getBool() {
		return this.bool;
	}
	
	public void setBool(String bool) {
		this.bool = bool;
	}
	
	public String getId() {
		return this.id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getSource() {
		return this.source;
	}
	
	public void setSource(String source) {
		this.source = source;
	}
	
	public String getTarget() {
		return this.target;
	}
	
	public void setTarget(String target) {
		this.target = target;
	}
}
