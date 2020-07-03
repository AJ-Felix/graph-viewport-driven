package Temporary;

public class VertexObject {
	
		private String bool;
		private String id;
		private Integer x;
		private Integer y;
		private String degree;
		
		public VertexObject() {
		}
		
		public VertexObject(String bool, String id, String x, String y, String degree) {
			super();
			this.bool = bool;
			this.id = id;
			this.x = Integer.parseInt(x);
			this.y = Integer.parseInt(y);
			this.degree = degree;
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
		
		public String getDegree() {
			return this.degree;
		}
		
		public void setDegree(String degree) {
			this.degree = degree;
		}
}
