package aljoschaRydzyk.Gradoop_Flink_Prototype;

public class VertexObject {
	
		private String bool;
		private String id;
		private String x;
		private String y;
		private String degree;
		
		public VertexObject() {
		}
		
		public VertexObject(String bool, String id, String x, String y, String degree) {
			super();
			this.bool = bool;
			this.id = id;
			this.x = x;
			this.y = y;
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
		
		public String getX() {
			return this.x;
		}
		
		public void setX(String x) {
			this.x = x;
		}
		
		public String getY() {
			return this.y;
		}
		
		public void setY(String y) {
			this.y = y;
		}
		
		public String getDegree() {
			return this.degree;
		}
		
		public void setDegree(String degree) {
			this.degree = degree;
		}
}
