package aljoschaRydzyk.Gradoop_Flink_Prototype;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CytoGraph {
	private transient HashMap<String, CytoVertex> vertex_map = new HashMap<>();
	
	public void addVertex (String data, Integer x, Integer y) {
		this.vertex_map.put(data, new CytoVertex(data, x, y));
	}
	
//	public void removeVertex (CytoVertex<String, Integer, Integer> vertex) {
//		this.vertex_map.remove(vertex.getData());
//	}
	
	@Override
	public String toString() {
		String result = "[";
		for (Map.Entry<String, CytoVertex> entry : this.vertex_map.entrySet()) {
			result += "{ group: 'nodes', data: " + entry.getValue().toString() + "},";
		}
		result = result.substring(0, result.length() - 1 );
		result += "{ group: 'nodes', data: { id: 'n0' }, position: { x: 100, y: 100 } }";
		result += "]";
		return result;
	}
}
