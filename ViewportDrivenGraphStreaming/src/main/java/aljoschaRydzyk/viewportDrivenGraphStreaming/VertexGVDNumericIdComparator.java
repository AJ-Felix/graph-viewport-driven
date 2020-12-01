package aljoschaRydzyk.viewportDrivenGraphStreaming;

import java.util.Comparator;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexGVD;

public class VertexGVDNumericIdComparator implements Comparator<VertexGVD>{
	@Override
	public int compare(VertexGVD v1, VertexGVD v2) {
		if (v1.getIdNumeric() > v2.getIdNumeric()) return -1;
		else if (v1.getIdNumeric() == v2.getIdNumeric()) return 0;
		else return 1;
	}	
}
