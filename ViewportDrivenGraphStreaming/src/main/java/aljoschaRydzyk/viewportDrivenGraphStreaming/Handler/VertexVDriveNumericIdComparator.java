package aljoschaRydzyk.viewportDrivenGraphStreaming.Handler;

import java.util.Comparator;

import aljoschaRydzyk.viewportDrivenGraphStreaming.FlinkOperator.GraphObject.VertexVDrive;

public class VertexVDriveNumericIdComparator implements Comparator<VertexVDrive>{
	@Override
	public int compare(VertexVDrive v1, VertexVDrive v2) {
		if (v1.getIdNumeric() > v2.getIdNumeric()) return -1;
		else if (v1.getIdNumeric() == v2.getIdNumeric()) return 0;
		else return 1;
	}	
}
