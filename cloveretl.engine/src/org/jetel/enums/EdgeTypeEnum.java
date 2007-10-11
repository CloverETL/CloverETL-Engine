package org.jetel.enums;

import org.jetel.graph.Edge;

public enum EdgeTypeEnum {
	DIRECT,
	BUFFERED,
	DIRECTFASTPROPAGATE;
	
	public static int UNKNOWN = -1;
	
	public static EdgeTypeEnum valueOfIgnoreCase(String edgeType) {
        if(edgeType == null) {
            return null;
        }
        for(EdgeTypeEnum et : EdgeTypeEnum.values()) {
            if(edgeType.equalsIgnoreCase(et.toString())) {
                return et;
            }
        }
        return null;
	}
	
	public static int getEdgeType(EdgeTypeEnum edgeType) {
		if (edgeType == null) return UNKNOWN;
		else if (edgeType.equals(DIRECT)) return Edge.EDGE_TYPE_DIRECT;
		else if (edgeType.equals(BUFFERED)) return Edge.EDGE_TYPE_BUFFERED;
		else if (edgeType.equals(DIRECTFASTPROPAGATE)) return Edge.EDGE_TYPE_DIRECT_FAST_PROPAGATE;
		else return UNKNOWN;
	}
}	

