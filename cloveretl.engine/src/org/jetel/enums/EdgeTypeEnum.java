package org.jetel.enums;

import java.lang.reflect.InvocationTargetException;

import org.jetel.graph.BufferedEdge;
import org.jetel.graph.DirectEdge;
import org.jetel.graph.DirectEdgeFastPropagate;
import org.jetel.graph.Edge;
import org.jetel.graph.EdgeBase;
import org.jetel.graph.PhaseConnectionEdge;


public enum EdgeTypeEnum {
	/**  Proxy represents Direct Edge */
	DIRECT("direct", DirectEdge.class),
	/**  Proxy represents Direct Edge fast propagate */
	DIRECT_FAST_PROPAGATE("directFastPropagate", DirectEdgeFastPropagate.class),
	/**  Proxy represents Buffered Edge */
	BUFFERED("buffered", BufferedEdge.class),
	/** Proxy represents Edge connecting two different phases */
	PHASE_CONNECTION("phaseConnection", PhaseConnectionEdge.class);
	
	String name;
	
	Class<? extends EdgeBase> edgeBaseClass;
	
	EdgeTypeEnum(String name, Class<? extends EdgeBase> edgeBaseClass) {
		this.name = name;
		this.edgeBaseClass = edgeBaseClass;
	}
	
	public EdgeBase createEdgeBase(Edge edge) {
		try {
			return edgeBaseClass.getConstructor(Edge.class).newInstance(edge);
		} catch (Exception e) {
			throw new RuntimeException("Can not create edge-base for this type of edge '" + this.name + "'.", e);
		}
	}
	
	public static EdgeTypeEnum valueOfIgnoreCase(String edgeTypeName) {
        if(edgeTypeName == null) {
            return null;
        }
        for(EdgeTypeEnum et : EdgeTypeEnum.values()) {
            if(edgeTypeName.equalsIgnoreCase(et.name)) {
                return et;
            }
        }
        return null;
	}
}	

