/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.enums;

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
		} catch (Throwable e) {
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

