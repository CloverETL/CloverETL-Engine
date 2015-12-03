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
package org.jetel.graph.modelview.impl;

import java.io.Serializable;
import java.util.Map;

import org.jetel.graph.Edge;
import org.jetel.graph.modelview.MVEdge;
import org.jetel.graph.modelview.MVGraph;

/**
 * This class represents result of automatic metadata propagation.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9. 9. 2015
 */
public class MetadataPropagationResult implements Serializable {

	private static final long serialVersionUID = -8276594149779221041L;

	private MetadataPropagationResolver metadataPropagationResolver;
	
	public MetadataPropagationResult(MetadataPropagationResolver metadataPropagationResolver) {
		this.metadataPropagationResolver = metadataPropagationResolver;
	}

	/**
	 * Returned model can be used to investigate which metadata has been propagated to which edges.
	 * @return model for root graph
	 */
	public MVGraph getRootMVGraph() {
		return metadataPropagationResolver.getRootMVGraph();
	}

	/**
	 * @param edge
	 * @return model for given edge, which is searched recursively
	 */
	public MVEdge getMVEdge(Edge edge) {
		return metadataPropagationResolver.getMVEdge(edge);
	}

	/**
	 * @param edgeId
	 * @return model for an edge with given identifier (the search is not recursive)
	 */
	public MVEdge getMVEdge(String edgeId) {
		return getRootMVGraph().getMVEdge(edgeId);
	}

	/**
	 * @return map with models for all edges in the root graph
	 */
	public Map<String, MVEdge> getMVEdges() {
		return getRootMVGraph().getMVEdges();
	}

}
