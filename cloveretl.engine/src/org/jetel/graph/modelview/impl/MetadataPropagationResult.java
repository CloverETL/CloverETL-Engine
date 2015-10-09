/*
 * Copyright 2006-2009 Opensys TM by Javlin, a.s. All rights reserved.
 * Opensys TM by Javlin PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 *
 * Opensys TM by Javlin a.s.; Kremencova 18; Prague; Czech Republic
 * www.cloveretl.com; info@cloveretl.com
 *
 */

package org.jetel.graph.modelview.impl;

import java.io.Serializable;

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

}
