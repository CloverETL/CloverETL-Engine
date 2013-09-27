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
package org.jetel.graph;

import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MVEngineEdge;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This graph pre-processor is executed immediately after graph instantiation.
 * For now only automatic metadata propagation is evaluated by this graph pre-processor.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 27. 8. 2013
 * @see MetadataPropagationResolver
 */
public class GraphPreProcessor {
	
	private TransformationGraph graph;

	private MetadataPropagationResolver<DataRecordMetadata> metadataPropagationResolver = new MetadataPropagationResolver<DataRecordMetadata>();
	
	public GraphPreProcessor(TransformationGraph graph) {
		this.graph = graph;
	}
	
	/**
	 * Pre-process the graph. Automatic metadata propagation is performed.
	 */
	public void preProcess() {
		for (Edge edge : graph.getEdges().values()) {
			if (edge.getMetadata() == null) {
				MVMetadata<DataRecordMetadata> metadata = metadataPropagationResolver.findMetadata(new MVEngineEdge(edge));
				if (metadata != null) {
					edge.setMetadata(metadata.getMetadata());
				}
			}
		}
	}
	
}
