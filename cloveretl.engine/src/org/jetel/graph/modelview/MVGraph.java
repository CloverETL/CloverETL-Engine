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
package org.jetel.graph.modelview;

import java.util.Map;

import org.jetel.graph.Edge;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This is general model view to a transformation graph.
 * This model view is used by {@link MetadataPropagationResolver}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6. 3. 2014
 */
public interface MVGraph extends MVGraphElement {

	@Override
	public TransformationGraph getModel();

	/**
	 * @param componentId id of requested component
	 * @return MV representation of a component from this graph 
	 */
	public MVComponent getMVComponent(String componentId);

	/**
	 * @return MV representation of all subgraph components from this graph 
	 */
	public Map<MVComponent, MVGraph> getMVSubgraphs();

	/**
	 * @param edgeId id of requested edge
	 * @return MV representation of an edge from this graph 
	 */
	public MVEdge getMVEdge(String edgeId);

	/**
	 * @param edge requested edge
	 * @return MV representation of the requested edge; the edge can be also from a subgraph 
	 */
	public MVEdge getMVEdgeRecursive(Edge edge);

	/**
	 * @param metadata
	 * @return newly created MV representation of given metadata 
	 */
	public MVMetadata createMVMetadata(DataRecordMetadata metadata);

	/**
	 * @param metadata
	 * @param priority
	 * @return newly created MV representation of given metadata 
	 */
	public MVMetadata createMVMetadata(DataRecordMetadata metadata, int priority);
	
}
