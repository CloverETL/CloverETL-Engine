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

/**
 * This is general model view to a transformation graph which is subgraph.
 * If a subgraph (sgrf) is analysed as top level graph, its MV representation is {@link MVGraph}.
 * This model view is used by {@link MetadataPropagationResolver}.
 * 
 * @author martin (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 9. 2016
 */
public interface MVSubgraph extends MVGraph {

	/**
	 * @return MV representation of SubgraphInput component
	 */
	public MVComponent getSubgraphInputComponent();
	
	/**
	 * @return MV representation of SubgraphOutput component
	 */
	public MVComponent getSubgraphOutputComponent();
	
	/**
	 * @return MV representation of all output edges of SubgraphInput component
	 */
	public Map<Integer, MVEdge> getInputEdges();
	
	/**
	 * @return MV representation of all input edges of SubgraphOutput component
	 */
	public Map<Integer, MVEdge> getOutputEdges();
	
}
