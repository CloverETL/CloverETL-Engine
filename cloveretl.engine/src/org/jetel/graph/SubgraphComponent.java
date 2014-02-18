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

import org.jetel.enums.EdgeTypeEnum;
import org.jetel.util.GraphUtils;

/**
 * Only implementation of this interface is Subgraph component. This interface provides
 * input (output edges of SubgraphInput component) and output (input edges of SubgraphOutput component)
 * edges of executed subgraph. These edges are used to decide whether they can share edge base with edges
 * from parent graph. Moreover, if the edge base can be shared, edge types in parent graph needs to be
 * properly updated to satisfy needs from both parent graph and subgraph.
 * 
 * @see GraphUtils#combineEdges(EdgeTypeEnum, EdgeTypeEnum)
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31. 10. 2013
 */
public interface SubgraphComponent {

	/**
	 * @param portIndex
	 * @return input edge of executed subgraph
	 */
	public Edge getSubgraphInputEdge(int portIndex);

	/**
	 * @param portIndex
	 * @return output edge of executed subgraph
	 */
	public Edge getSubgraphOutputEdge(int portIndex);
	
}
