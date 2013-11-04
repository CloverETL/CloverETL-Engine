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
 * Only implementation of this interface is SubGraph component. This interface provides
 * types of edges derived from sub-graph instance. These types are used for local sub-graph
 * execution, where edges from parent graph shares edge base with edges from sub-graph.
 * The real type of this shared edge is combination of these two types.
 * @see GraphUtils#combineEdges(EdgeTypeEnum, EdgeTypeEnum)
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31. 10. 2013
 */
public interface SubGraphComponent {

	/**
	 * @param portIndex
	 * @return type of input edge of executed sub-graph
	 */
	public EdgeTypeEnum getInputEdgeType(int portIndex);

	/**
	 * @param portIndex
	 * @return type of output edge of executed sub-graph
	 */
	public EdgeTypeEnum getOutputEdgeType(int portIndex);
	
}
