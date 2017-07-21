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
package org.jetel.graph.analyse;

import org.jetel.graph.Edge;

/**
 * This class defines transitional function from a component to an other component.
 * The transition is possible only if an edge is between components, but this class
 * can define filter on these possible transitions.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8. 10. 2014
 * @see GraphProvider
 */
public interface EdgeFunction {

	/**
	 * @param edge
	 * @param entryEdge edge which has been used to get to the current component (edge.getWriter())
	 * @return true if the given edge can be used for transition from a reader component to writer component
	 */
	public boolean isInputEdgeAllowed(Edge edge, Edge entryEdge);

	/**
	 * @param edge
	 * @param entryEdge edge which has been used to get to the current component (edge.getWriter())
	 * @return true if the given edge can be used for transition from a writer component to reader component
	 */
	public boolean isOutputEdgeAllowed(Edge edge, Edge entryEdge);

}
