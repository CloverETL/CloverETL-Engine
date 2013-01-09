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
import org.jetel.graph.Node;

/**
 * This interface is used by {@link GraphCycleInspector} for graph layout tracking.
 * This is handler for a component and provides iterable view to all connected
 * components.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.12.2012
 * @see GraphCycleInspector
 * @see SingleGraphProvider
 */
public interface InspectedComponent {

	/**
	 * Provides iterable view to all components connected with wrapped component.
	 * Sequentially returns wrappers for all connected components.
	 */
	public InspectedComponent getNextComponent();

	/**
	 * @return an edge through which was reached the wrapped component
	 * in cycle detection algorithm (see {@link GraphCycleInspector}
	 */
	public Edge getEntryEdge();

	/**
	 * @return wrapped component
	 */
	public Node getComponent();
	
}
