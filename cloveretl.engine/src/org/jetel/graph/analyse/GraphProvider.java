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


/**
 * This interface is primarily dedicated for {@link GraphCycleInspector} class.
 * In general two implementations exist of this interface.
 * {@link SingleGraphProvider} is used for cycle detection in basic transformation graphs.
 * ClusteredGraphProvider is used for cycle detection in clustered graphs.
 * This interface allows to use same cycle detection algorithm for both cases.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.12.2012
 * @see GraphCycleInspector
 * @see InspectedComponent
 */
public interface GraphProvider {

	/**
	 * This method can be invoked repeatedly and handlers for all components are sequentially returned.
	 * The {@link InspectedComponent} is handler for component, which can be used later for graph layout tracking.
	 */
	public InspectedComponent getNextComponent();

	/**
	 * Resets the provider to initial state. 
	 */
	public void reset();
	
}
