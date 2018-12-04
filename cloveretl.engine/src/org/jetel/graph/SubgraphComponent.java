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

/**
 * Only implementation of this interface is Subgraph component.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 31. 10. 2013
 */
public interface SubgraphComponent {
	
	/**
	 * @param strict should be an exception thrown or just null return in case of some error
	 * @return subgraph instance without metadata propagation
	 * (this instance is dedicated for further automatic metadata propagation) 
	 */
	public TransformationGraph getSubgraphNoMetadataPropagation(boolean strict);

	/**
	 * Sets flag which indicates the subgraph should be executed in fast-propagate mode.
	 * Fast-propagate subgraph execution is used if the subgraph is part of a loop (see Loop component).
	 * All edges between SubgraphInput and SubgraphOutput components have to be fast-propagated.
	 * @param fastPropagateExection
	 */
	public void setFastPropagateExecution(boolean fastPropagateExection);
	
}
