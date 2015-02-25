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
 * This object represents input port of a subgraph with relating settings.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 2. 2015
 */
public class SubgraphInputPort extends SubgraphPort {

	/**
	 * Creates engine model for input port of a subgraph
	 * @param parentSubgraphPorts subgraph ports container
	 * @param index index of the subgraph port
	 * @param required true if an edge attached to the port is required
	 * @param keepEdge if the port is not required (is optional),
	 * the related edge is either kept or removed from the subgraph
	 */
	public SubgraphInputPort(SubgraphPorts parentSubgraphPorts, int index, boolean required, boolean keepEdge) {
		super(parentSubgraphPorts, index, required, keepEdge);
	}

	@Override
	public boolean isConnected() {
		if (getGraph().getRuntimeJobType().isSubJob()) {
			return getGraph().getAuthorityProxy().getParentGraphSourceEdge(index) != null;
		} else {
			return isRequired() ? true : getGraph().getSubgraphInputComponent().getInputPort(index) != null;
		}
	}
	
}
