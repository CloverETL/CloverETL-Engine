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

import org.jetel.graph.Node;
import org.jetel.util.ClusterUtils;
import org.jetel.util.SubgraphUtils;


/**
 * Common ancestor for both implementation of {@link GraphProvider} interface.
 * This abstract implementation is empty for now, can be used later.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.12.2012
 */
public abstract class AbstractGraphProvider implements GraphProvider {

	/**
	 * Edge function defines how to traverse from a component to an other component.
	 * Some edges can be avoided from algorithm or edges can be used in opposite direction.
	 */
	private EdgeFunction edgeFunction;
	
	/**
	 * This flag indicates the SubgraphInput (or SubgraphOutput) component will be considered
	 * as single component. Otherwise, each port of SGI or SGO component represents separate
	 * 'component' entity for this graph provider.
	 */
	private boolean subgraphInputOutputAsSingleComponent = false;
	
	/**
	 * This flag indicates whether the SubgraphInput and SubgraphOutput components are
	 * part of provided graph.
	 */
	private boolean subgraphInputOutputVisibility = false;
	
	/**
	 * RemoteEdgeComponent and SubJobInput/Output components are not processed in regular way.
	 * @return true for the component which are dedicated to be root of graph processing
	 */
	protected boolean isAllowedComponent(Node component) {
		return !ClusterUtils.isRemoteEdgeComponent(component.getType())
				&& (isSubgraphInputOutputVisibility() || !SubgraphUtils.isSubJobInputOutputComponent(component.getType()));
	}
	
	@Override
	public void setEdgeFunction(EdgeFunction edgeFunction) {
		this.edgeFunction = edgeFunction;
	}
	
	@Override
	public EdgeFunction getEdgeFunction() {
		return edgeFunction;
	}
	
	@Override
	public void setSubgraphInputOutputAsSingleComponent(boolean subgraphInputOutputAsSingleComponent) {
		this.subgraphInputOutputAsSingleComponent = subgraphInputOutputAsSingleComponent;
	}
	
	@Override
	public boolean isSubgraphInputOutputAsSingleComponent() {
		return subgraphInputOutputAsSingleComponent;
	}
	
	@Override
	public void setSubgraphInputOutputVisibility(boolean subgraphInputOutputVisibility) {
		this.subgraphInputOutputVisibility = subgraphInputOutputVisibility;
	}
	
	@Override
	public boolean isSubgraphInputOutputVisibility() {
		return subgraphInputOutputVisibility;
	}
	
}
