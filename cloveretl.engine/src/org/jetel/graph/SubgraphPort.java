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
 * This object represents input and output port of a subgraph with relating settings.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2. 12. 2014
 */
public abstract class SubgraphPort {

	protected SubgraphPorts subgraphPorts;
	
	protected int index;
	
	protected boolean required = true;
	
	/**
	 * This flag is used only for optional ports (required == false) and 
	 * indicates whether the edge related with this port should be kept in the subgraph
	 * or should be removed from the subgraph.
	 */
	protected boolean keepEdge = false;
	
	/**
	 * This flag is loaded from sgrf file and indicates whether the optional port should
	 * be considered as connected. It is used only for top-level execution of the subgraph.
	 */
	protected boolean connected;
	
	public SubgraphPort(SubgraphPorts subgraphPorts, int index, boolean required, boolean keepEdge, boolean connected) {
		this.subgraphPorts = subgraphPorts;
		this.index = index;
		this.required = required;
		this.keepEdge = keepEdge;
		this.connected = connected;
	}

	public int getIndex() {
		return index;
	}
	
	public boolean isRequired() {
		return required;
	}
	
	/**
	 * @return for optional ports returns flag which indicates the related edge
	 * to this port is kept or remove
	 */
	public boolean isKeptEdge() {
		return keepEdge;
	}
	
	/**
	 * @return graph of this port 
	 */
	public TransformationGraph getGraph() {
		return subgraphPorts.getGraph();
	}
	
	/**
	 * @return true if the port is connected (optional ports do not need to be connected)
	 */
	public abstract boolean isConnected();

	/**
	 * @return true for input port and false for output port
	 */
	public abstract boolean isInputPort();

	/**
	 * @return true for output port and false for input port
	 */
	public abstract boolean isOutputPort();

}
