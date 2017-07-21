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

import java.util.Iterator;

import org.jetel.enums.EdgeTypeEnum;
import org.jetel.graph.Edge;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;

/**
 * Abstract implementation of {@link InspectedComponent}.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.12.2012
 */
public abstract class AbstractInspectedComponent implements InspectedComponent {

	/** Wrapped component */
	protected Node component;
	
	/** Entry edge - edge which is associated with this component handler. */
	protected Edge entryEdge;

	/** Internal iterator over all input ports - necessary for {@link #getNextComponent()} */
	private Iterator<InputPort> inputPorts;
	
	/** Internal iterator over all output ports - necessary for {@link #getNextComponent()} */
	private Iterator<OutputPort> outputPorts;
	
	public AbstractInspectedComponent(Node component, Edge entryEdge) {
		this.component = component;
		this.entryEdge = entryEdge;
		inputPorts = component.getInPorts().iterator();
		outputPorts = component.getOutPorts().iterator();
	}
	
	/**
	 * Returns sequence of components connected with this component.
	 * Components behind BUFFERED and PHASE edges are omitted.
	 * The entryEndge is ignored as well. 
	 */
	@Override
	public InspectedComponent getNextComponent() {
		InspectedComponent result;
		
		while (outputPorts.hasNext()) {
			OutputPort outputPort = outputPorts.next();
			if ((result = getNextComponent(outputPort)) != null) {
				return result;
			}
		}
		
		while (inputPorts.hasNext()) {
			InputPort inputPort = inputPorts.next();
			if ((result = getNextComponent(inputPort)) != null) {
				return result;
			}
		}
		
		return null;
	}
	
	/**
	 * @return linked component for the given input port 
	 */
	protected abstract InspectedComponent getNextComponent(InputPort inputPort);

	/**
	 * @return linked component for the given output port 
	 */
	protected abstract InspectedComponent getNextComponent(OutputPort outputPort);

	protected boolean isInputEdgeAllowed(Edge edge) {
		return (entryEdge != edge);
	}

	protected boolean isOutputEdgeAllowed(Edge edge) {
		if (entryEdge != edge
				&& edge.getEdgeType() != EdgeTypeEnum.BUFFERED
				&& edge.getEdgeType() != EdgeTypeEnum.PHASE_CONNECTION) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Node getComponent() {
		return component;
	}

	@Override
	public Edge getEntryEdge() {
		return entryEdge;
	}
	
	@Override
	public boolean equals(Object otherObj) {
		if (otherObj == null || !(otherObj.getClass().equals(this.getClass()))) {
			return false;
		}
		InspectedComponent otherInspectedComponent = (InspectedComponent) otherObj;
		return component == otherInspectedComponent.getComponent();
	}
	
	@Override
	public int hashCode() {
		return component.hashCodeIdentity();
	}
	
	@Override
	public String toString() {
		return component.toString();
	}
	
}
