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

import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Edge;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.util.HashCodeUtil;
import org.jetel.util.SubgraphUtils;

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
	
	/**
	 * This flag is used only SubJobInput/Output components in {@link #getNextComponent()} method
	 */
	private boolean nextComponentReturned = false;
	
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
		
		if (!SubgraphUtils.isSubJobInputOutputComponent(getComponent().getType())) {
			//handling of regular components
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
		} else {
			//special handling of SubJobInput and SubJobOutput components
			if (!nextComponentReturned) {
				nextComponentReturned = true;
				if (isEntryEdgeInputEdge()) {
					OutputPort outputPort = component.getOutputPort(entryEdge.getInputPortNumber());
					if (outputPort != null) {
						return getNextComponent(outputPort);
					}
				} else {
					InputPort inputPort = component.getInputPort(entryEdge.getOutputPortNumber());
					if (inputPort != null) {
						return getNextComponent(inputPort);
					}
				}
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
		//never return back to already visited path and do not use buffered edges - phase, buffered and buffered_fast_propagate
		if (entryEdge != edge && !edge.getEdgeType().isBuffered()) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * @return true if the entryEdge is connected to input port of this component
	 */
	private boolean isEntryEdgeInputEdge() {
		if (entryEdge != null) {
			return (entryEdge.getReader() == component);
		} else {
			throw new JetelRuntimeException("entryEdge is null");
		}
	}
	
	/**
	 * @return port index of the entry edge
	 */
	public int getEntryEdgeIndex() {
		if (entryEdge != null) {
			if (isEntryEdgeInputEdge()) {
				return entryEdge.getInputPortNumber();
			} else {
				return entryEdge.getOutputPortNumber();
			}
		} else {
			throw new JetelRuntimeException("entryEdge is null");
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
		AbstractInspectedComponent otherInspectedComponent = (AbstractInspectedComponent) otherObj;
		if (component == otherInspectedComponent.getComponent()) {
			if (SubgraphUtils.isSubJobInputOutputComponent(component.getType())) {
				//port index of entryEdge has to same for SubJobInput/Output components as well
				return getEntryEdgeIndex() == otherInspectedComponent.getEntryEdgeIndex();
			} else {
				return true;
			}
		} else {
			return false;
		}
	}
	
	@Override
	public int hashCode() {
		int hash = component.hashCodeIdentity();
		if (SubgraphUtils.isSubJobInputOutputComponent(component.getType())) {
			hash = HashCodeUtil.hash(hash, getEntryEdgeIndex());
		}
		return hash;
	}
	
	@Override
	public String toString() {
		return component.toString();
	}
	
}
