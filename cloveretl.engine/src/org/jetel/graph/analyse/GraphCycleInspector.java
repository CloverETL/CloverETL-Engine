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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Edge;

/**
 * This is graph cycle detector. Inspects a graph provided by a {@link GraphProvider} and
 * all cycles are interrupted by BUFFERED edge.
 * 
 * In general, this is a core class for graph analysis and detection, where the buffered
 * edges needs to be used to avoid deadlocks.
 * 
 * Each DIRECT edge is considered as double-way relationship between two linked components.
 * BUFFERED and PHASE edges are considered as only one-way relationship oriented from reader
 * to writer. Each cycle of relationships is interrupted by a BUFFERED edge.
 *  
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19.12.2012
 */
public class GraphCycleInspector {

	private GraphProvider graphProvider;

	private Set<InspectedComponent> visitedComponents = new HashSet<InspectedComponent>();
	
	public GraphCycleInspector(GraphProvider graphProvider) {
		this.graphProvider = graphProvider;
	}
	
	/**
	 * Inspects the graph provided by {@link GraphProvider} passed in constructor.
	 * Some of DIRECT edges are switched to BUFFERED - that is only result.
	 * In case an oriented cycle is detected, {@link JetelRuntimeException} is thrown.
	 */
	public void inspectGraph() {
		InspectedComponent component = null;
		//iterate over all components in the graph and inspect them, whether the component is part of an cycle of relationships
		while ((component = graphProvider.getNextComponent()) != null) {
			//already visited components does need to be tested again
			if (!visitedComponents.contains(component)) {
				inspectComponent(component);
			}
		}
	}

	private void inspectComponent(InspectedComponent component) {
		//stack for recursion algorithm
		Stack<InspectedComponent> componentsStack = new Stack<InspectedComponent>();
		
		componentsStack.push(component);
		visitedComponents.add(component);
		while (!componentsStack.isEmpty()) {
			//what is current component?
			InspectedComponent topComponent = componentsStack.peek();
			//what is following component?
			InspectedComponent nextComponent = topComponent.getNextComponent();
			if (nextComponent != null) {
				visitedComponents.add(component);
				//following component found
				if (componentsStack.contains(nextComponent)) {
					//cycle found
					componentsStack.push(nextComponent);
					cycleFound(componentsStack);
				} else {
					//recursion can go deeper 
					componentsStack.push(nextComponent);
				}
			} else {
				//no other follower, let's step back in recursion
				componentsStack.pop();
			}
		}
	}

	/**
	 * Cycle in the graph found - first regularly oriented edge is marked as BUFFERED. 
	 */
	private void cycleFound(Stack<InspectedComponent> visitedComponents) {
		List<InspectedComponent> theCycle = new ArrayList<InspectedComponent>();
		InspectedComponent endOfCycle = visitedComponents.pop();
		InspectedComponent component = endOfCycle;
		theCycle.add(component);
		boolean bufferedEdgeFound = false;
		boolean reverseEdgeFound = false;
		//Go back in the cycle and find first regularly oriented edge, which can be changed to BUFFERED.
		//The cycle is interrupted and recursive cycle detection can continue.
		//Moreover the founded cycle is checked, whether is uniformly oriented - exception is thrown.
		do {
			Edge entryEdge = component.getEntryEdge();
			if (entryEdge.getReader() == component.getComponent()) {
				if (!bufferedEdgeFound) {
					setEdgeAsBuffered(entryEdge);
					bufferedEdgeFound = true;
					break;
				}
			} else {
				reverseEdgeFound = true;
			}
			if (!bufferedEdgeFound) {
				component = visitedComponents.pop(); //step back in recursion
			} else {
				component = visitedComponents.peek(); //recursion stack is not changed, just check whether the cycle is not uniformly oriented 
			}
			theCycle.add(component);
		} while (!component.equals(endOfCycle) && (!bufferedEdgeFound || !reverseEdgeFound));
		
		if (!bufferedEdgeFound) {
			throw new JetelRuntimeException("Oriented cycle found in the graph. " + theCycle);
		}
	}

	/**
	 * @return
	 */
	private void setEdgeAsBuffered(Edge edge) {
		edge.setEdgeType(EdgeTypeEnum.BUFFERED);
	}

}
