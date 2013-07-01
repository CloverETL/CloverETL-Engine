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
import java.util.List;
import java.util.Stack;

import org.jetel.enums.EdgeTypeEnum;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Edge;
import org.jetel.graph.JobType;
import org.jetel.graph.TransformationGraph;

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

	public static final String LOOP_COMPONENT_TYPE = "LOOP"; //TODO what is better place?
	
	private GraphProvider graphProvider;

	private List<InspectedComponent> visitedComponents = new ArrayList<InspectedComponent>();
	
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
		while (!componentsStack.isEmpty()) {
			//what is current component?
			InspectedComponent topComponent = componentsStack.peek();
			//what is following component?
			InspectedComponent nextComponent = topComponent.getNextComponent();
			if (nextComponent != null) {
				if (!alreadyVisitedFromTheDirection(visitedComponents, nextComponent)) {
					//following component found
					if (componentsStack.contains(nextComponent)) {
						//cycle found
						componentsStack.push(nextComponent);
						cycleFound(componentsStack);
					} else {
						//recursion can go deeper 
						componentsStack.push(nextComponent);
					}
				}
			} else {
				visitedComponents.add(topComponent);
				//no other follower, let's step back in the stack
				componentsStack.pop();
			}
		}
	}

	/**
	 * Checks whether the given component is already reached from the same direction (from the same entry edge).
	 */
	private boolean alreadyVisitedFromTheDirection(List<InspectedComponent> visitedComponents, InspectedComponent nextComponent) {
		for (InspectedComponent component : visitedComponents) {
			if (component.equals(nextComponent) && component.getEntryEdge() == nextComponent.getEntryEdge()) {
				return true;
			}
		}
		return false;
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
		int index = 0;
		//Go back in the cycle and find first regularly oriented edge, which can be changed to BUFFERED.
		//The cycle is interrupted and recursive cycle detection can continue.
		//Moreover the founded cycle is checked, whether is uniformly oriented - exception is thrown in that case
		do {
			Edge entryEdge = component.getEntryEdge();
			if (entryEdge.getReader() == component.getComponent()) {
				if (!bufferedEdgeFound) {
					setEdgeAsBuffered(entryEdge);
					bufferedEdgeFound = true;
					index = visitedComponents.size() - 1;
				}
			} else {
				reverseEdgeFound = true;
			}
			if (!bufferedEdgeFound) {
				component = visitedComponents.pop(); //step back in recursion
			} else {
				component = visitedComponents.get(index--); //recursion stack is not changed, just check whether the cycle is not uniformly oriented 
			}
			theCycle.add(component);
		} while (!component.equals(endOfCycle) && (!bufferedEdgeFound || !reverseEdgeFound));
		
		if (!bufferedEdgeFound || !reverseEdgeFound) {
			//oriented cycle found!
			orientedCycleFound(theCycle);
		}
	}

	private void orientedCycleFound(List<InspectedComponent> theCycle) {
		//if whole cycle is in a jobflow and contains a WhileCycle component, then it is ok, otherwise exception is thrown
		TransformationGraph g = null;
		boolean hasWhileCycle = false;
		for (InspectedComponent c : theCycle) {
			if (c.getComponent().getType().equals(LOOP_COMPONENT_TYPE)) {
				hasWhileCycle = true;
			}
			if (g == null) {
				g = c.getComponent().getGraph();
			} else if (g != c.getComponent().getGraph()) {
				throw new JetelRuntimeException("Oriented cycle found in the graph. " + theCycle);
			}
		}
		if (g.getJobType() != JobType.JOBFLOW) {
			throw new JetelRuntimeException("Oriented cycle found in the graph. Cycles are available only in jobflows. " + theCycle);
		}
		if (!hasWhileCycle) {
			throw new JetelRuntimeException("Oriented cycle without WhileCycle component found in the graph. " + theCycle);
		}
	}
	
	/**
	 * @return
	 */
	private void setEdgeAsBuffered(Edge edge) {
		if (edge.getGraph().getJobType() == JobType.ETL_GRAPH) {
			edge.setEdgeType(EdgeTypeEnum.BUFFERED);
		} else if (edge.getGraph().getJobType() == JobType.JOBFLOW) {
			edge.setEdgeType(EdgeTypeEnum.BUFFERED_FAST_PROPAGATE);
		} else {
			throw new JetelRuntimeException("unexpected job type " + edge.getGraph().getJobType());
		}
	}

}
