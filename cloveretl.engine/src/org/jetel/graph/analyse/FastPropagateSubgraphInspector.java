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

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Node;
import org.jetel.graph.SubgraphComponent;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.GraphUtils;
import org.jetel.util.SubgraphUtils;

/**
 * This class is used to inspect subgraphs and makes all edges interconnecting
 * SubgraphInput and SubgraphOutput components with fast propagating variant of edge types.
 * 
 * This algorithm is executed if the usage of a subgraph is located in a loop
 * (see Loop component).
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9. 10. 2014
 */
public class FastPropagateSubgraphInspector {

	/**
	 * Finds all components between SGI and SGO and all attached edges are converted
	 * to fast-propagate type.
	 * 
	 * @param transformationGraph
	 */
	public static void inspectEdges(TransformationGraph transformationGraph) {
		//initialise graph provider, which allows to go through the graph and perform searching algorithm  
		SingleGraphProvider graph = new SingleGraphProvider(transformationGraph);
		graph.setEdgeFunction(new EdgeFunctionImpl());
		graph.setSubgraphInputOutputAsSingleComponent(true);
		graph.setSubgraphInputOutputVisibility(true);
		
		//find SGI and SGO components first
		InspectedComponent component;
		InspectedComponent subgraphInputComponent = null;
		InspectedComponent subgraphOutputComponent = null;
		while ((component = graph.getNextComponent()) != null) {
			if (SubgraphUtils.isSubJobInputComponent(component.getComponent().getType())) {
				subgraphInputComponent = component;
			}
			if (SubgraphUtils.isSubJobOutputComponent(component.getComponent().getType())) {
				subgraphOutputComponent = component;
			}
		}
		
		//find all edges between SGI and SGO
		inspectEdges(subgraphInputComponent, subgraphOutputComponent);
	}
	
	/**
	 * Finds all components on the way from SubgraphInput to SubgraphOutput
	 * @param loopComponent
	 */
	private static void inspectEdges(InspectedComponent subgraphInputComponent, InspectedComponent subgraphOutputComponent) {
		Set<InspectedComponent> componentsOnTheWay = new HashSet<>();
		componentsOnTheWay.add(subgraphInputComponent);
		componentsOnTheWay.add(subgraphOutputComponent);
		
		Stack<InspectedComponent> path = new Stack<>();
		
		InspectedComponent pointer = subgraphInputComponent;
		path.push(pointer);
		while (pointer != null) {
			InspectedComponent follower = pointer.getNextComponent();
			if (follower != null) {
				if (componentsOnTheWay.contains(follower)) {
					componentsOnTheWay.addAll(path);
				} else if (!path.contains(follower)) {
					path.push(follower);
					pointer = follower;
				}
			} else {
				path.pop();
				pointer = !path.isEmpty() ? path.peek() : null;
			}
		}
		
		//converts list of inspected components to list of engine components
		Set<Node> engineComponents = new HashSet<>();
		for (InspectedComponent component : componentsOnTheWay) {
			Node engineComponent = component.getComponent();
			//if one of inspected components is a Subgraph, the subgraph has to be executed in fast-propagate mode
			if (SubgraphUtils.isSubJobComponent(engineComponent.getType())) {
				//the component can be also instance of MockupComponent, which is used for cluster graph analyse 
				if (engineComponent instanceof SubgraphComponent) {
					((SubgraphComponent) engineComponent).setFastPropagateExecution(true);
				}
			}
			engineComponents.add(engineComponent);
		}

		//convert all edges to fast-propagate type
		try {
			GraphUtils.makeEdgesFastPropagate(engineComponents);
		} catch (Exception e) {
			throw new JetelRuntimeException("The subgraph can not be used in loops. Phase edge detected inside the subgraph.", e);
		}
	}

}
