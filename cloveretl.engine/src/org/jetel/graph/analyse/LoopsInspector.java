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
 * This class is used to inspect graphs with Loop components and
 * all edges on the loop are converted to fast-propagate type.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9. 10. 2014
 */
public class LoopsInspector {

	public static final String LOOP_COMPONENT_TYPE = "LOOP";

	/**
	 * Finds all Loop component and all edges on the loops are converted
	 * to fast-propagate type.
	 * 
	 * @param transformationGraph
	 */
	public static void inspectEdgesInLoops(TransformationGraph transformationGraph) {
		//initialise graph provider, which allows to go through the graph and perform searching algorithm  
		SingleGraphProvider graph = new SingleGraphProvider(transformationGraph);
		graph.setEdgeFunction(new EdgeFunctionImpl());
		
		InspectedComponent component;
		while ((component = graph.getNextComponent()) != null) {
			if (LOOP_COMPONENT_TYPE.equals(component.getComponent().getType())) {
				//Loop component is found - inspect the loop
				inspectEdgesInLoop(component);
			}
		}
	}
	
	/**
	 * Finds all components on the loop and convert all edges to fast-propagate.
	 * @param loopComponent
	 */
	private static void inspectEdgesInLoop(InspectedComponent loopComponent) {
		Set<InspectedComponent> componentsInLoop = new HashSet<>();
		componentsInLoop.add(loopComponent);
		
		Stack<InspectedComponent> path = new Stack<>();
		
		InspectedComponent pointer = loopComponent.getOutputPortComponent(1);
		if (pointer != null) {
			path.push(pointer);
			while (pointer != null) {
				InspectedComponent follower = pointer.getNextComponent();
				if (follower != null) {
					if (componentsInLoop.contains(follower)) {
						componentsInLoop.addAll(path);
					} else if (!path.contains(follower)) {
						path.push(follower);
						pointer = follower;
					}
				} else {
					path.pop();
					pointer = !path.isEmpty() ? path.peek() : null;
				}
			}
		}

		//converts list of inspected components to list of engine components
		Set<Node> engineComponents = new HashSet<>();
		for (InspectedComponent component : componentsInLoop) {
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
			throw new JetelRuntimeException("Phase edge detected inside the loop.", e);
		}
	}

}
