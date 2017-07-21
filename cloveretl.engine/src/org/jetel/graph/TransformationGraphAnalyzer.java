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

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.enums.EnabledEnum;
import org.jetel.exception.GraphConfigurationException;

/*
 *  import org.apache.log4j.Logger;
 *  import org.apache.log4j.BasicConfigurator;
 */
/**
 * A class that analyzes relations between Nodes and Edges of the Transformation Graph
 * 
 * @author D.Pavlis
 * @since April 2, 2002
 * @see OtherClasses
 */

public class TransformationGraphAnalyzer {

	static Log logger = LogFactory.getLog(TransformationGraphAnalyzer.class);

	static PrintStream log = System.out;// default info messages to stdout

	/**
	 * Apply disabled property of node to graph. Called in graph initial phase.
	 * 
	 * @throws GraphConfigurationException
	 */
	public static void disableNodesInPhases(TransformationGraph graph) throws GraphConfigurationException {
		Set<Node> nodesToRemove = new HashSet<Node>();
		Phase[] phases = graph.getPhases();

		for (int i = 0; i < phases.length; i++) {
			nodesToRemove.clear();
			for (Node node : phases[i].getNodes().values()) {
				if (node.getEnabled() == EnabledEnum.DISABLED) {
					nodesToRemove.add(node);
					disconnectAllEdges(node);
				} else if (node.getEnabled() == EnabledEnum.PASS_THROUGH) {
					nodesToRemove.add(node);
					final InputPort inputPort = node.getInputPort(node.getPassThroughInputPort());
					final OutputPort outputPort = node.getOutputPort(node.getPassThroughOutputPort());
					if (inputPort == null || outputPort == null
					// if the component has an output edge which is directly connected into its input port
					// whole component is removed even with the edge
					// this is not normally possible however see issue #4960
					|| inputPort.getEdge() == outputPort.getEdge()) {
						disconnectAllEdges(node);
						continue;
					}
					final Edge inEdge = inputPort.getEdge();
					final Edge outEdge = outputPort.getEdge();
					final Node sourceNode = inEdge.getWriter();
					final Node targetNode = outEdge.getReader();
					final int sourceIdx = inEdge.getOutputPortNumber();
					final int targetIdx = outEdge.getInputPortNumber();
					disconnectAllEdges(node);
					sourceNode.addOutputPort(sourceIdx, inEdge);
					targetNode.addInputPort(targetIdx, inEdge);
					try {
						node.getGraph().addEdge(inEdge);
					} catch (GraphConfigurationException e) {
						logger.error(e);
					}
				}
			}
			for (Node node : nodesToRemove) {
				phases[i].deleteNode(node);
			}
		}
	}

	/**
	 * Disconnect all edges connected to the given node.
	 * 
	 * @param node
	 * @throws GraphConfigurationException
	 */
	private static void disconnectAllEdges(Node node) throws GraphConfigurationException {
		for (Iterator<InputPort> it1 = node.getInPorts().iterator(); it1.hasNext();) {
			final Edge edge = it1.next().getEdge();
			Node writer = edge.getWriter();
			if (writer != null)
				writer.removeOutputPort(edge);
			node.getGraph().deleteEdge(edge);
		}

		for (Iterator<OutputPort> it1 = node.getOutPorts().iterator(); it1.hasNext();) {
			final Edge edge = it1.next().getEdge();
			final Node reader = edge.getReader();
			if (reader != null)
				reader.removeInputPort(edge);
			node.getGraph().deleteEdge(edge);
		}
	}

	/**
	 * @param node
	 * @param reflectedNodes
	 *            reflected set of nodes, typically nodes in phase; the resulted nodes will be only from this set of
	 *            nodes
	 * @return list of all precedent nodes for given node
	 */
	public static List<Node> findPrecedentNodes(Node node, Collection<Node> reflectedNodes) {
		List<Node> result = new ArrayList<Node>();

		for (InputPort inputPort : node.getInPorts()) {
			final Node writer = inputPort.getWriter();
			if (reflectedNodes == null || reflectedNodes.contains(writer)) {
				result.add(writer);
			}
		}

		return result;
	}

	/**
	 * Finds all components which precede given root component. Recursive version of {@link #findPrecedentNodes(Node, Collection)} method.
	 */
	public static List<Node> findPrecedentNodesRecursive(Node rootComponent, Collection<Node> reflectedNodes) {
		List<Node> result = new ArrayList<Node>();
		Queue<Node> toProcess = new LinkedList<Node>();
		
		toProcess.addAll(findPrecedentNodes(rootComponent, reflectedNodes));
		while (!toProcess.isEmpty()) {
			Node component = toProcess.poll();
			if (!result.contains(component) && component != rootComponent) {
				toProcess.addAll(findPrecedentNodes(component, reflectedNodes));
				toProcess.addAll(findFollowingNodes(component, reflectedNodes));
				result.add(component);
			}
		}
		
		return result;
	}
	
	/**
	 * @param node
	 * @param reflectedNodes
	 *            reflected set of nodes, typically nodes in phase; the resulted nodes will be only from this set of
	 *            nodes
	 * @return list of all following nodes for given node
	 */
	public static List<Node> findFollowingNodes(Node node, Collection<Node> reflectedNodes) {
		List<Node> result = new ArrayList<Node>();

		for (OutputPort outputPort : node.getOutPorts()) {
			final Node reader = outputPort.getReader();
			if (reflectedNodes == null || reflectedNodes.contains(reader)) {
				result.add(reader);
			}
		}

		return result;
	}

	/**
	 * Finds all components which follow given root component. Recursive version of {@link #findFollowingNodes(Node, Collection)} method.
	 */
	public static List<Node> findFollowingNodesRecursive(Node rootComponent, Collection<Node> reflectedNodes) {
		List<Node> result = new ArrayList<Node>();
		Queue<Node> toProcess = new LinkedList<Node>();
		
		toProcess.addAll(findFollowingNodes(rootComponent, reflectedNodes));
		while (!toProcess.isEmpty()) {
			Node component = toProcess.poll();
			if (!result.contains(component) && component != rootComponent) {
				toProcess.addAll(findPrecedentNodes(component, reflectedNodes));
				toProcess.addAll(findFollowingNodes(component, reflectedNodes));
				result.add(component);
			}
		}
		
		return result;
	}

	/**
	 * Components topological sorting based on depth-first search. This algorithm is used mainly for user friendly nodes
	 * visualization.
	 * 
	 * @param givenNodes
	 * @return
	 * @note algorithm is described for example at http://en.wikipedia.org/wiki/Topological_sorting
	 */
	public static List<Node> nodesTopologicalSorting(List<Node> givenNodes) {
		List<Node> result = new ArrayList<Node>();
		List<Node> roots = new ArrayList<Node>();

		// find root nodes - nodes without precedent nodes in the given list of nodes
		for (Node givenNode : givenNodes) {
			if (findPrecedentNodes(givenNode, givenNodes).isEmpty()) {
				roots.add(givenNode);
			}
		}

		// topological sorting
		Stack<Node> nodesToProcess = new Stack<Node>();
		nodesToProcess.addAll(roots);
		List<Node> visited = new ArrayList<Node>();
		while (!nodesToProcess.isEmpty()) {
			Node nodeToProcess = nodesToProcess.pop();
			if (!visited.contains(nodeToProcess)) {
				visited.add(nodeToProcess);
				nodesToProcess.addAll(findFollowingNodes(nodeToProcess, givenNodes));
				result.add(nodeToProcess);
			}
		}

		return result;
	}

}
/*
 * end class TransformationGraphAnalyzer
 */

