/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
// FILE: c:/projects/jetel/org/jetel/graph/TransformationGraph.java

package org.jetel.graph;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import java.util.logging.Logger;
import org.jetel.exception.GraphConfigurationException;
/*
 *  import org.apache.log4j.Logger;
 *  import org.apache.log4j.BasicConfigurator;
 */
/**
 * A class that analyzes relations between Nodes and Edges of the Transformation Graph
 *
 * @author      D.Pavlis
 * @since       April 2, 2002
 * @revision    $Revision$
 * @see         OtherClasses
 */

public class TransformationGraphAnalyzer {

	static Logger logger = Logger.getLogger("org.jetel");

	static PrintStream log = System.out;// default info messages to stdout


	/**
	 *  Returns list (precisely array) of all Nodes. The order of Nodes listed is such that
	 *  any parent Node is guaranteed to be listed befor child Node.
	 *  The circular references between nodes should be detected.
	 *
	 * @param  nodes                            Description of the Parameter
	 * @return                                  Description of the Returned Value
	 * @exception  GraphConfigurationException  Description of the Exception
	 * @since                                   July 29, 2002
	 */
	public static Node[] enumerateNodes(List nodes) throws GraphConfigurationException {
		Set set1 = new HashSet();
		Set set2 = new HashSet();
		Set actualSet;
		Set enumerationOfNodes = new LinkedHashSet(nodes.size());
		Stack nodesStack = new Stack();
		List rootNodes;
		Node node;
		Iterator iterator;

		// initial populating of set1 - with root Nodes only
		iterator = nodes.iterator();
		while (iterator.hasNext()) {
			node = (Node) iterator.next();
			if (node.isRoot()) {
				set1.add(node);
			}
		}

		if (set1.isEmpty()) {
			logger.severe("No root Nodes detected! There must be at least one root node defined." +
					" (Root node is	node with output ports defined only.)");
			throw new GraphConfigurationException("No root node!");
		}

		// we need root nodes to traverse graph
		rootNodes = new LinkedList(set1);

		// DETECTING CIRCULAR REFERENCES IN GRAPH
		iterator = rootNodes.iterator();
		while (iterator.hasNext()) {
			nodesStack.clear();
			nodesStack.push(new AnalyzedNode((Node) iterator.next()));
			if (!inspectCircularReference(nodesStack)) {
				throw new GraphConfigurationException("Circular reference found in graph !");
			}
		}
		// enumerate all nodes

		actualSet = set1;
		// initialize - actualSet is set1 for the very first run
		while (!actualSet.isEmpty()) {
			// add individual nodes from set
			enumerationOfNodes.addAll(actualSet);

			// find successors , switch actualSet
			if (actualSet == set1) {
				findNodesSuccessors(set1, set2);
				actualSet = set2;
			} else {
				findNodesSuccessors(set2, set1);
				actualSet = set1;
			}
		}

		// returning nodes ordered by their appearance in the graph -> not really guratanteed that it
		// works for all configurations, but should be sufficient

		return (Node[]) enumerationOfNodes.toArray(new Node[0]);
	}

	
	/**
	 * Method which analyzes the need of forcing buffered edge in case
	 * when one component feeds through multiple output ports other components
	 * and dead-lock could occure. See inspectMultipleFeeds() method.
	 * 
	 * @param nodes
	 */
	public static void analyzeMultipleFeeds(List nodes){
		Stack nodesStack = new Stack();
		List nodesToAnalyze = new LinkedList();
		Node node;
		Iterator iterator;
		
		// set up initial list of nodes to analyze
		// ontly those with 2 or more input ports need inspection
		iterator = nodes.iterator();
		while (iterator.hasNext()) {
			node = (Node) iterator.next();
			if (node.getInPorts().size()>1 ) {
				nodesToAnalyze.add(node);
			}
		}
		//	DETECTING buffering needs
		iterator = nodesToAnalyze.iterator();
		while (iterator.hasNext()) {
			nodesStack.clear();
			nodesStack.push(new AnalyzedNode((Node) iterator.next()));
			inspectMultipleFeeds(nodesStack);
		}
	}
	
	/**
	 *  Tests whether there is no loop/cycle in path from root node to leaf node
	 *  This test must be run for each root note to ensure that the whole graph is free of cycles
	 *  It assumes that the IDs of individual nodes are unique -> it is constraint imposed by design
	 *
	 * @param  nodesStack  Stack with one elemen - root node from which to start analyzing
	 * @return             true if path has no loops, otherwise false
	 */
	private static boolean inspectCircularReference(Stack nodesStack) {
		OutputPort port;
		Node nextNode;
		Set nodesEncountered = new HashSet();
		while (!nodesStack.empty()) {
			port = ((AnalyzedNode) nodesStack.peek()).getNextOutPort();
			if (port == null) {
				// this node has no more ports (offsprings)
				// we have to remove it from already visited nodes
				nodesEncountered.remove(((AnalyzedNode) nodesStack.pop()).getNode().getID());
			} else {
				nextNode = port.getReader();
				//debug ! System.out.println("-"+nextNode.getID());
				if (nextNode != null) {
					// have we seen this node before ? if yes, then it is a loop
					if (!nodesEncountered.add(nextNode.getID())) {
						dumpNodesReferences(nodesStack.iterator(), nextNode);
						return false;
					}
					nodesStack.push(new AnalyzedNode(nextNode));// put this node on top
				}
			}
		}
		return true;
	}

	/**
	 * Method which checks components which concentrate more than one input for potential deadlocks.<br>
	 * If, for example, join component merges data from two flows which both originate at the same 
	 * node (e.g. data reader) then deadlock situation can occure when the join waits for data reader to send next
	 * record on one port and the reader waits for join to consume record on the other port.<br>
	 * If such situation is found, all input ports (Edges) of join has to be buffered. 
	 * 
	 * @param nodesStack
	 * @return
	 */
	private static void inspectMultipleFeeds(Stack nodesStack) {
		InputPort port;
		Node prevNode;
		Set nodesEncountered = new HashSet();
		Node startNode=((AnalyzedNode) nodesStack.peek()).getNode();
		while (!nodesStack.empty()) {
			port = ((AnalyzedNode) nodesStack.peek()).getNextInPort();
			if (port == null) {
				// this node has no more input ports
				// we have to remove it from already visited nodes as the is the end of road.
				nodesStack.pop();
			} else {
				prevNode = port.getWriter();
				if (prevNode != null) {
					// have we seen this node before ? if yes, then we need to buffer start node (its
					// input ports
					if (!nodesEncountered.add(prevNode.getID())) {
						for (int i=0;i<startNode.getInPorts().size();i++){
							//TODO: potential problem if port is not backed by EDGE - this should not happen
							Object edge=startNode.getInputPort(i);
							assert edge instanceof Edge : "Port not backed by Edge object !";
							((Edge)edge).setType(Edge.EDGE_TYPE_BUFFERED);
							// DEBUG
							System.out.println(((Edge)edge).getID()+" edge should be set to TYPE_BUFFERED.");
							//logger.info(((Edge)edge).getID()+" edge has been set to TYPE_BUFFERED.");
						}
					}
					nodesStack.push(new AnalyzedNode(prevNode));// put this node on top
				}
			}
		}
	}


	/**
	 *  Finds all the successors of Nodes from source Set
	 *
	 * @param  source                           Set of source Nodes
	 * @param  destination                      Set of all immediate successors of Nodes from <source> set
	 * @exception  GraphConfigurationException  Description of the Exception
	 * @since                                   April 18, 2002
	 */
	protected static void findNodesSuccessors(Set source, Set destination) throws GraphConfigurationException {
		Iterator nodeIterator = source.iterator();
		Iterator portIterator;
		OutputPort outPort;
		Node currentNode;
		Node nextNode;
		// remove all previous items from dest.
		destination.clear();
		// iterate through all source nodes
		while (nodeIterator.hasNext()) {
			currentNode = ((Node) nodeIterator.next());
			portIterator = currentNode.getOutPorts().iterator();
			// iterate through all output ports
			// some other node is perhaps connected to these ports
			while (portIterator.hasNext()) {
				outPort = (OutputPort) portIterator.next();
				// is some Node reading data produced by our source node ?
				nextNode = outPort.getReader();
				if (nextNode != null) {
					if (currentNode.getPhase() > nextNode.getPhase()) {
						logger.severe("Wrong phase order between components: " +
								currentNode.getID() + " phase: " + currentNode.getPhase() + " and " +
								nextNode.getID() + " phase: " + nextNode.getPhase());
						throw new GraphConfigurationException("Wrong phase order !");
					}
					destination.add(nextNode);
				}
			}
		}
	}


	/**
	 *  This is only for reporting problems
	 *
	 * @param  iterator     Description of the Parameter
	 * @param  problemNode  Description of the Parameter
	 */
	protected static void dumpNodesReferences(Iterator iterator, Node problemNode) {
		System.out.println("Dump of references between nodes:");
		System.out.println("Detected loop when encountered node " + problemNode.getID());
		System.out.println("Chain of references:");
		while (iterator.hasNext()) {
			System.out.print(((AnalyzedNode) iterator.next()).getNode().getID());
			System.out.print(" -> ");

		}
		System.out.println(problemNode.getID());
	}


	/**
	 *  This method puts Nodes of the graph into appropriate Phase objects (Edges too).
	 *  Phases are run one by one and when finished, all Nodes&Edges in phase are
	 *  destroyed (memory is freed and resources reclaimed).<br>
	 *  Then next phase is started.
	 *
	 * @param  nodes       Description of the Parameter
	 * @param  edges       Description of the Parameter
	 * @param  phases      Description of the Parameter
	 */
	public static void distributeNodes2Phases(Phase[] phases, Node[] nodes, List edges) {
		Map phaseMap = new HashMap(phases.length);
		Iterator iterator;
		Phase phase;
		Phase phaseTmp;
		Edge edge;
		Integer currentPhase;

		// create map of Phases so we can easily get appropriate phase number
		for (int i = 0; i < phases.length; i++) {
			if (phaseMap.put(new Integer(phases[i].getPhaseNum()), phases[i]) != null) {
				// we have two phases with the same number - wrong !!
				logger.severe("Phase number not unique: " + phases[i].getPhaseNum());
				throw new RuntimeException("Phase number not unique: " + phases[i].getPhaseNum());
			}
		}

		// put nodes into proper phases
		currentPhase = new Integer(0);
		phase = (Phase) phaseMap.get(currentPhase);
		for (int i = 0; i < nodes.length; i++) {
			if (currentPhase.intValue() != nodes[i].getPhase()) {
				currentPhase = new Integer(nodes[i].getPhase());
				phase = (Phase) phaseMap.get(currentPhase);

			}
			if (phase == null) {
				throw new RuntimeException("Phase doesn't exist -" + nodes[i].getPhase());
			}
			phase.assignNode(nodes[i]);
		}
		// analyze edges (whether they need to be buffered and put them into proper phases
		// edges connecting nodes from two different phases has to be put into both phases
		iterator = edges.iterator();
		currentPhase = new Integer(0);
		phase = (Phase) phaseMap.get(currentPhase);
		while (iterator.hasNext()) {
			edge = (Edge) iterator.next();
			int readerPhase = edge.getReader().getPhase();
			if (currentPhase.intValue() != readerPhase) {
				currentPhase = new Integer(readerPhase);
				phase = (Phase) phaseMap.get(currentPhase);
			}
			phase.assignEdge(edge);
			if (readerPhase != edge.getWriter().getPhase()) {
				// edge connecting two nodes belonging to different phases
				// has to be buffered
				edge.setType(Edge.EDGE_TYPE_BUFFERED);
				// because at the end of each phase, edges from that phase
				// are destroyed (their references), we need to preserve
				// references for those, which span two phases
				phaseTmp = (Phase) phaseMap.get(new Integer(edge.getWriter().getPhase()));
				phaseTmp.assignEdge(edge);
			}
		}
	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       12. únor 2004
	 * @revision    $Revision$
	 */
	private static class AnalyzedNode {
		Node node;
		int analyzedOutPort;
		int analyzedInPort;


		/**
		 *Constructor for the AnalyzedNode object
		 *
		 * @param  node  Description of the Parameter
		 */
		AnalyzedNode(Node node) {
			this.node = node;
			analyzedOutPort = 0;
			analyzedInPort = 0;
		}


		/**
		 *  Gets the nextPort attribute of the AnalyzedNode object
		 *
		 * @return    The nextPort value
		 */
		OutputPort getNextOutPort() {
			if (analyzedOutPort >= node.getOutPorts().size()) {
				return null;
			} else {
				return node.getOutputPort(analyzedOutPort++);
			}
		}

		InputPort getNextInPort() {
			if (analyzedInPort >= node.getInPorts().size()) {
				return null;
			} else {
				return node.getInputPort(analyzedInPort++);
			}
		}

		/**
		 *  Gets the node attribute of the AnalyzedNode object
		 *
		 * @return    The node value
		 */
		Node getNode() {
			return node;
		}
	}

}
/*
 *  end class TransformationGraphAnalyzer
 */

