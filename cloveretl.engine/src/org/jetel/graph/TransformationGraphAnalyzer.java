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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.enums.EdgeTypeEnum;
import org.jetel.enums.EnabledEnum;
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

	static Log logger = LogFactory.getLog(TransformationGraphAnalyzer.class);

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
	public static List<Node> analyzeGraphTopology(List<Node> nodes) throws GraphConfigurationException {
		Set<Node> set1 = new HashSet<Node>();
		Set<Node> set2 = new HashSet<Node>();
		Set<Node> actualSet;
		Set<Node> enumerationOfNodes = new LinkedHashSet<Node>(nodes.size());
		Stack<AnalyzedNode> nodesStack = new Stack<AnalyzedNode>();
		List<Node> rootNodes;
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
			logger.error("No root Nodes detected! There must be at least one root node defined." +
					" (Root node is	node with output ports defined only.)");
			throw new GraphConfigurationException("No root node!");
		}

		// we need root nodes to traverse graph
		rootNodes = new LinkedList<Node>(set1);

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

		return Arrays.asList(enumerationOfNodes.toArray(new Node[enumerationOfNodes.size()]));
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
				nodesEncountered.remove(((AnalyzedNode) nodesStack.pop()).getNode().getId());
			} else {
				nextNode = port.getReader();
				//DEBUG ! System.out.println("-"+nextNode.getID());
				if (nextNode != null) {
					// have we seen this node before ? if yes, then it is a loop
					if (!nodesEncountered.add(nextNode.getId())) {
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
					if (!nodesEncountered.add(prevNode.getId())) {
						for (int i=0;i<startNode.getInPorts().size();i++){
							//TODO: potential problem if port is not backed by EDGE - this should not happen
							Object edge=startNode.getInputPort(i);
							//assert edge instanceof Edge : "Port not backed by Edge object !";
							if(((Edge)edge).getEdgeType() == EdgeTypeEnum.DIRECT || ((Edge)edge).getEdgeType() == EdgeTypeEnum.DIRECT_FAST_PROPAGATE) {
								((Edge)edge).setEdgeType(EdgeTypeEnum.BUFFERED);
								// DEBUG
								//System.out.println(((Edge)edge).getID()+" edge should be set to TYPE_BUFFERED.");
								logger.debug(((Edge)edge).getId()+" edge has been set to TYPE_BUFFERED.");
							}
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
					if (currentNode.getPhase().getPhaseNum() > nextNode.getPhase().getPhaseNum()) {
						logger.error("Wrong phase order between components: " +
								currentNode.getId() + " phase: " + currentNode.getPhase() + " and " +
								nextNode.getId() + " phase: " + nextNode.getPhase());
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
	    logger.debug("Dump of references between nodes:");
	    logger.debug("Detected loop when encountered node " + problemNode.getId());
	    logger.debug("Chain of references:");
		StringBuffer buffer=new StringBuffer(64);
	    while (iterator.hasNext()) {
			buffer.append(((AnalyzedNode) iterator.next()).getNode().getId());
			buffer.append(" -> ");

		}
		buffer.append(problemNode.getId());
        logger.debug(buffer.toString());
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
	 * @throws GraphConfigurationException 
	 */
	public static void analyzeEdges(List edges) throws GraphConfigurationException {
		Phase readerPhase;
		Phase writerPhase;
		Edge edge;

		// analyze edges (whether they need to be buffered and put them into proper phases
		// edges connecting nodes from two different phases has to be put into both phases
		for (Iterator iterator=edges.iterator();iterator.hasNext();) {
			edge = (Edge) iterator.next();
			readerPhase = edge.getReader().getPhase();
            writerPhase = edge.getWriter().getPhase();
            writerPhase.addEdge(edge);
			if (readerPhase != writerPhase ) {
				// edge connecting two nodes belonging to different phases
				// has to be buffered
				edge.setEdgeType(EdgeTypeEnum.PHASE_CONNECTION);
			}
		}
	}


	/**
	 *  Description of the Class
	 *
	 * @author      dpavlis
	 * @since       12. ???nor 2004
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

    /**
     * Apply disabled property of node to graph. Called in graph initial phase. 
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
                    final Edge inEdge = (Edge) node.getInputPort(node
                            .getPassThroughInputPort());
                    final Edge outEdge = (Edge) node.getOutputPort(node
                            .getPassThroughOutputPort());
                    if (inEdge == null || outEdge == null) {
                        disconnectAllEdges(node);
                        continue;
                    }
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
                        logger.error("Unexpected error: " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }
            for(Node node : nodesToRemove) {
            	phases[i].deleteNode(node);
            }
        }
    }
    
    /**
     * Disconnect all edges connected to the given node.
     * @param node
     * @throws GraphConfigurationException 
     */
    private static void disconnectAllEdges(Node node) throws GraphConfigurationException {
        for(Iterator it1 = node.getInPorts().iterator(); it1.hasNext();) {
            final Edge edge = (Edge) it1.next();
            Node writer = edge.getWriter();
            if(writer != null) writer.removeOutputPort(edge);
            node.getGraph().deleteEdge(edge);
        }

        for(Iterator it1 = node.getOutPorts().iterator(); it1.hasNext();) {
            final Edge edge = (Edge) it1.next();
            final Node reader = edge.getReader();
            if(reader != null) reader.removeInputPort(edge);
            node.getGraph().deleteEdge(edge);
        }
    }

}
/*
 *  end class TransformationGraphAnalyzer
 */

