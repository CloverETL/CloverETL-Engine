/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
// FILE: c:/projects/jetel/org/jetel/graph/TransformationGraph.java

package org.jetel.graph;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.io.PrintStream;
import org.jetel.data.Defaults;
import org.jetel.exception.GraphConfigurationException;

import java.util.logging.Logger;
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


	//public TransformationGraphAnalyzer() {
	//}


	/*
	 *  public static void analyzeEdges(List edges){
	 *  Iterator iterator=edges.iterator();
	 *  Edge edge;
	 *  while(iterator.hasNext()){
	 *  edge=(Edge)iterator.next();
	 *  if (edge.getReader().getPhase()!=edge.getWriter().getPhase()){
	 *  / edge connecting two nodes belonging to different phases
	 *  / has to be buffered
	 *  edge.setType(Edge.EDGE_TYPE_BUFFERED);
	 *  }
	 *  }
	 *  }
	 */
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
		Set tmpSet = new HashSet(nodes.size());
		Map nodesEncountered=new HashMap(nodes.size());
		Set actualSet;
		Set enumerationOfNodes = new LinkedHashSet(nodes.size());
		int totalNodesEncountered = 0;
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

		int nRootNodes=set1.size();
		// populate hash of all nodes with #of input ports (for detecting loops in graph) multiplied by
		// number of root nodes.
		// the algorithm is simple/stupid. It expects that each node can be visited only
		// so many times as there are edges going to node multiplied by number of root nodes
		// basically, from each root node, there is some path through graph
		// 
		for(Iterator i=nodes.iterator();i.hasNext();){
			node=(Node)i.next();
			nodesEncountered.put(node,new Integer(node.getInPorts().size()*nRootNodes));	
		}
		
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
		/* following piece of code doesn't work well ;-)	
			for(iterator=actualSet.iterator();iterator.hasNext();){
				node=(Node)iterator.next();
				Integer count=(Integer)nodesEncountered.get(node);
				if (count.intValue()<=0){
					logger.severe("Circular reference found in graph ! Suspicious node: "+node.getID());
					dumpNodesReferences(actualSet.iterator());
					throw new GraphConfigurationException("Circular reference found in graph !");
				}else{
					nodesEncountered.put(node,new Integer(count.intValue()-1));
				}
			}*/
			
		}
		// returning nodes ordered by their appearance in the graph -> not really guratanteed that it
		// works for all configurations, but should be sufficient
		
		return (Node[]) enumerationOfNodes.toArray(new Node[0]);
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
	 * @param  iterator  Description of the Parameter
	 */
	protected static void dumpNodesReferences(Iterator iterator) {
		Node node;
		Iterator portIterator;
		InputPort inPort;
		while (iterator.hasNext()) {
			node = (Node) iterator.next();
			log.print("> " + node.getID());
			log.println(" referenced from: ");

			portIterator = node.getInPorts().iterator();
			while (portIterator.hasNext()) {
				inPort = (InputPort) portIterator.next();
				// is some Node reading data produced by our source node ?
				if (inPort.getWriter() != null) {
					log.println(" -- " + inPort.getWriter().getID());
				}
			}
		}
	}


	/**
	 *  This method puts Nodes of the graph into appropriate Phase objects (Edges too).
	 *  Phases are run one by one and when finished, all Nodes&Edges in phase are
	 *  destroyed (memory is freed and resources reclaimed).<br>
	 *  Then next phase is started.
	 *
	 * @param  phasesList  Description of the Parameter
	 * @param  nodes       Description of the Parameter
	 * @param  edges       Description of the Parameter
	 */
	public static void distributeNodes2Phases(Phase[] phases, Node[] nodes, List edges) {
		Map phaseMap = new HashMap(phases.length);
		Iterator iterator;
		Phase phase;
		Phase phaseTmp;
		Edge edge;
		Integer currentPhase;

		// create map of Phases so we can easily get appropriate phase number
		for(int i=0;i<phases.length;i++){
			if (phaseMap.put(new Integer(phases[i].getPhaseNum()), phases[i])!=null){
				// we have two phases with the same number - wrong !!
				logger.severe("Phase number not unique: "+phases[i].getPhaseNum());
				throw new RuntimeException("Phase number not unique: "+phases[i].getPhaseNum());
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
}
/*
 *  end class TransformationGraphAnalyzer
 */

