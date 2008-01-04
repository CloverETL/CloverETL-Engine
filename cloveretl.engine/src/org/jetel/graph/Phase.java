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
package org.jetel.graph;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.graph.runtime.PhaseTrackingDetail;
import org.jetel.graph.runtime.TrackingDetail;

/**
 * A class that represents processing Phase of Transformation Graph
 *
 * @author      D.Pavlis
 * @since       July 23, 2003
 * @revision    $Revision$
 * @see         OtherClasses
 */

public class Phase extends GraphElement implements Comparable {

	// Attributes

	// Associations
	/**
	 * @since    April 2, 2002
	 */
	private Map<String, Node> nodes;
	private Map<String, Edge> edges; //edges with the source component in this phase
    private List <Node> leafNodes;

	// specifies the order of this phase within graph
	private int phaseNum;

	private PhaseTrackingDetail phaseTracking;
    
	private Result result;
    private boolean isCheckPoint;
    private Map<String,TrackingDetail> tracking;

	protected TransformationGraph graph;

	static Log logger = LogFactory.getLog(Phase.class);

	// Operations

	/**
	 *Constructor for the TransformationGraph object
	 *
	 * @param  phaseNum  Description of the Parameter
	 * @since            April 2, 2002
	 */
	public Phase(int phaseNum) {
		super(Integer.toString(phaseNum));
		
		this.phaseNum = phaseNum;
		nodes = new LinkedHashMap<String, Node>();
		edges = new LinkedHashMap<String, Edge>();
        result=Result.N_A;
	}


	/**
	 *  Sets the Graph attribute of the Phase object
	 *
	 * @param  graph  The new Graph value
	 * @since         April 5, 2002
	 */
	public void setGraph(TransformationGraph graph) {
		super.setGraph(graph);
		
		//sets related graph to all included nodes
		for(Node node : nodes.values()) {
			node.setGraph(graph);
		}
		
		//sets related graph to all included nodes
		for(Edge edge : edges.values()) {
			edge.setGraph(graph);
		}
	}

	/**
	 *  Gets the phaseNum attribute of the Phase object
	 *
	 * @return    The phaseNum value
	 */
	public int getPhaseNum() {
		return phaseNum;
	}


	/**
	 * An operation that starts execution of graph
	 *
	 * @param  out  OutputStream - if defined, info messages are printed there
	 * @return      True if all nodes successfully started, otherwise False
	 * @since       April 2, 2002
	 */

	/**
	 *  Description of the Method
	 *
	 * @param  out  OutputStream - if defined, info messages are printed thereDescription of Parameter
	 * @return      returns TRUE if succeeded or FALSE if some Node or Edge failed initialization
	 * @since       April 10, 2002
	 */
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

        // list of leaf nodes -will be filled later
        leafNodes = new LinkedList<Node>();
        
		// if the output stream is specified, create logging possibility information
		logger.info("[Clover] Initializing phase: "	+ phaseNum);

		// iterate through all nodes and initialize them
		logger.debug(" initializing nodes: ");
		for(Node node : nodes.values()) {
			try {
                // is it a leaf node ?
                if (node.isLeaf() || node.isPhaseLeaf()) {
                    leafNodes.add(node);
                }
				// if logger exists, print some out information
				node.init();
				logger.debug("\t" + node.getId() + " ...OK");
			} catch (ComponentNotReadyException ex) {
				throw new ComponentNotReadyException(node.getId() + " ...FAILED !", ex);
			} catch (Exception ex) {
				throw new ComponentNotReadyException(node.getId() + " ...FATAL ERROR !", ex);
			}
		}
        
        //initialization of all edges
		logger.debug(" initializing edges: ");
        for(Edge edge : edges.values()) {
        	edge.init();
        }
		logger.debug(" all edges initialized successfully... ");

		logger.info("[Clover] phase: " + phaseNum + " initialized successfully.");
		
        result = Result.READY;
		// initialized OK
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		
		//phase reset
        result=Result.N_A;
        setPhaseTracking(null);
        
		//reset all components
		for(Node node : nodes.values()) {
			node.reset();
		}
		
		//reset all edges
		for(Edge edge : edges.values()) {
			edge.reset();
		}

	}
	
	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        //check nodes configuration
        for(Node node : nodes.values()) {
            node.checkConfig(status);
        }

        //check edges configuration
        for(Edge edge : edges.values()) {
            edge.checkConfig(status);
        }

        return status;
	}

	/**
	 *  Adds node to the graph (through this Phase).<br>
     *  Node is registered to belong to this phase and also
     *  is globally registered within graph
	 *
	 * @param  node  The feature to be added to the Node attribute
     * @throws GraphConfigurationException in case node with the same ID has already
     * been registered withing graph
	 */
	public void addNode(Node node) throws GraphConfigurationException {
		nodes.put(node.getId(), node);
        node.setPhase(this);
        node.setGraph(getGraph());
	}

    /**
     *  Deletes node from the phase.
     * @param node the node to be removed from the phase
     */
    public void deleteNode(Node node) {
    	Node removedNode = nodes.remove(node.getId());
        if(removedNode != null) {
        	removedNode.setPhase(null);
        	removedNode.setGraph(null);
        }
    }

	public void addEdge(Edge edge) throws GraphConfigurationException{
		Node writer = edge.getWriter();
		if(writer == null) {
			throw new GraphConfigurationException("Edge cannot be added into the phase without source component.");
		}
		if(writer.getPhase() != this) {
			throw new GraphConfigurationException("Edge cannot be added to this phase.");
		}

		edges.put(edge.getId(), edge);
		edge.setGraph(getGraph());
	}

    /**
     *  Deletes edge from the phase.
     * @param edge the edge to be removed from the edge
     */
    public void deleteEdge(Edge edge) {
    	Edge removedEdge = edges.remove(edge.getId());
    	
    	if(removedEdge != null) {
    		removedEdge.setGraph(null);
    	}
    }

//	/**
//	 *  Adds edge to the graph (through this Phase).<br>
//     *  Edge is registered globally within Graph 
//	 *
//	 * @param  edge  The feature to be added to the Edge attribute
//     * @throws GraphConfigurationException in case node with the same ID has already
//     * been registered withing graph
//	 */
//	public void addEdge(Edge edge) throws GraphConfigurationException {
//        assignEdge(edge);
//		graph.addEdge(edge);
//	}

	/**
	 *  Removes all Nodes from Phase
	 *
	 * @since    April 2, 2002
     * @deprecated please use free method instead
	 */
    @Deprecated
	public void destroy() {
        free();
	}

    /**
     *  Removes all Nodes from Phase
     *
     * @since    April 2, 2002
     */
    public void free() {
        
        //free all nodes in this phase
        for(Node node : nodes.values()) {
            node.free();
        }
        
        //free all edges in this phase
        for(Edge edge : edges.values()) {
        	edge.free();
        }

    }
    
	/**
	 * Returns reference to Nodes contained in this phase.
	 *
	 * @return    The nodes value
	 * @since     July 29, 2002
	 */
	public Map<String, Node> getNodes() {
		return Collections.unmodifiableMap(nodes);
	}


	/**
	 *  Gets the edges attribute of the Phase object
	 *
	 * @return    The edges value
	 */
	public Map<String, Edge> getEdges() {
		return Collections.unmodifiableMap(edges);
	}


	/**
	 *  Description of the Method
	 *
	 * @param  to  Description of the Parameter
	 * @return     Description of the Return Value
	 */
	public int compareTo(Object to) {
		int toPhaseNum = ((Phase) to).getPhaseNum();
		if (phaseNum > toPhaseNum) {
			return 1;
		} else if (phaseNum < toPhaseNum) {
			return -1;
		} else {
			return 0;
		}
	}
    
    @Override public int hashCode(){
        return phaseNum;
    }

    @Override public boolean equals(Object obj){
        if (obj instanceof Phase){
            return ((Phase)obj).getPhaseNum()==phaseNum;
        }else{
            return false;
        }
    }
    

	/**
	 *  Gets the phase execution time in milliseconds
	 *
	 * @return    The phaseExecTime value
	 */
	@Deprecated public int getPhaseExecTime() {
		return phaseTracking.getExecTime();
	}


	/**
	 *  Gets the phase memory utilization in KB (kilobytes)
	 *
	 * @return    The phaseMemUtilization value
	 */
	@Deprecated public int getPhaseMemUtilization() {
		return phaseTracking.getMemUtilizationKB();
	}


    public Result getResult() {
        return result;
    }


    public void setResult(Result result) {
        this.result = result;
    }


    public boolean isCheckPoint() {
        return isCheckPoint;
    }


    public void setCheckPoint(boolean isCheckPoint) {
        this.isCheckPoint = isCheckPoint;
    }

    /**
     * @return the leafNodes
     * @since 10.1.2007
     */
    public List<Node> getLeafNodes() {
        return leafNodes;
    }


    /**
     * @return the tracking
     * @since 10.1.2007
     */
    public Map<String, TrackingDetail> getTracking() {
        return tracking;
    }


    /**
     * @param tracking the tracking to set
     * @since 10.1.2007
     */
    public void setTracking(Map<String, TrackingDetail> tracking) {
        this.tracking = tracking;
    }


    /**
     * @return the phaseTracking
     * @since 26.2.2007
     */
    public PhaseTrackingDetail getPhaseTracking() {
        return phaseTracking;
    }


    /**
     * @param phaseTracking the phaseTracking to set
     * @since 26.2.2007
     */
    public void setPhaseTracking(PhaseTrackingDetail phaseTracking) {
        this.phaseTracking = phaseTracking;
    }
}
/*
 *  end class Phase
 */

