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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.graph.runtime.NodeTrackingDetail;
import org.jetel.graph.runtime.TrackingDetail;

/**
 * A class that represents processing Phase of Transformation Graph
 *
 * @author      D.Pavlis
 * @since       July 23, 2003
 * @revision    $Revision$
 * @see         OtherClasses
 */

public class Phase implements Comparable {

	// Attributes

	// Associations
	/**
	 * @since    April 2, 2002
	 */
	private List <Node> nodesInPhase;
	private List <Edge> edgesInPhase;
    private List <Node> leafNodes;

	// specifies the order of this phase within graph
	private int phaseNum;

	private int phaseExecTime;
	private int phaseMemUtilization;
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
		this.phaseNum = phaseNum;
		nodesInPhase = new LinkedList<Node> ();
		edgesInPhase = new LinkedList<Edge> ();
        result=Result.N_A;
	}


	/**
	 *  Sets the Graph attribute of the Phase object
	 *
	 * @param  graph  The new Graph value
	 * @since         April 5, 2002
	 */
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
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
	public boolean init() {
		Node node = null;
		Edge edge;
		Iterator nodeIterator = nodesInPhase.iterator();
		Iterator edgeIterator = edgesInPhase.iterator();
		phaseExecTime = phaseMemUtilization = 0;

        // list of leaf nodes -will be filled later
        leafNodes = new LinkedList<Node>();
        
		// if the output stream is specified, create logging possibility information
		logger.info("[Clover] Initializing phase: "
			+ phaseNum);

		// iterate through all edges and initialize them
		logger.debug(" initializing edges: ");
		while (edgeIterator.hasNext()) {
			try {
				edge = (Edge) edgeIterator.next();
				edge.init();
			} catch (ComponentNotReadyException ex) {
				logger.error(ex);
				return false;
			}
		}
		// if logger exists, print some out information
		logger.debug(" all edges initialized successfully... ");

		// iterate through all nodes and initialize them
		logger.debug(" initializing nodes: ");
		while (nodeIterator.hasNext()) {
			try {
				node = (Node) nodeIterator.next();
                // is it a leaf node ?
                if (node.isLeaf() || node.isPhaseLeaf()) {
                    leafNodes.add(node);
                }
				// if logger exists, print some out information
				node.init();
				logger.debug("\t" + node.getId() + " ...OK");
			} catch (ComponentNotReadyException ex) {
				logger.error("\t" + node.getId() + " ...FAILED !", ex);
				return false;
			} catch (Exception ex) {
				logger.error("\t" + node.getId() + " ...FATAL ERROR !", ex);
				return false;
			}
		}
        
		logger.info("[Clover] phase: "
			+ phaseNum 
			+ " initialized successfully.");
		
        result = Result.READY;
		return true;
		// initialized OK
	}


	/**
	 *  Description of the Method
	 *
	 * @return    Description of the Return Value
	 */
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        //check nodes configuration
        for(Node node : getNodes()) {
            node.checkConfig(status);
        }
        
        //check edges configuration
//        for(Edge edge : getEdgesInPhase()) {
//            edge.checkConfig(status);
//        }
        
        return status;
	}


	/**
	 * An operation that registers Node within current Phase.
     * 
	 *
	 * @param  node  The feature to be added to the Node attribute
	 * @since        April 2, 2002
	 */
	protected void assignNode(Node node) {
		nodesInPhase.add(node);
        node.setPhase(this);
	}


	/**
	 *  Assigns Edge to belong (also) to this phase.<br> Needed
     *  for cross-phase edges mostly
	 *
	 * @param  edge  The feature to be added to the Edge attribute
	 */
	@Deprecated protected void assignEdge(Edge edge) {
        edgesInPhase.add(edge);
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
	public void addNode(Node node) throws GraphConfigurationException{
	    assignNode(node);
        graph.addNode(node,this);
	}


	/**
	 *  Adds edge to the graph (through this Phase).<br>
     *  Edge is registered globally within Graph 
	 *
	 * @param  edge  The feature to be added to the Edge attribute
     * @throws GraphConfigurationException in case node with the same ID has already
     * been registered withing graph
	 */
	public void addEdge(Edge edge) throws GraphConfigurationException {
		graph.addEdge(edge);
	}


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
        for(Node node : nodesInPhase) {
            node.free();
        }
        
        //free all edges in this phase
        for(Edge edge : edgesInPhase) {
            edge.free();
        }

        nodesInPhase.clear();
        edgesInPhase.clear();
    }

	/**
	 * Returns reference to Nodes contained in this phase.
	 *
	 * @return    The nodes value
	 * @since     July 29, 2002
	 */
	public List<Node> getNodes() {
		return nodesInPhase;
	}


	/**
	 *  Gets the edges attribute of the Phase object
	 *
	 * @return    The edges value
	 */
	@Deprecated public List<Edge> getEdges() {
		return edgesInPhase;
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
	 *  Sets the phaseExecTime attribute of the Phase object
	 *
	 * @param  time  The new phaseExecTime value
	 */
	public void setPhaseExecTime(int time) {
		phaseExecTime = time;
	}


	/**
	 *  Sets the phaseMemUtilization attribute of the Phase object
	 *
	 * @param  mem  The new phaseMemUtilization value
	 */
	public void setPhaseMemUtilization(int mem) {
		phaseMemUtilization = mem;
	}


	/**
	 *  Gets the phaseExecTime attribute of the Phase object
	 *
	 * @return    The phaseExecTime value
	 */
	public int getPhaseExecTime() {
		return phaseExecTime;
	}


	/**
	 *  Gets the phaseMemUtilization attribute of the Phase object
	 *
	 * @return    The phaseMemUtilization value
	 */
	public int getPhaseMemUtilization() {
		return phaseMemUtilization;
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


    public List<Edge> getEdgesInPhase() {
        return edgesInPhase;
    }


    public void setEdgesInPhase(List<Edge> edgesInPhase) {
        this.edgesInPhase = edgesInPhase;
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
}
/*
 *  end class Phase
 */

