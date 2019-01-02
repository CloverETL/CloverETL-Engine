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
package org.jetel.graph.runtime;

import java.util.ArrayList;
import java.util.List;

import org.jetel.graph.Node;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraphAnalyzer;
import org.jetel.graph.runtime.jmx.CloverJMX;
import org.jetel.util.ClusterUtils;

/**
 * This class represents tracking information about an phase.
 * 
 * State of an instance is supposed to be changed over time
 * (it is used by WatchDog to gather information during an execution of graph).
 * 
 * @author Filip Reichman
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jan 2, 2019
 */
public class PhaseTrackingDetail {

	protected int phaseNum;
	
	protected long startTime = -1;

	protected long endTime = -1;
    
	protected long memoryUtilization;
    
	protected Result result;
	
	protected String phaseLabel;
	
	private final transient Phase phase;
	private NodeTrackingDetail[] nodesDetails;

	/**
	 * Constructor.
	 * @param phase
	 */
	public PhaseTrackingDetail(Phase phase) {
		this.phase = phase;
		this.phaseNum = phase.getPhaseNum();
		this.result = Result.N_A;
		this.phaseLabel = phase.getLabel();
		
		List<NodeTrackingDetail> details = new ArrayList<>();
		for (Node node : TransformationGraphAnalyzer.nodesTopologicalSorting(new ArrayList<Node>(phase.getNodes().values()))) {
			if (!ClusterUtils.isRemoteEdgeComponent(node.getType())
					&& !ClusterUtils.isClusterRegather(node.getType())) {
				details.add(new NodeTrackingDetail(this, node));
			}
		}
		this.nodesDetails = details.toArray(new NodeTrackingDetail[details.size()]);
	}
	
	Phase getPhase() {
		return phase;
	}
	
	public NodeTrackingDetail[] getNodeTracking() {
		return nodesDetails;
	}
	
	public NodeTrackingDetail getNodeTracking(String nodeID) {
		for (NodeTrackingDetail nodeDetail : nodesDetails) {
			if(nodeID.equals(nodeDetail.getNodeID())) {
				return nodeDetail;
			}
		}
		
		return null;
	}
	
	public int getPhaseNum() {
		return phaseNum;
	}
	
	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public long getMemoryUtilization() {
		return memoryUtilization;
	}

	public Result getResult() {
		return result;
	}
	
	public String getPhaseLabel() {
		return phaseLabel;
	}

	public long getExecutionTime() {
		if (startTime == -1) {
			return -1;
		} else if (endTime == -1) {
			return System.currentTimeMillis() - startTime;
		} else {
			return endTime - startTime;
		}
	}
	
	//******************* SETTERS *******************/
	
	public void setNodesDetails(NodeTrackingDetail[] nodesDetails) {
		this.nodesDetails = nodesDetails;
	}
	
	//******************* EVENTS ********************/
	void phaseStarted() {
		startTime = System.currentTimeMillis();
	}

	void gatherTrackingDetails() {
		//gather maximum memory utilization
		memoryUtilization = Math.max(memoryUtilization, CloverJMX.MEMORY_MXBEAN.getHeapMemoryUsage().getUsed());
		
		//gather node related data
		for(NodeTrackingDetail nodeDetail : nodesDetails) {
			nodeDetail.gatherTrackingDetails();
		}
	}

	void phaseFinished() {
		result = phase.getResult();
		endTime = System.currentTimeMillis();

		//notice all node - phase finished
		for(NodeTrackingDetail nodeDetail : nodesDetails) {
			nodeDetail.phaseFinished();
		}
	}
}
