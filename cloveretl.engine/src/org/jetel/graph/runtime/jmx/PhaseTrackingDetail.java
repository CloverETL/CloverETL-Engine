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
package org.jetel.graph.runtime.jmx;

import org.jetel.graph.Node;
import org.jetel.graph.Phase;
import org.jetel.graph.Result;

/**
 * This class represents tracking information about an phase.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 6, 2008
 */
public class PhaseTrackingDetail implements PhaseTracking {

	private static final long serialVersionUID = 929691539023786046L;

	private final transient Phase phase;

	private final GraphTrackingDetail parentGraphDetail;
	
	private NodeTrackingDetail[] nodesDetails;
	
	private int phaseNum;
	
    private long startTime = -1;

    private long endTime = -1;
    
    private long memoryUtilization;
    
    private Result result;
    
	/**
	 * Constructor.
	 * @param phase
	 */
	public PhaseTrackingDetail(GraphTrackingDetail parentGraphDetail, Phase phase) {
		this.parentGraphDetail = parentGraphDetail;
		this.phase = phase;
		this.nodesDetails = new NodeTrackingDetail[phase.getNodes().size()];
		this.phaseNum = phase.getPhaseNum();
		this.result = Result.N_A;
		
		int i = 0;
		for (Node node : phase.getNodes().values()) {
			nodesDetails[i++] = new NodeTrackingDetail(this, node); 
		}
	}
	
	public PhaseTrackingDetail(GraphTrackingDetail parentGraphDetail) {
		phase = null;
		this.parentGraphDetail = parentGraphDetail;
		
	}
	
	public void copyFrom(PhaseTrackingDetail phaseDetail) {
		this.startTime = phaseDetail.startTime;
		this.endTime = phaseDetail.endTime;
		this.memoryUtilization = phaseDetail.memoryUtilization;
		this.result = phaseDetail.result;
		
		int i = 0;
		for (NodeTrackingDetail nodeDetail : nodesDetails) {
			nodeDetail.copyFrom(phaseDetail.nodesDetails[i++]);
		}
	}
	
	Phase getPhase() {
		return phase;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PhaseTracking#getParentGraphTracking()
	 */
	public GraphTracking getParentGraphTracking() {
		return parentGraphDetail;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PhaseTracking#getPhaseNum()
	 */
	public int getPhaseNum() {
		return phaseNum;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PhaseTracking#getStartTime()
	 */
	public long getStartTime() {
		return startTime;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PhaseTracking#getEndTime()
	 */
	public long getEndTime() {
		return endTime;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PhaseTracking#getMemoryUtilization()
	 */
	public long getMemoryUtilization() {
		return memoryUtilization;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PhaseTracking#getResult()
	 */
	public Result getResult() {
		return result;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PhaseTracking#getExecutionTime()
	 */
	public long getExecutionTime() {
		if (startTime == -1) {
			return -1;
		} else if (endTime == -1) {
			return System.nanoTime() - startTime;
		} else {
			return endTime - startTime;
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PhaseTracking#getNodeTracking()
	 */
	public NodeTracking[] getNodeTracking() {
		return nodesDetails;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PhaseTracking#getNodeTracking(java.lang.String)
	 */
	public NodeTracking getNodeTracking(String nodeID) {
		for (NodeTrackingDetail nodeDetail : nodesDetails) {
			if(nodeID.equals(nodeDetail.getNodeID())) {
				return nodeDetail;
			}
		}
		
		return null;
	}
	
	//******************* SETTERS *******************/
	
	public void setNodesDetails(NodeTrackingDetail[] nodesDetails) {
		this.nodesDetails = nodesDetails;
	}

	public void setPhaseNum(int phaseNum) {
		this.phaseNum = phaseNum;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public void setMemoryUtilization(long memoryUtilization) {
		this.memoryUtilization = memoryUtilization;
	}

	public void setResult(Result result) {
		this.result = result;
	}
	
	//******************* EVENTS ********************/
	void phaseStarted() {
		startTime = System.nanoTime();
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
		//notice all node - phase finished
		for(NodeTrackingDetail nodeDetail : nodesDetails) {
			nodeDetail.phaseFinished();
		}
		
		result = phase.getResult();
		endTime = System.nanoTime();
	}


}
