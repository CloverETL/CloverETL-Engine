/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-08  David Pavlis <david_pavlis@hotmail.com>
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
package org.jetel.graph.runtime.jmx;

import java.io.Serializable;

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
public class PhaseTrackingDetail implements Serializable {

	private static final long serialVersionUID = 929691539023786046L;

	private final transient Phase phase;

	private final GraphTrackingDetail parentGraphDetail;
	
	private final NodeTrackingDetail[] nodesDetails;
	
	private final int phaseNum;
	
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
	
	Phase getPhase() {
		return phase;
	}

	public GraphTrackingDetail getParentGraphDetail() {
		return parentGraphDetail;
	}

	public NodeTrackingDetail[] getNodesDetails() {
		return nodesDetails;
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

	public long getExecutionTime() {
		if (startTime == -1) {
			return -1;
		} else if (endTime == -1) {
			return System.nanoTime() - startTime;
		} else {
			return endTime - startTime;
		}
	}

	public int getExecutionTimeSec() {
		return (int) (getExecutionTime() / 1000000000L);
	}

	public NodeTrackingDetail getNodeTrackingDetail(String nodeId) {
		for (NodeTrackingDetail nodeDetail : nodesDetails) {
			if(nodeId.equals(nodeDetail.getNodeId())) {
				return nodeDetail;
			}
		}
		
		return null;
	}
	
	//******************* EVENTS ********************/
	public void phaseStarted() {
		startTime = System.nanoTime();
	}

	public void gatherTrackingDetails() {
		//gather maximum memory utilization
		memoryUtilization = Math.max(memoryUtilization, CloverJMX.MEMORY_MXBEAN.getHeapMemoryUsage().getUsed());
		
		//gather node related data
		for(NodeTrackingDetail nodeDetail : nodesDetails) {
			nodeDetail.gatherTrackingDetails();
		}
	}

	public void phaseFinished() {
		endTime = System.nanoTime();
		result = phase.getResult();
	}

}
