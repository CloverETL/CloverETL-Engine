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

import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;

/**
 * This class represents tracking information about whole graph.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 6, 2008
 */
public class GraphTrackingDetail implements Serializable {

	private static final long serialVersionUID = 7586330827349162718L;

	private transient final TransformationGraph graph;
	
	private final CloverJMX parentCloverJMX;
	
	private PhaseTrackingDetail runningPhaseDetail;
	
	private final PhaseTrackingDetail[] phasesDetails;
	
	private final String graphName;
	
	private long startTime = -1;
	
	private long endTime = -1;

    private Result result;

    private transient Result lastPhaseResult;
    
	/**
	 * Constructor.
	 * @param graph
	 */
	public GraphTrackingDetail(CloverJMX parentCloverJMX, TransformationGraph graph) {
		this.parentCloverJMX = parentCloverJMX;
		this.graph = graph;
		this.graphName = graph.getName();
		this.result = Result.N_A;
		this.phasesDetails = new PhaseTrackingDetail[graph.getPhases().length];
		
		int i = 0;
		for(Phase phase : graph.getPhases()) {
			phasesDetails[i++] = new PhaseTrackingDetail(this, phase);
		}
	}
	
	TransformationGraph getGraph() {
		return graph;
	}

	public CloverJMX getParentCloverJMX() {
		return parentCloverJMX;
	}
	
	public PhaseTrackingDetail getRunningPhaseDetail() {
		return runningPhaseDetail;
	}
	
	public void setRunningPhaseDetail(PhaseTrackingDetail runningphaseDetail) {
		this.runningPhaseDetail = runningphaseDetail; 
	}
	
	public PhaseTrackingDetail[] getPhasesDetails() {
		return phasesDetails;
	}

	public String getGraphName() {
		return graphName;
	}

	public long getStartTime() {
		return startTime;
	}

	public long getEndTime() {
		return endTime;
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

	public Result getResult() {
		return result;
	}

	private PhaseTrackingDetail getPhaseDetail(Phase phase) {
		for(PhaseTrackingDetail phaseTrackingDetail : phasesDetails) {
			if(phaseTrackingDetail.getPhase() == phase) {
				return phaseTrackingDetail;
			}
		}
		
		throw new IllegalArgumentException("Phase " + phase.getPhaseNum() + " is not tracked.");
	}

	public NodeTrackingDetail getNodeTrackingDetail(String nodeId) {
		for(PhaseTrackingDetail phaseDetail : phasesDetails) {
			NodeTrackingDetail nodeDetail = phaseDetail.getNodeTrackingDetail(nodeId);
			if(nodeDetail != null) {
				return nodeDetail;
			}
		}
		
		return null;
	}
	
	//******************* EVENTS ********************/
	void graphStarted() {
		startTime = System.nanoTime();
		
		result = Result.RUNNING;
	}

	void phaseStarted(Phase phase) {
		setRunningPhaseDetail(getPhaseDetail(phase));
		
		getRunningPhaseDetail().phaseStarted();
	}

	void gatherTrackingDetails() {
		getRunningPhaseDetail().gatherTrackingDetails();
	}

	void phaseFinished() {
		gatherTrackingDetails();
		getRunningPhaseDetail().phaseFinished();
		
		lastPhaseResult = getRunningPhaseDetail().getResult();
	}

	void graphFinished() {
		result = lastPhaseResult;
		
		endTime = System.nanoTime();
	}

}
