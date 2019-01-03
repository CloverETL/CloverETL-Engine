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

import org.jetel.graph.Phase;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.jmx.GraphError;
import org.jetel.graph.runtime.jmx.GraphErrorDetail;
import org.jetel.graph.runtime.jmx.GraphTracking;
import org.jetel.graph.runtime.jmx.GraphTrackingImpl;

/**
 * This class represents tracking information about whole graph.
 * 
 * State of an instance is supposed to be changed over time
 * (it is used by WatchDog to gather information during an execution of graph).
 * 
 * @author Filip Reichman
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jan 2, 2019
 */
public class GraphTrackingDetail {

	private final TransformationGraph graph;
	
	protected String graphName;
	
	protected long startTime = -1;
	
	protected long endTime = -1;

	protected Result result;

	protected String nodeId;
    
	protected long runId;
	
	private PhaseTrackingDetail runningPhaseDetail;
	
	private PhaseTrackingDetail[] phasesDetails;
	
	private GraphErrorDetail graphError;
	
	/**
	 * Constructor.
	 * @param graph
	 */
	public GraphTrackingDetail(TransformationGraph graph) {
		this.graphName = graph.getName();
		this.result = Result.N_A;
		this.graph = graph;
		this.phasesDetails = new PhaseTrackingDetail[graph.getPhases().length];
		
		int i = 0;
		for(Phase phase : graph.getPhases()) {
			phasesDetails[i++] = new PhaseTrackingDetail(phase);
		}
	}
	
	public GraphTracking createSnapshot() {
		return new GraphTrackingImpl(this);
	}
	
	public void setRunningPhaseDetail(PhaseTrackingDetail runningphaseDetail) {
		this.runningPhaseDetail = runningphaseDetail; 
	}

	public PhaseTrackingDetail[] getPhaseTracking() {
		return phasesDetails;
	}

	public PhaseTrackingDetail getRunningPhaseTracking() {
		return runningPhaseDetail;
	}
	
	public GraphError getGraphError() {
		return graphError;
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
			return System.currentTimeMillis() - startTime;
		} else {
			return endTime - startTime;
		}
	}

	public Result getResult() {
		return result;
	}
	
	public String getNodeId() {
		return nodeId;
	}
	
	public long getRunId() {
		return runId;
	}

	private PhaseTrackingDetail getPhaseDetail(int phaseNum) {
		for(PhaseTrackingDetail phaseTracking : phasesDetails) {
			if(phaseTracking.getPhaseNum() == phaseNum) {
				return phaseTracking;
			}
		}
		
		throw new IllegalArgumentException("Phase " + phaseNum + " is not tracked.");
	}
	
	
	
	//******************* EVENTS ********************/
	public void graphStarted() {
		startTime = System.currentTimeMillis();
		
		result = Result.RUNNING;
		runId = graph.getWatchDog().getGraphRuntimeContext().getRunId();
	}


	public void phaseStarted(Phase phase) {
		setRunningPhaseDetail(getPhaseDetail(phase.getPhaseNum()));
		
		runningPhaseDetail.phaseStarted();
	}

	public void gatherTrackingDetails() {
		if (runningPhaseDetail != null)
			runningPhaseDetail.gatherTrackingDetails();
	}

	public void phaseFinished() {
		gatherTrackingDetails();
		runningPhaseDetail.phaseFinished();
	}

	public void graphFinished() {
		if (!result.isStop()) {
			endTime = System.currentTimeMillis();
			result = graph.getWatchDog().getStatus();
			
			//populate graph error
			graphError = GraphErrorDetail.createInstance(graph.getWatchDog());
			
			runningPhaseDetail = null;
		}
	}
}
