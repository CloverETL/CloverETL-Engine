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
public class GraphTrackingDetail implements GraphTracking {

	private static final long serialVersionUID = 7586330827349162718L;

	private transient final TransformationGraph graph;
	
	private PhaseTrackingDetail runningPhaseDetail;
	
	private PhaseTrackingDetail[] phasesDetails;
	
	private String graphName;
	
	private long startTime = -1;
	
	private long endTime = -1;

    private Result result;

    private String nodeId;
    
    private transient Result lastPhaseResult;
    
	/**
	 * Constructor.
	 * @param graph
	 */
	public GraphTrackingDetail(TransformationGraph graph) {
		this.graph = graph;
		this.graphName = graph.getName();
		this.result = Result.N_A;
		this.phasesDetails = new PhaseTrackingDetail[graph.getPhases().length];
		
		int i = 0;
		for(Phase phase : graph.getPhases()) {
			phasesDetails[i++] = new PhaseTrackingDetail(this, phase);
		}
	}
	
	/**
	 * Allocates a new <tt>GraphTrackingDetail</tt> object.
	 *
	 */
	public GraphTrackingDetail() {
		graph = null;
	}
	
	public void copyFrom(GraphTrackingDetail graphDetail) {
		this.runningPhaseDetail = getPhaseDetail(graphDetail.getRunningPhaseTracking().getPhaseNum());
		this.startTime = graphDetail.startTime;
		this.endTime = graphDetail.endTime;
		this.result = graphDetail.result;
		
		int i = 0;
		for (PhaseTrackingDetail phaseDetail : phasesDetails) {
			phaseDetail.copyFrom(graphDetail.phasesDetails[i++]);
		}
	}
	
	TransformationGraph getGraph() {
		return graph;
	}

	public void setRunningPhaseDetail(PhaseTrackingDetail runningphaseDetail) {
		this.runningPhaseDetail = runningphaseDetail; 
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.GraphTracking#getPhaseTracking(int)
	 */
	@Override
	public PhaseTracking getPhaseTracking(int phaseNum) {
		for (PhaseTrackingDetail phaseDetail : phasesDetails) {
			if (phaseDetail.getPhaseNum() == phaseNum) {
				return phaseDetail;
			}
		}
		return null;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.GraphTracking#getPhaseTracking()
	 */
	@Override
	public PhaseTracking[] getPhaseTracking() {
		return phasesDetails;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.GraphTracking#getRunningPhaseTracking()
	 */
	@Override
	public PhaseTracking getRunningPhaseTracking() {
		return runningPhaseDetail;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.GraphTracking#getGraphName()
	 */
	@Override
	public String getGraphName() {
		return graphName;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.GraphTracking#getStartTime()
	 */
	@Override
	public long getStartTime() {
		return startTime;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.GraphTracking#getEndTime()
	 */
	@Override
	public long getEndTime() {
		return endTime;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.GraphTracking#getExecutionTime()
	 */
	@Override
	public long getExecutionTime() {
		if (startTime == -1) {
			return -1;
		} else if (endTime == -1) {
			return System.currentTimeMillis() - startTime;
		} else {
			return endTime - startTime;
		}
	}

	@Override
	public int getUsedMemory() {
		return graph.getMemoryTracker().getUsedMemory();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.GraphTracking#getResult()
	 */
	@Override
	public Result getResult() {
		return result;
	}

	@Override
	public String getNodeId() {
		return nodeId;
	}
	
	private PhaseTrackingDetail getPhaseDetail(int phaseNum) {
		for(PhaseTrackingDetail phaseTracking : phasesDetails) {
			if(phaseTracking.getPhaseNum() == phaseNum) {
				return phaseTracking;
			}
		}
		
		throw new IllegalArgumentException("Phase " + phaseNum + " is not tracked.");
	}

	
	//******************* SETTERS *******************/
	
	public void setPhasesDetails(PhaseTrackingDetail[] phasesDetails) {
		this.phasesDetails = phasesDetails;
	}

	public void setGraphName(String graphName) {
		this.graphName = graphName;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public void setEndTime(long endTime) {
		this.endTime = endTime;
	}

	public void setResult(Result result) {
		this.result = result;
	}
	
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public void setLastPhaseResult(Result lastPhaseResult) {
		this.lastPhaseResult = lastPhaseResult;
	}

	//******************* EVENTS ********************/
	void graphStarted() {
		startTime = System.currentTimeMillis();
		
		result = Result.RUNNING;
	}


	void phaseStarted(Phase phase) {
		setRunningPhaseDetail(getPhaseDetail(phase.getPhaseNum()));
		
		runningPhaseDetail.phaseStarted();
	}

	void gatherTrackingDetails() {
		if (runningPhaseDetail != null)
			runningPhaseDetail.gatherTrackingDetails();
	}

	void phaseFinished() {
		gatherTrackingDetails();
		runningPhaseDetail.phaseFinished();
		
		lastPhaseResult = runningPhaseDetail.getResult();
	}

	void graphFinished() {
		result = lastPhaseResult;
		
		endTime = System.currentTimeMillis();
	}


}
