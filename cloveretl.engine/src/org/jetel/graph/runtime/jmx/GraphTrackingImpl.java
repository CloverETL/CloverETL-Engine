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
import org.jetel.graph.runtime.GraphTrackingProvider;
import org.jetel.graph.runtime.PhaseTrackingProvider;

/**
 * Simple DTO holding tracking information about whole graph.
 * 
 * @author Filip Reichman
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jan 2, 2019
 */
public class GraphTrackingImpl implements GraphTracking {

	private static final long serialVersionUID = 7586330827349162718L;

	private PhaseTracking runningPhaseDetail;
	
	private PhaseTracking[] phasesDetails;

	private GraphError graphError;
	
	protected String graphName;
	
	protected long startTime = -1;
	
	protected long endTime = -1;

	protected Result result;

	protected String nodeId;
    
	protected long runId;
	
	public GraphTrackingImpl() {
		this.phasesDetails = new PhaseTrackingImpl[0];
	}

	public GraphTrackingImpl(GraphTrackingProvider graphTracking) {
		this.graphName = graphTracking.getGraphName();
		this.result = graphTracking.getResult();
		
		this.startTime = graphTracking.getStartTime();
		this.endTime = graphTracking.getEndTime();
		this.result = graphTracking.getResult();
		this.graphError = graphTracking.getGraphError();
		this.nodeId = graphTracking.getNodeId();
		this.runId = graphTracking.getRunId();
		
		this.phasesDetails = new PhaseTrackingImpl[graphTracking.getPhaseTracking().length];
		int i = 0;
		for (PhaseTrackingProvider phaseDetail : graphTracking.getPhaseTracking()) {
			phasesDetails[i++] = phaseDetail.createSnapshot();
		}
		
		if (this.getRunningPhaseTracking() != null) {
			this.runningPhaseDetail = getPhaseTracking(graphTracking.getRunningPhaseTracking().getPhaseNum());
		}
	}
	
	public GraphTrackingImpl(TransformationGraph graph) {
		this.graphName = graph.getName();
		this.result = Result.N_A;
		this.phasesDetails = new PhaseTrackingImpl[graph.getPhases().length];
		
		int i = 0;
		for(Phase phase : graph.getPhases()) {
			phasesDetails[i++] = new PhaseTrackingImpl(phase);
		}
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
	
	@Override
	public long getRunId() {
		return runId;
	}
	
	@Override
	public GraphError getGraphError() {
		return graphError;
	}

	@Override
	public PhaseTracking[] getPhaseTracking() {
		return phasesDetails;
	}

	@Override
	public PhaseTracking getRunningPhaseTracking() {
		return runningPhaseDetail;
	}

	@Override
	public PhaseTracking getPhaseTracking(int phaseNum) {
		for (PhaseTracking phaseDetail : phasesDetails) {
			if (phaseDetail.getPhaseNum() == phaseNum) {
				return phaseDetail;
			}
		}
		return null;
	}

	public void setRunningPhaseDetail(PhaseTracking runningPhaseDetail) {
		this.runningPhaseDetail = runningPhaseDetail;
	}

	public void setPhasesDetails(PhaseTracking[] phasesDetails) {
		this.phasesDetails = phasesDetails;
	}

	public void setResult(Result result) {
		this.result = result;
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
	
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public void setRunId(long runId) {
		this.runId = runId;
	}
}
