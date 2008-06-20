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
import java.util.concurrent.TimeUnit;

import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;

/**
 * This class represents tracking information about an node.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 6, 2008
 */
public class NodeTrackingDetail implements Serializable {

	private static final long serialVersionUID = 3570320889692545386L;
	
	private final transient Node node;
	private final PhaseTrackingDetail parentPhaseDetail;
	
    private Result result;

    private final InputPortTrackingDetail[] inputPortsDetails;
    private final OutputPortTrackingDetail[] outputPortsDetails;
    
    private final String nodeId;
    private long totalCPUTime;
    private long totalUserTime;
    private float usageCPU;
    private float peakUsageCPU;
    private float usageUser;
    private float peakUsageUser;

	public NodeTrackingDetail(PhaseTrackingDetail parentPhaseDetail, Node node) {
		this.parentPhaseDetail = parentPhaseDetail;
		this.node = node;
		this.nodeId = node.getId();
		this.result = Result.N_A;
		int i;
		
		this.inputPortsDetails = new InputPortTrackingDetail[node.getInPorts().size()];
		i = 0;
		for (InputPort inputPort : node.getInPorts()) {
			inputPortsDetails[i] = new InputPortTrackingDetail(this, inputPort, i);
			i++;
		}
		
		this.outputPortsDetails = new OutputPortTrackingDetail[node.getOutPorts().size()];
		i = 0;
		for (OutputPort outputPort : node.getOutPorts()) {
			outputPortsDetails[i] = new OutputPortTrackingDetail(this, outputPort, i);
			i++;
		}
	}

	Node getNode() {
		return node;
	}

	public String getNodeId() {
		return nodeId;
	}
	
	public PhaseTrackingDetail getParentPhaseDetail() {
		return parentPhaseDetail;
	}

	public Result getResult() {
		return result;
	}

	public InputPortTrackingDetail[] getInputPortsDetails() {
		return inputPortsDetails;
	}

	public OutputPortTrackingDetail[] getOutputPortsDetails() {
		return outputPortsDetails;
	}

	/**
	 * @return total CPU time in nanoseconds
	 */
	public long getTotalCPUTime() {
		return totalCPUTime;
	}

	public long getTotalCPUTime(TimeUnit timeUnit) {
		return timeUnit.convert(getTotalCPUTime(), TimeUnit.NANOSECONDS);
	}

	/**
	 * @return total user time in nanoseconds
	 */
	public long getTotalUserTime() {
		return totalUserTime;
	}

	public long getTotalUserTime(TimeUnit timeUnit) {
		return timeUnit.convert(getTotalUserTime(), TimeUnit.NANOSECONDS);
	}

	public float getUsageCPU() {
		return usageCPU;
	}

	public float getPeakUsageCPU() {
		return peakUsageCPU;
	}

	public float getPeakUsageUser() {
		return peakUsageUser;
	}

	public float getUsageUser() {
		return usageUser;
	}

	public PortTrackingDetail[] getPortsDetails() {
		PortTrackingDetail[] ret = new PortTrackingDetail[inputPortsDetails.length + outputPortsDetails.length];
		
		System.arraycopy(inputPortsDetails, 0, ret, 0, inputPortsDetails.length);
		System.arraycopy(outputPortsDetails, 0, ret, inputPortsDetails.length, outputPortsDetails.length);
		
		return ret;
	}
	
	public boolean hasPorts() {
		return inputPortsDetails.length > 0 || outputPortsDetails.length > 0;
	}
	
	//******************* EVENTS ********************/
	public void gatherTrackingDetails() {
		//result
		result = node.getResultCode();
		if (result != Result.RUNNING && result != Result.FINISHED_OK) {
			return;
		}

		long phaseExecutionTime = getParentPhaseDetail().getExecutionTime();
		
		//totalCPUTime
		final long tempTotalCPUTime = CloverJMX.isThreadCpuTimeSupported() ? 
				CloverJMX.THREAD_MXBEAN.getThreadCpuTime(node.getNodeThread().getId()) : 0;
		if(tempTotalCPUTime > totalCPUTime) {
			totalCPUTime = tempTotalCPUTime;
		}
		
		//totalUserTime
		final long tempTotalUserTime = CloverJMX.isThreadCpuTimeSupported() ? 
				CloverJMX.THREAD_MXBEAN.getThreadUserTime(node.getNodeThread().getId()) : 0;
		if(tempTotalUserTime > totalUserTime) {
			totalUserTime = tempTotalUserTime;
		}
				
        //usageCPU
        usageCPU = (float) totalCPUTime / phaseExecutionTime;
        
        //peakUsageCPU
        peakUsageCPU = Math.max(peakUsageCPU, usageCPU);
        
        //usageUser
        usageUser = (float) totalUserTime / phaseExecutionTime;
        
        //peakUsageUser
        peakUsageUser = Math.max(peakUsageUser, usageUser);

		//gather input ports related data
		for(InputPortTrackingDetail inputPortDetail: inputPortsDetails) {
			inputPortDetail.gatherTrackingDetails();
		}

		//gather output ports related data
		for(OutputPortTrackingDetail outputPortDetail: outputPortsDetails) {
			outputPortDetail.gatherTrackingDetails();
		}
	}

	void phaseFinished() {
		//gather input ports related data
		for(InputPortTrackingDetail inputPortDetail: inputPortsDetails) {
			inputPortDetail.phaseFinished();
		}

		//gather output ports related data
		for(OutputPortTrackingDetail outputPortDetail: outputPortsDetails) {
			outputPortDetail.phaseFinished();
		}
	}
	
}
