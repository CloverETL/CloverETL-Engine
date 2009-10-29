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
public class NodeTrackingDetail implements NodeTracking {

	private static final long serialVersionUID = 3570320889692545386L;
	
	private final transient Node node;
	private final PhaseTrackingDetail parentPhaseDetail;
	
    private Result result;

    private InputPortTrackingDetail[] inputPortsDetails;
    private OutputPortTrackingDetail[] outputPortsDetails;
    
    private String nodeId;
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
			inputPortsDetails[i] = new InputPortTrackingDetail(this, inputPort);
			i++;
		}
		
		this.outputPortsDetails = new OutputPortTrackingDetail[node.getOutPorts().size()];
		i = 0;
		for (OutputPort outputPort : node.getOutPorts()) {
			outputPortsDetails[i] = new OutputPortTrackingDetail(this, outputPort);
			i++;
		}
	}
	
	public NodeTrackingDetail(PhaseTrackingDetail parentPhaseDetail) {
		this.parentPhaseDetail = parentPhaseDetail;
		this.node = null;
	}

	public void copyFrom(NodeTrackingDetail nodeDetail) {
		this.result = nodeDetail.result;
		this.totalCPUTime = nodeDetail.totalCPUTime;
		this.totalUserTime = nodeDetail.totalUserTime;
		this.usageCPU = nodeDetail.usageCPU;
		this.peakUsageCPU = nodeDetail.peakUsageCPU;
		this.usageUser = nodeDetail.usageUser;
		this.peakUsageUser = nodeDetail.peakUsageUser;
		
		int i = 0;
		for (InputPortTrackingDetail inputPortDetail : inputPortsDetails) {
			inputPortDetail.copyFrom(nodeDetail.inputPortsDetails[i++]);
		}

		i = 0;
		for (OutputPortTrackingDetail outputPortDetail : outputPortsDetails) {
			outputPortDetail.copyFrom(nodeDetail.outputPortsDetails[i++]);
		}
	}
	
	Node getNode() {
		return node;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getParentPhaseTracking()
	 */
	public PhaseTracking getParentPhaseTracking() {
		return parentPhaseDetail;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getResult()
	 */
	public Result getResult() {
		return result;
	}

	public InputPortTrackingDetail[] getInputPortsDetails() {
		return inputPortsDetails;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getInputPortTracking(int)
	 */
	public PortTracking getInputPortTracking(int portNumber) {
		for (InputPortTrackingDetail inputPortDetail : inputPortsDetails) {
			if (inputPortDetail.getIndex() == portNumber) {
				return inputPortDetail;
			}
		}
		return null;
	}

	public OutputPortTrackingDetail[] getOutputPortsDetails() {
		return outputPortsDetails;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getOutputPortTracking(int)
	 */
	public PortTracking getOutputPortTracking(int portNumber) {
		for (OutputPortTrackingDetail outputPortDetail : outputPortsDetails) {
			if (outputPortDetail.getIndex() == portNumber) {
				return outputPortDetail;
			}
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getTotalCPUTime()
	 */
	public long getTotalCPUTime() {
		return totalCPUTime;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getTotalUserTime()
	 */
	public long getTotalUserTime() {
		return totalUserTime;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getUsageCPU()
	 */
	public float getUsageCPU() {
		return usageCPU;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getPeakUsageCPU()
	 */
	public float getPeakUsageCPU() {
		return peakUsageCPU;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getPeakUsageUser()
	 */
	public float getPeakUsageUser() {
		return peakUsageUser;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getUsageUser()
	 */
	public float getUsageUser() {
		return usageUser;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getNodeID()
	 */
	public String getNodeID() {
		return nodeId;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getInputPortTracking()
	 */
	public PortTracking[] getInputPortTracking() {
		return inputPortsDetails;
	}


	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#getOutputPortTracking()
	 */
	public PortTracking[] getOutputPortTracking() {
		return outputPortsDetails;
	}

	public PortTrackingDetail[] getPortsDetails() {
		PortTrackingDetail[] ret = new PortTrackingDetail[inputPortsDetails.length + outputPortsDetails.length];
		
		System.arraycopy(inputPortsDetails, 0, ret, 0, inputPortsDetails.length);
		System.arraycopy(outputPortsDetails, 0, ret, inputPortsDetails.length, outputPortsDetails.length);
		
		return ret;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.NodeTracking#hasPorts()
	 */
	public boolean hasPorts() {
		return inputPortsDetails.length > 0 || outputPortsDetails.length > 0;
	}
	
	//******************* SETTERS *******************/
	
	public void setResult(Result result) {
		this.result = result;
	}

	public void setInputPortsDetails(InputPortTrackingDetail[] inputPortsDetails) {
		this.inputPortsDetails = inputPortsDetails;
	}

	public void setOutputPortsDetails(OutputPortTrackingDetail[] outputPortsDetails) {
		this.outputPortsDetails = outputPortsDetails;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public void setTotalCPUTime(long totalCPUTime) {
		this.totalCPUTime = totalCPUTime;
	}

	public void setTotalUserTime(long totalUserTime) {
		this.totalUserTime = totalUserTime;
	}

	public void setUsageCPU(float usageCPU) {
		this.usageCPU = usageCPU;
	}

	public void setPeakUsageCPU(float peakUsageCPU) {
		this.peakUsageCPU = peakUsageCPU;
	}

	public void setUsageUser(float usageUser) {
		this.usageUser = usageUser;
	}

	public void setPeakUsageUser(float peakUsageUser) {
		this.peakUsageUser = peakUsageUser;
	}

	//******************* EVENTS ********************/
	public void gatherTrackingDetails() {
		//result
		result = node.getResultCode();
		if (result != Result.RUNNING && result != Result.FINISHED_OK) {
			return;
		}

		long phaseExecutionTime = getParentPhaseTracking().getExecutionTime();
		
		if (CloverJMX.isThreadCpuTimeSupported()) {
			//totalCPUTime
			long tempTotalCPUTime = CloverJMX.THREAD_MXBEAN.getThreadCpuTime(node.getNodeThread().getId());
			//totalCPUTime for child threads
			for (Thread childThread : node.getChildThreads()) {
				tempTotalCPUTime += CloverJMX.THREAD_MXBEAN.getThreadCpuTime(childThread.getId());
			}
			if (tempTotalCPUTime > totalCPUTime) {
				totalCPUTime = tempTotalCPUTime;
			}
			
			//totalUserTime
			long tempTotalUserTime = CloverJMX.THREAD_MXBEAN.getThreadUserTime(node.getNodeThread().getId());
			//totalUserTime for child threads
			for (Thread childThread : node.getChildThreads()) {
				tempTotalUserTime += CloverJMX.THREAD_MXBEAN.getThreadUserTime(childThread.getId());
			}
			if(tempTotalUserTime > totalUserTime) {
				totalUserTime = tempTotalUserTime;
			}
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
