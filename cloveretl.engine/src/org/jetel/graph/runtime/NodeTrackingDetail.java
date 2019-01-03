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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.runtime.jmx.CloverJMX;
import org.jetel.graph.runtime.jmx.TrackingUtils;
import org.jetel.util.SubgraphUtils;

/**
 * This class represents tracking information about an node.
 * 
 * State of an instance is supposed to be changed over time
 * (it is used by WatchDog to gather information during an execution of graph).
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 6, 2008
 */
public class NodeTrackingDetail {
	
	private final PhaseTrackingDetail parentPhaseDetail;
	
	private final Node node;
	
	protected Result result;
    
    protected String nodeId;
    protected String nodeName;
    protected long totalCPUTime;
    protected long totalUserTime;
    protected float usageCPU;
    protected float peakUsageCPU;
    protected float usageUser;
    protected float peakUsageUser;
	
    private InputPortTrackingDetail[] inputPortsDetails;
    private OutputPortTrackingDetail[] outputPortsDetails;
    
    /**
     * Initial CPU time for component's threads.
     * Component's threads can be recycled, so we
     * need to remember initial state of CPU and user time for each of them.
     * Keys are Thread.getId() longs.
     */
    private final Map<Long, Long> initialThreadCpuTime = new HashMap<Long, Long>(); 
    private final Map<Long, Long> initialThreadUserTime = new HashMap<Long, Long>(); 
    
	public NodeTrackingDetail(PhaseTrackingDetail parentPhaseDetail, Node node) {
		this.parentPhaseDetail = parentPhaseDetail;
		this.nodeId = node.getId();
		this.nodeName = node.getName();
		this.result = Result.N_A;
		this.node = node;
		
		this.inputPortsDetails = new InputPortTrackingDetail[node.getInPorts().size()];
		int i = 0;
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
	
	public Result getResult() {
		return result;
	}

	public long getTotalCPUTime() {
		return totalCPUTime;
	}

	public long getTotalUserTime() {
		return totalUserTime;
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

	public String getNodeID() {
		return nodeId;
	}

	public String getNodeName() {
		return nodeName;
	}
	
	public PhaseTrackingDetail getParentPhaseTracking() {
		return parentPhaseDetail;
	}
	
	public InputPortTrackingDetail[] getInputPortTracking() {
		return inputPortsDetails;
	}

	public OutputPortTrackingDetail[] getOutputPortTracking() {
		return outputPortsDetails;
	}

	//******************* EVENTS ********************/
	void gatherTrackingDetails() {
		//get the node result (SubgraphInput and SubgraphOutput is handled in special way
		if (SubgraphUtils.isSubJobInputComponent(node.getType())) {
			result = getResultOfSubgraphInput(node);
		} else if (SubgraphUtils.isSubJobOutputComponent(node.getType())) {
			result = getResultOfSubgraphOutput(node);
		} else {
			result = node.getResultCode();
		}

		long phaseExecutionTime = getParentPhaseTracking().getExecutionTime();
		
		if (CloverJMX.isThreadCpuTimeSupported()) {
			Thread nodeThread = node.getNodeThread();
			if (nodeThread != null) {
				//totalCPUTime
				long tempTotalCPUTime = getThreadCpuTime(nodeThread);
				//totalCPUTime for child threads
				for (Thread childThread : node.getChildThreads()) {
					tempTotalCPUTime += getThreadCpuTime(childThread);
				}
				if (tempTotalCPUTime > totalCPUTime) {
					totalCPUTime = tempTotalCPUTime;
				}
				
				//totalUserTime
				long tempTotalUserTime = getThreadUserTime(nodeThread);
				//totalUserTime for child threads
				for (Thread childThread : node.getChildThreads()) {
					tempTotalUserTime += getThreadUserTime(childThread);
				}
				if(tempTotalUserTime > totalUserTime) {
					totalUserTime = tempTotalUserTime;
				}
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
	
	/**
	 * @return CPU time of given thread, first call for each thread is just initialization call
	 * where current CPU time is cached and used for next invocations.
	 */
	private long getThreadCpuTime(Thread thread) {
		long threadCPUTime = TrackingUtils.convertTime(
				CloverJMX.THREAD_MXBEAN.getThreadCpuTime(thread.getId()), 
				TimeUnit.NANOSECONDS,
				TrackingUtils.DEFAULT_TIME_UNIT);
		
		if (!initialThreadCpuTime.containsKey(thread.getId())) {
			initialThreadCpuTime.put(thread.getId(), threadCPUTime);
			return 0;
		} else {
			return threadCPUTime - initialThreadCpuTime.get(thread.getId());
		}
	}

	/**
	 * @return user time of given thread, first call for each thread is just initialization call
	 * where current user time is cached and used for next invocations.
	 */
	private long getThreadUserTime(Thread thread) {
		long threadUserTime = TrackingUtils.convertTime(
				CloverJMX.THREAD_MXBEAN.getThreadUserTime(thread.getId()),
				TimeUnit.NANOSECONDS,
				TrackingUtils.DEFAULT_TIME_UNIT);
		
		if (!initialThreadUserTime.containsKey(thread.getId())) {
			initialThreadUserTime.put(thread.getId(), threadUserTime);
			return 0;
		} else {
			return threadUserTime - initialThreadUserTime.get(thread.getId());
		}
	}

	/**
	 * @param node subgraph input component
	 * @return virtual result status of SubgraphInput component, this result is derived
	 * from traffic on output edges
	 */
	private static Result getResultOfSubgraphInput(Node node) {
		boolean isStop = true;
		for (OutputPort outputPort : node.getOutPorts()) {
			if (!outputPort.getEdge().isEofSent()
					&& !outputPort.getEdge().getReader().getResultCode().isStop()) {
				isStop = false; 
			}
		}
		return isStop ? Result.FINISHED_OK : Result.RUNNING;
	}
	
	/**
	 * @param node subgraph output component
	 * @return virtual result status of SubgraphOutput component, this result is derived
	 * from traffic on input edges
	 */
	private static Result getResultOfSubgraphOutput(Node node) {
		boolean isStop = true;
		for (InputPort inputPort : node.getInPorts()) {
			if (!inputPort.getEdge().isEofSent()
					&& !inputPort.getEdge().getWriter().getResultCode().isStop()) {
				isStop = false; 
			}
		}
		return isStop ? Result.FINISHED_OK : Result.RUNNING;
	}

}
