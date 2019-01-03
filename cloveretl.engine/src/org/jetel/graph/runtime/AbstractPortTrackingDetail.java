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

/**
 * This abstract class represents common tracking information on a port.
 * 
 * State of an instance is supposed to be changed over time
 * (it is used by WatchDog to gather information during an execution of graph). 
 * 
 * 
 * @author Filip Reichman
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jan 2, 2019
 */
public abstract class AbstractPortTrackingDetail {
	
	private static final int MIN_TIMESLACE = 1000;
	
	protected final NodeTrackingDetail parentNodeTracking;
	
	protected final int index;
	
	protected long totalRecords;
	protected long totalBytes;
    
	protected int recordFlow;
	protected int recordPeak;
    
	protected int byteFlow;
	protected int bytePeak;
    
	protected int waitingRecords;
	protected int averageWaitingRecords;

	protected int usedMemory;
	
	protected long remoteRunId;

	private long lastGatherTime;
	
	protected AbstractPortTrackingDetail(NodeTrackingDetail parentNodeTracking, int index) {
		this.parentNodeTracking = parentNodeTracking;
		this.index = index;
	}

	abstract void gatherTrackingDetails();
	
	public int getIndex() {
		return index;
	}
	
	public long getTotalRecords() {
		return totalRecords;
	}
	public long getTotalBytes() {
		return totalBytes;
	}

	public int getRecordFlow() {
		return recordFlow;
	}

	public int getRecordPeak() {
		return recordPeak;
	}

	public int getByteFlow() {
		return byteFlow;
	}

	public int getBytePeak() {
		return bytePeak;
	}

	public int getWaitingRecords() {
		return waitingRecords;
	}

	public int getAverageWaitingRecords() {
		return averageWaitingRecords;
	}

	public int getUsedMemory() {
		return usedMemory;
	}
	
	public long getRemoteRunId() {
		return remoteRunId;
	}
	
	protected void gatherTrackingDetails0(long newTotalRecords, long newTotalBytes, int waitingRecords) {
		long currentTime = System.currentTimeMillis();
		long timespan = lastGatherTime != 0 ? currentTime - lastGatherTime : 0; 

    	if(timespan > MIN_TIMESLACE) { // for too small time slice are statistic values too distorted
    	    //recordFlow
	        recordFlow = (int) (((long) (newTotalRecords - totalRecords)) * 1000 / timespan);

	        //recordPeak
	        recordPeak = Math.max(recordPeak, recordFlow);

    	    //byteFlow
	        byteFlow = (int) (((long) (newTotalBytes - totalBytes)) * 1000 / timespan);

	        //bytePeak
	        bytePeak = Math.max(bytePeak, byteFlow);
	        
	    	lastGatherTime = currentTime;
    	} else {
    		if(lastGatherTime == 0) {
    	    	lastGatherTime = currentTime;
    		}
    	}
		
		//totalRows
	    totalRecords = newTotalRecords;

	    //totalBytes
	    totalBytes = newTotalBytes;
	    
    	//waitingRecords
        this.waitingRecords = waitingRecords;
        
	    //averageWaitingRecords
        averageWaitingRecords = Math.abs(waitingRecords - averageWaitingRecords) / 2;
	}

	void phaseFinished() {
		long executionTime = getParentNodeTracking().getParentPhaseTracking().getExecutionTime();
		if (executionTime > 0) {
		    //recordFlow - average flow is calculated
			recordFlow = (int) ((totalRecords * 1000) / executionTime);
		    //byteFlow - average flow is calculated
	        byteFlow = (int) ((totalBytes * 1000) / executionTime);
		} else {
			recordFlow = 0;
			byteFlow = 0;
		}
	}
	
	private NodeTrackingDetail getParentNodeTracking() {
		return parentNodeTracking;
	}
}
