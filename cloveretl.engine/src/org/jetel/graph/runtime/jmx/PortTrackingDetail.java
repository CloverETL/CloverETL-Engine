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


/**
 * This abstract class represents common tracking information on an port.
 * 
 * @see InputPortTrackingDetail
 * @see OutputPortTrackingDetail
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 6, 2008
 */
public abstract class PortTrackingDetail implements PortTracking {

	private static final long serialVersionUID = -8999440507780259714L;
	
	private static final int MIN_TIMESLACE = 100000;

	private long lastGatherTime;

	private final NodeTrackingDetail parentNodeDetail;

	protected final int index;
	
	protected int totalRecords;
	protected long totalBytes;
    
	protected int recordFlow;
	protected int recordPeak;
    
	protected int byteFlow;
	protected int bytePeak;
    
	protected int waitingRecords;
	protected int averageWaitingRecords;

	protected int usedMemory;
	
    protected PortTrackingDetail(NodeTrackingDetail parentNodeDetail, int index) {
    	this.parentNodeDetail = parentNodeDetail;
    	this.index = index;
	}

    public void copyFrom(PortTrackingDetail portDetail) {
    	this.lastGatherTime = portDetail.lastGatherTime;
    	this.totalRecords = portDetail.totalRecords;
    	this.totalBytes = portDetail.totalBytes;
    	this.recordFlow = portDetail.recordFlow;
    	this.recordPeak = portDetail.recordPeak;
    	this.byteFlow = portDetail.byteFlow;
    	this.bytePeak = portDetail.bytePeak;
    	this.waitingRecords= portDetail.waitingRecords;
    	this.averageWaitingRecords = portDetail.averageWaitingRecords;
    	this.usedMemory = portDetail.usedMemory;
    }
    
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTracking#getParentNodeTracking()
	 */
	@Override
	public NodeTracking getParentNodeTracking() {
		return parentNodeDetail;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTracking#getIndex()
	 */
	@Override
	public int getIndex() {
		return index;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTracking#getTotalRecords()
	 */
	@Override
	public long getTotalRecords() {
		return totalRecords;
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTracking#getTotalBytes()
	 */
	@Override
	public long getTotalBytes() {
		return totalBytes;
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTracking#getRecordFlow()
	 */
	@Override
	public int getRecordFlow() {
		return recordFlow;
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTracking#getRecordPeak()
	 */
	@Override
	public int getRecordPeak() {
		return recordPeak;
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTracking#getByteFlow()
	 */
	@Override
	public int getByteFlow() {
		return byteFlow;
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTracking#getBytePeak()
	 */
	@Override
	public int getBytePeak() {
		return bytePeak;
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTracking#getWaitingRecords()
	 */
	@Override
	public int getWaitingRecords() {
		return waitingRecords;
	}
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTracking#getAverageWaitingRecords()
	 */
	@Override
	public int getAverageWaitingRecords() {
		return averageWaitingRecords;
	}

	@Override
	public int getUsedMemory() {
		return usedMemory;
	}
	
	public void setLastGatherTime(long lastGatherTime) {
		this.lastGatherTime = lastGatherTime;
	}

	public void setTotalRecords(int totalRecords) {
		this.totalRecords = totalRecords;
	}

	public void setTotalBytes(long totalBytes) {
		this.totalBytes = totalBytes;
	}

	public void setRecordFlow(int recordFlow) {
		this.recordFlow = recordFlow;
	}

	public void setRecordPeak(int recordPeak) {
		this.recordPeak = recordPeak;
	}

	public void setByteFlow(int byteFlow) {
		this.byteFlow = byteFlow;
	}

	public void setBytePeak(int bytePeak) {
		this.bytePeak = bytePeak;
	}

	public void setWaitingRecords(int waitingRecords) {
		this.waitingRecords = waitingRecords;
	}

	public void setAverageWaitingRecords(int averageWaitingRecords) {
		this.averageWaitingRecords = averageWaitingRecords;
	}
	
	public void setUsedMemory(int usedMemory) {
		this.usedMemory = usedMemory;
	}

	abstract void gatherTrackingDetails();
	
	protected void gatherTrackingDetails0(int newTotalRecords, long newTotalBytes, int waitingRecords) {
		long currentTime = System.nanoTime();
		long timespan = lastGatherTime != 0 ? currentTime - lastGatherTime : 0; 

    	if(timespan > MIN_TIMESLACE) { // for too small time slice are statistic values too distorted
    	    //recordFlow
	        recordFlow = (int) (((long) (newTotalRecords - totalRecords)) * 1000000000 / timespan);

	        //recordPeak
	        recordPeak = Math.max(recordPeak, recordFlow);

    	    //byteFlow
	        byteFlow = (int) (((long) (newTotalBytes - totalBytes)) * 1000000000 / timespan);

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
	    //recordFlow
        recordFlow = 0;

	    //byteFlow
        byteFlow = 0;
	}
	
}
