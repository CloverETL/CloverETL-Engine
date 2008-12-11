/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
abstract public class PortTrackingDetail implements Serializable {

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
    }
    
	public NodeTrackingDetail getParentNodeDetail() {
		return parentNodeDetail;
	}

	public int getIndex() {
		return index;
	}

	public int getTotalRecords() {
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

	abstract public String getType();
	
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
