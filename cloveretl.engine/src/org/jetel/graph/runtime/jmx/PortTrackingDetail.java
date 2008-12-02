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
	
	protected int totalRows;
	protected long totalBytes;
    
	protected int averageRows;
	protected int peakRows;
    
	protected int averageBytes;
	protected int peakBytes;
    
	protected int waitingRows;
	protected int averageWaitingRows;

    protected PortTrackingDetail(NodeTrackingDetail parentNodeDetail, int index) {
    	this.parentNodeDetail = parentNodeDetail;
    	this.index = index;
	}

    public void copyFrom(PortTrackingDetail portDetail) {
    	this.lastGatherTime = portDetail.lastGatherTime;
    	this.totalRows = portDetail.totalRows;
    	this.totalBytes = portDetail.totalBytes;
    	this.averageRows = portDetail.averageRows;
    	this.peakRows = portDetail.peakRows;
    	this.averageBytes = portDetail.averageBytes;
    	this.peakBytes = portDetail.peakBytes;
    	this.waitingRows = portDetail.waitingRows;
    	this.averageWaitingRows = portDetail.averageWaitingRows;
    }
    
	public NodeTrackingDetail getParentNodeDetail() {
		return parentNodeDetail;
	}

	public int getIndex() {
		return index;
	}

	public int getTotalRows() {
		return totalRows;
	}
	public long getTotalBytes() {
		return totalBytes;
	}
	public int getAverageRows() {
		return averageRows;
	}
	public int getPeakRows() {
		return peakRows;
	}
	public int getAverageBytes() {
		return averageBytes;
	}
	public int getPeakBytes() {
		return peakBytes;
	}
	public int getWaitingRows() {
		return waitingRows;
	}
	public int getAverageWaitingRows() {
		return averageWaitingRows;
	}

	abstract public String getType();
	
	abstract void gatherTrackingDetails();
	
	protected void gatherTrackingDetails0(int newTotalRows, long newTotalBytes, int waitingRows) {
		long currentTime = System.nanoTime();
		long timespan = lastGatherTime != 0 ? currentTime - lastGatherTime : 0; 

    	if(timespan > MIN_TIMESLACE) { // for too small time slice are statistic values too distorted
    	    //averageRows
	        averageRows = (int) (((long) (newTotalRows - totalRows)) * 1000000000 / timespan);

	        //peakRows
	        peakRows = Math.max(peakRows, averageRows);

    	    //averageBytes
	        averageBytes = (int) (((long) (newTotalBytes - totalBytes)) * 1000000000 / timespan);

	        //peakBytes
	        peakBytes = Math.max(peakBytes, averageBytes);
	        
	    	lastGatherTime = currentTime;
    	} else {
    		if(lastGatherTime == 0) {
    	    	lastGatherTime = currentTime;
    		}
    	}
		
		//totalRows
	    totalRows = newTotalRows;

	    //totalBytes
	    totalBytes = newTotalBytes;
	    
    	//waitingRows
        this.waitingRows = waitingRows;
        
	    //averageWaitingRows
        averageWaitingRows = Math.abs(waitingRows - averageWaitingRows) / 2;
	}

	void phaseFinished() {
	    //averageRows
        averageRows = 0;

	    //averageBytes
        averageBytes = 0;
	}
	
}
