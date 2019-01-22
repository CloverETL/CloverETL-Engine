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

import java.io.Serializable;

import org.jetel.graph.runtime.AbstractPortTrackingProvider;

/**
 * This abstract class represents common tracking information on a port.
 * 
 * @author Filip Reichman
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jan 2, 2019
 */
public abstract class AbstractPortTracking implements Serializable {

	private static final long serialVersionUID = -8999440507780259714L;

	private final NodeTracking parentNodeTracking;
	
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
	
    protected AbstractPortTracking(NodeTracking parentNodeTracking, int index) {
    	this.parentNodeTracking = parentNodeTracking;
    	this.index = index;
	}

    public void copyFrom(AbstractPortTrackingProvider portDetail) {
    	this.totalRecords = portDetail.getTotalRecords();
    	this.totalBytes = portDetail.getTotalBytes();
    	this.recordFlow = portDetail.getRecordFlow();
    	this.recordPeak = portDetail.getRecordPeak();
    	this.byteFlow = portDetail.getByteFlow();
    	this.bytePeak = portDetail.getBytePeak();
    	this.waitingRecords = portDetail.getWaitingRecords();
    	this.averageWaitingRecords = portDetail.getAverageWaitingRecords();
    	this.usedMemory = portDetail.getUsedMemory();
    	this.remoteRunId = portDetail.getRemoteRunId();
    }

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
	
	public NodeTracking getParentNodeTracking() {
		return parentNodeTracking;
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

	public void setRemoteRunId(long remoteRunId) {
		this.remoteRunId = remoteRunId;
	}
}
