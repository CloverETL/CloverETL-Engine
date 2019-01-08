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

import org.jetel.graph.InputPort;
import org.jetel.graph.runtime.jmx.InputPortTracking;
import org.jetel.graph.runtime.jmx.InputPortTrackingImpl;
import org.jetel.graph.runtime.jmx.NodeTracking;
import org.jetel.graph.runtime.jmx.PortTracking.PortType;

/**
 * This class represents tracking information about an input port.
 * 
 * @author Filip Reichman
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jan 2, 2019
 */
public class InputPortTrackingProvider extends AbstractPortTrackingProvider {
	
	private final InputPort inputPort;
	
	protected long readerWaitingTime;

	public InputPortTrackingProvider(NodeTrackingProvider parentNodeDetail, InputPort inputPort) {
		super(parentNodeDetail, inputPort.getInputPortNumber());
		this.inputPort = inputPort;
		
	}
	
	public InputPortTracking createSnaphot(NodeTracking parentNodeTracking) {
		return new InputPortTrackingImpl(parentNodeTracking, this);
	}

	public long getReaderWaitingTime() {
		return readerWaitingTime;
	}
	
	public void setReaderWaitingTime(long readerWaitingTime) {
		this.readerWaitingTime = readerWaitingTime;
	}
	
	public PortType getType() {
		return InputPortTracking.TYPE;
	}
	
	//******************* EVENTS ********************/
	@Override
	void gatherTrackingDetails() {
		gatherTrackingDetails0(
				inputPort.getInputRecordCounter(), 
				inputPort.getInputByteCounter(),
				(inputPort.getEdge()).getBufferedRecords());
		
		//gather memory usage
		usedMemory = inputPort.getUsedMemory();
		
		//aggregated time how long the reader thread waits for data
		setReaderWaitingTime(inputPort.getReaderWaitingTime());
		
		//define remote runId for remote edges
		if (inputPort.getEdge().isRemote()) {
			remoteRunId = inputPort.getEdge().getWriterRunId();
		}
	}

}
