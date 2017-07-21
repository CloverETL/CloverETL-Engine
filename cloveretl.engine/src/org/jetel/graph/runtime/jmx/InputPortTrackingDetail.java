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

import org.jetel.component.RemoteEdgeComponent;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;

/**
 * This class represents tracking information about an input port.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 6, 2008
 */
public class InputPortTrackingDetail extends PortTrackingDetail implements InputPortTracking, Serializable {

	private static final long serialVersionUID = 1796185855793703918L;
	
	private final transient InputPort inputPort;
	
	protected long readerWaitingTime;

	public InputPortTrackingDetail(NodeTrackingDetail parentNodeDetail, InputPort inputPort) {
		super(parentNodeDetail, inputPort.getInputPortNumber());
		this.inputPort = inputPort;
		
	}

	public InputPortTrackingDetail(NodeTrackingDetail parentNodeDetail, int portNumber) {
		super(parentNodeDetail, portNumber);
		inputPort = null;
	}

	public void copyFrom(InputPortTrackingDetail portDetail) {
		super.copyFrom(portDetail);

		this.readerWaitingTime = portDetail.readerWaitingTime;
	}
	
	InputPort getInputPort() {
		return inputPort;
	}

	@Override
	public long getReaderWaitingTime() {
		return readerWaitingTime;
	}
	
	public void setReaderWaitingTime(long readerWaitingTime) {
		this.readerWaitingTime = readerWaitingTime;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTrackingDetail#getType()
	 */
	@Override
	public PortType getType() {
		return InputPortTracking.TYPE;
	}
	
	//******************* EVENTS ********************/
	@Override
	public void gatherTrackingDetails() {
		gatherTrackingDetails0(
				inputPort.getInputRecordCounter(), 
				inputPort.getInputByteCounter(),
				(inputPort.getEdge()).getBufferedRecords());
		
		//gather memory usage
		setUsedMemory(inputPort.getUsedMemory());
		
		//aggregated time how long the reader thread waits for data
		setReaderWaitingTime(inputPort.getReaderWaitingTime());
		
		//define remote runId for remote edges
		Node dataProducent = inputPort.getWriter();
		if (dataProducent instanceof RemoteEdgeComponent) {
			remoteRunId = ((RemoteEdgeComponent) dataProducent).getRemoteRunId();
		}
	}

}
