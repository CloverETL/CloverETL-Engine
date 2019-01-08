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

import org.jetel.graph.OutputPort;
import org.jetel.graph.runtime.jmx.NodeTracking;
import org.jetel.graph.runtime.jmx.OutputPortTracking;
import org.jetel.graph.runtime.jmx.OutputPortTrackingImpl;
import org.jetel.graph.runtime.jmx.PortTracking.PortType;

/**
 * This class represents tracking information about an output port.
 * 
 * State of an instance is supposed to be changed over time
 * (it is used by WatchDog to gather information during an execution of graph).
 * 
 * @author Filip Reichman
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jan 2, 2019
 */
public class OutputPortTrackingProvider extends AbstractPortTrackingProvider {

	private final OutputPort outputPort;
	
	protected long writerWaitingTime;

	public OutputPortTrackingProvider(NodeTrackingProvider parentNodeDetail, OutputPort outputPort) {
		super(parentNodeDetail, outputPort.getOutputPortNumber());
		this.outputPort = outputPort;
	}
	
	public OutputPortTracking createSnaphot(NodeTracking parentNodeTracking) {
		return new OutputPortTrackingImpl(parentNodeTracking, this);
	}

	public long getWriterWaitingTime() {
		return writerWaitingTime;
	}

	public void setWriterWaitingTime(long writerWaitingTime) {
		this.writerWaitingTime = writerWaitingTime;
	}
	
	public PortType getType() {
		return OutputPortTracking.TYPE;
	}

	//******************* EVENTS ********************/
	@Override
	void gatherTrackingDetails() {
		gatherTrackingDetails0(
				outputPort.getOutputRecordCounter(), 
				outputPort.getOutputByteCounter(),
				(outputPort.getEdge()).getBufferedRecords());

		//gather memory usage
		usedMemory = outputPort.getUsedMemory();

		//aggregated time how long the writer thread waits for data
		setWriterWaitingTime(outputPort.getWriterWaitingTime());
		
		//define remote runId for remote edges
		if (outputPort.getEdge().isRemote()) {
			remoteRunId = outputPort.getEdge().getReaderRunId();
		}
	}

}
