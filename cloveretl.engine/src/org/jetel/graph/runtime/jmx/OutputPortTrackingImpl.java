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

import org.jetel.graph.OutputPort;
import org.jetel.graph.runtime.OutputPortTrackingProvider;

/**
 * This class represents tracking information about an output port.
 * 
 * @author Filip Reichman
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jan 2, 2019
 */
public class OutputPortTrackingImpl extends AbstractPortTracking implements OutputPortTracking {

	private static final long serialVersionUID = 7091559190536591635L;
	
	private long writerWaitingTime;
	
	public OutputPortTrackingImpl(NodeTracking parentNodeDetail, int portIndex) {
		super(parentNodeDetail, portIndex);
	}
	
	public OutputPortTrackingImpl(NodeTracking parentNodeDetail, OutputPortTrackingProvider outputPortTracking) {
		super(parentNodeDetail, outputPortTracking.getIndex());
		super.copyFrom(outputPortTracking);

		this.writerWaitingTime = outputPortTracking.getWriterWaitingTime();
	}
	
	public OutputPortTrackingImpl(NodeTracking parentNodeTracking, OutputPort outputPort) {
		super(parentNodeTracking, outputPort.getOutputPortNumber());
	}

	@Override
	public long getWriterWaitingTime() {
		return writerWaitingTime;
	}

	public void setWriterWaitingTime(long writerWaitingTime) {
		this.writerWaitingTime = writerWaitingTime;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTrackingDetail#getType()
	 */
	@Override
	public PortType getType() {
		return OutputPortTracking.TYPE;
	}
}
