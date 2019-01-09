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

import org.jetel.graph.InputPort;
import org.jetel.graph.runtime.InputPortTrackingProvider;

/**
 * Simple DTO holding tracking information about an input port.
 * 
 * @author Filip Reichman
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jan 2, 2019
 */
public class InputPortTrackingImpl extends AbstractPortTracking implements InputPortTracking, Serializable {

	private static final long serialVersionUID = 1796185855793703918L;
	
	protected long readerWaitingTime;
	
	public InputPortTrackingImpl(NodeTracking parentNodeTracking, int portIndex) {
		super(parentNodeTracking, portIndex);
	}
	
	public InputPortTrackingImpl(NodeTracking parentNodeTracking, InputPortTrackingProvider inputPortTracking) {
		super(parentNodeTracking, inputPortTracking.getIndex());
		super.copyFrom(inputPortTracking);

		this.readerWaitingTime = inputPortTracking.getReaderWaitingTime();
	}
	
	public InputPortTrackingImpl(NodeTracking parentNodeTracking, InputPort inputPort) {
		super(parentNodeTracking, inputPort.getInputPortNumber());
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
}
