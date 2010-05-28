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

import org.jetel.graph.Edge;
import org.jetel.graph.OutputPort;

/**
 * This class represents tracking information about an output port.
 * 
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created Jun 6, 2008
 */
public class OutputPortTrackingDetail extends PortTrackingDetail implements Serializable {

	private static final long serialVersionUID = 7091559190536591635L;
	
	private final transient OutputPort outputPort;
	
	public OutputPortTrackingDetail(NodeTrackingDetail parentNodeDetail, OutputPort outputPort) {
		super(parentNodeDetail, outputPort.getOutputPortNumber());
		this.outputPort = outputPort;
		
	}

	public OutputPortTrackingDetail(NodeTrackingDetail parentNodeDetail, int portNumber) {
		super(parentNodeDetail, portNumber);
		this.outputPort = null;
		
	}

	OutputPort getOutputPort() {
		return outputPort;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.runtime.jmx.PortTrackingDetail#getType()
	 */
	@Override
	public String getType() {
		// TODO we should have a constant for this
		return "Output";
	}

	//******************* EVENTS ********************/
	@Override
	public void gatherTrackingDetails() {
		gatherTrackingDetails0(
				outputPort.getOutputRecordCounter(), 
				outputPort.getOutputByteCounter(),
				((Edge) outputPort).getBufferedRecords());
	}
	
}
