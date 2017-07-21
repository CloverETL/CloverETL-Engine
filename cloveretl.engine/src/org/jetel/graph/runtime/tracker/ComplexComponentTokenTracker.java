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
package org.jetel.graph.runtime.tracker;

import org.jetel.data.DataRecord;
import org.jetel.graph.Node;

/**
 * This token tracker is used in components by default.
 * 
 * Suitable for components creating N out-tokens for one in-token. All received
 * tokens are freed. All written tokens are linked to the last read token.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9 May 2012
 */
public class ComplexComponentTokenTracker extends BasicComponentTokenTracker {

	/**
	 * Index of port from which the last record was read.
	 */
	protected DataRecord lastReadDataRecord;
	
	/**
	 * Bits array containing 0 for input ports which already received EOF.
	 */
	private int activeInPorts;
	
	/**
	 * @param tokenTracker
	 * @param component
	 */
	public ComplexComponentTokenTracker(Node component) {
		super(component);

		activeInPorts = component.getInPorts().size();
	}

	@Override
	public void readToken(int portNum, DataRecord token) {
		super.readToken(portNum, token);
		
		if (lastReadDataRecord != null) {
			readTokenFree(lastReadDataRecord);
		}
		
		lastReadDataRecord = token.duplicate();
	}

	@Override
	public void writeToken(int portNum, DataRecord token) {
		writeTokenInit(token);
		if (lastReadDataRecord != null) {
			writeTokenAction(lastReadDataRecord, token);
		}
		
		super.writeToken(portNum, token);
	}

	@Override
	public void eofInputPort(int portNum) {
		activeInPorts--;
		
		// free last read token if necessary
		if (activeInPorts == 0 && lastReadDataRecord != null) {
			readTokenFreeEof(lastReadDataRecord);
		}
	}

	protected void readTokenFree(DataRecord token) {
		freeToken(token);
	}

	protected void readTokenFreeEof(DataRecord token) {
		freeToken(token);
	}

	protected void writeTokenInit(DataRecord token) {
		initToken(token);
	}
	
	protected void writeTokenAction(DataRecord lastReadToken, DataRecord writtenToken) {
		linkTokens(lastReadToken, writtenToken);
	}

}
