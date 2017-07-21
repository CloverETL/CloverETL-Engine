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
 * Simple token tracker which frees all incoming tokens and initializes all outgoing tokens.
 * No relation between whichever tokens is created.
 * 
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18.5.2012
 */
public class ReaderWriterComponentTokenTracker extends BasicComponentTokenTracker {

	public ReaderWriterComponentTokenTracker(Node component) {
		super(component);
	}

	@Override
	public void readToken(int inputPort, DataRecord token) {
		super.readToken(inputPort, token);
		freeToken(token);
	}
	
	@Override
	public void writeToken(int outputPort, DataRecord token) {
		initToken(token);
		super.writeToken(outputPort, token);
	}
	
}
