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
 * Token tracker for components creating N out-tokens for each in-token. The 1st of the N tokens is 
 * unified with the last read input token (which is the only difference between this and {@link ComplexComponentTokenTracker}),
 * all other N-1 out-tokens are linked to the last read token.
 * 
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18.5.2012
 */
public class ReformatComponentTokenTracker extends ComplexComponentTokenTracker {

	protected boolean newToken = true;
	
	public ReformatComponentTokenTracker(Node component) {
		super(component);
	}

	@Override
	public void readToken(int portNum, DataRecord token) {
		newToken = true;
		super.readToken(portNum, token);
	}
	
	@Override
	protected void readTokenFree(DataRecord token) {
		// nothing
	}
	
	@Override
	protected void writeTokenInit(DataRecord token) {
		if (newToken && lastReadDataRecord != null) {
			// nothing
		} else {
			initToken(token);
		}
	}
	
	@Override
	protected void writeTokenAction(DataRecord lastReadToken, DataRecord writtenToken) {
		if (lastReadToken != null) {
			if (newToken) {
				unifyTokens(lastReadToken, writtenToken);
				newToken = false;
			} else {
				linkTokens(lastReadToken, writtenToken);
			}
		}
	}
	
	@Override
	protected void readTokenFreeEof(DataRecord token) {
		if (newToken) {
			freeToken(token);
		}
	}
}
