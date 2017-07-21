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

import java.util.LinkedList;
import java.util.Queue;

import org.jetel.data.DataRecord;
import org.jetel.graph.Node;

/**
 * Token tracker for components reading N tokens for which single out-token is created.
 * All in-records are freed and linked with next written token.
 * 
 * @author tkramolis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18.5.2012
 */
public class DenormalizerComponentTokenTracker extends BasicComponentTokenTracker {

	private Queue<DataRecord> readTokens = new LinkedList<DataRecord>();
	private int cachedRecords;
	private int activeInPorts;
	
	
	public DenormalizerComponentTokenTracker(Node component) {
		this(component, 0);
	}

	/**
	 * @param component
	 * @param readRecordsCacheSize number of tokens cached by the component; 
	 * readRecordsCacheSize number of last read tokens will not be linked to next out-token.
	 * E.g. Denormalizer has to read N+1 records to determine that group of N records has ended,
	 * therefore it sets the readRecordsCacheSize to 1.
	 */
	public DenormalizerComponentTokenTracker(Node component, int readRecordsCacheSize) {
		super(component);
		if (readRecordsCacheSize < 0) {
			throw new IllegalArgumentException("Negative records cache size: " + readRecordsCacheSize);
		}
		cachedRecords = readRecordsCacheSize;
		activeInPorts = component.getInPorts().size();
	}
	
	@Override
	public void readToken(int inputPort, DataRecord token) {
		super.readToken(inputPort, token);
		readTokens.add(token.duplicate());
	}
	
	@Override
	public void writeToken(int outputPort, DataRecord token) {
		initToken(token);
		
		int tokensToLink = readTokens.size() - cachedRecords;
		for (int i = 0; i < tokensToLink; i++) {
			DataRecord readToken = readTokens.poll();
			linkTokens(readToken, token);
			freeToken(readToken);
		}
		
		super.writeToken(outputPort, token);
	}
	
	@Override
	public void eofInputPort(int portNum) {
		activeInPorts--;
		if (activeInPorts == 0) {
			cachedRecords = 0; // all tokens from readTokens will be linked in next (and last) writeToken
		}
	}
	
}
