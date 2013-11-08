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
package org.jetel.graph;

import java.io.IOException;
import java.io.InputStream;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.BinaryDataParser;
import org.jetel.graph.DirectEdge;
import org.jetel.graph.Edge;
import org.jetel.graph.EdgeBase;
import org.jetel.util.bytes.CloverBuffer;

/**
 * NOTE: this edge type is not used for now
 * 
 * This class represents the "right" side of a remote edge, which is used in cluster run of graphs.
 * Functionality of this edge type is analogical to {@link DirectEdge}. No source component is expected.
 * Data record requested by consumer component are directly de-serialized from data stream and provided
 * to the target component.
 * 
 * This edge type is not used for now. For each remote edge except the phase edges
 * is used {@link RRemoteBufferedEdge} instead. Direct edge implementation
 * of remote edges {@link RRemoteDirectEdge} is not used to avoid deadlock, since cycles in cluster graphs
 * are not detected for now so each remote edge is buffered or phase edge just for sure.

 * @author Kokon (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8.11.2012
 */
public class RRemoteDirectEdge extends EdgeBase {

	private InputStream inputStream;
	private BinaryDataParser dataParser;
	private long inputRecordsCounter;
	private volatile boolean eofReached;

	/**
     * Monitor for {@link #waitForEOF()}
     */
	private final Object eofMonitor = new Object();

	/**
	 * @param proxy
	 * @param index 
	 * @param remoteNodeId 
	 */
	public RRemoteDirectEdge(Edge proxy) {
		super(proxy);
	}

	@Override
	public void init() throws IOException, InterruptedException {
		dataParser = new BinaryDataParser(proxy.getMetadata());
		dataParser.init();
	}

	@Override
	public void preExecute() {
		super.preExecute();

		inputRecordsCounter = 0;
		eofReached = false;
		
		inputStream = proxy.getGraph().getAuthorityProxy().getRemoteEdgeDataSource(proxy.getId()).getInputStream();
		
		dataParser.setDataSource(inputStream);
	}

	@Override
	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
		DataRecord result = dataParser.getNext(record);
		if (result == null) {
			dataParser.close();
			eofReached();
		} else {
			inputRecordsCounter++;
		}
		return result;
	}

	@Override
	public boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		boolean result = dataParser.getNext(record);
		if (result == false) {
			dataParser.close();
			eofReached();
		} else {
			inputRecordsCounter++;
		}
		return result;
	}

	@Override
	public void writeRecord(DataRecord record) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getOutputRecordCounter() {
		return 0;
	}

	@Override
	public long getInputRecordCounter() {
		return inputRecordsCounter;
	}

	@Override
	public long getOutputByteCounter() {
		return 0;
	}

	@Override
	public long getInputByteCounter() {
		return dataParser.getPosition();
	}

	@Override
	public int getBufferedRecords() {
		return 0;
	}

	@Override
	public int getUsedMemory() {
		return 0;
	}

	@Override
	public void eof() throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEOF() {
		return eofReached;
	}

	@Override
	public void free() {
		
	}

	@Override
	public boolean hasData() {
		throw new UnsupportedOperationException();
	}

    private void eofReached() {
    	synchronized (eofMonitor) {
    		eofReached = true;
    		eofMonitor.notifyAll();
    	}
    }
    
    @Override
    public void waitForEOF() throws InterruptedException {
    	synchronized (eofMonitor) {
    		while (!eofReached) {
    			eofMonitor.wait();
    		}
    	}
    }
	
}
