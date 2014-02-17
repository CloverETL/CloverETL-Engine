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
import java.io.OutputStream;

import org.jetel.data.DataRecord;
import org.jetel.data.formatter.BinaryDataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.Edge;
import org.jetel.graph.EdgeBase;
import org.jetel.graph.runtime.IAuthorityProxy;
import org.jetel.util.bytes.CloverBuffer;

/**
 * This edge represents the "left" side of a remote edge, used by graph running splitted on cluster.
 * An artificial consumer {@link RemoteEdgeDataTransmitter} is attached to ensure parallel initialisation
 * of IO streams for remote data transmission. The data are not read by the consumer, but the data records
 * are send directly to the prepared output stream by the producer thread.
 * 
 * All incoming data are serialised by {@link BinaryDataFormatter} to an output stream
 * provided by {@link RemoteEdgeDataTransmitter}, which was created in pre-execution phase using
 * {@link IAuthorityProxy#getRemoteEdgeOutputStream(String)} method.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 8.11.2012
 */
public class LRemoteEdge extends EdgeBase {
	
	private BinaryDataFormatter dataFormatter;
	private long outputRecordsCounter;
	private long outputBytesCounter;
	private boolean eof;
    
	/**
     * Monitor for {@link #waitForEOF()}
     */
	private final Object eofMonitor = new Object();
	
	/**
	 * @param proxy
	 * @param index 
	 * @param remoteNodeId 
	 */
	public LRemoteEdge(Edge proxy) {
		super(proxy);
	}

	@Override
	public void init() throws IOException, InterruptedException {
		dataFormatter = new BinaryDataFormatter();
		try {
			dataFormatter.init(proxy.getMetadata());
		} catch (ComponentNotReadyException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public void preExecute() {
		super.preExecute();

		outputRecordsCounter = 0;
		outputBytesCounter = 0;
		synchronized (eofMonitor) {
			eof = false;
		}
	}

	@Override
	public void postExecute() {
		super.postExecute();
		
		//closing is performed on eof(), which is safer, because eof() is performed in execute() phase
		//which is interruptable, but postExecute() phase is not interruptable
		//
		//this close() invocation is blocking, waiting for close of opposite side of remote piped stream 
//		dataFormatter.close();
	}
	
	@Override
	public void writeRecord(DataRecord record) throws IOException, InterruptedException {
		outputBytesCounter += dataFormatter.write(record);
		outputRecordsCounter++;
	}

	@Override
	public void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		outputBytesCounter += dataFormatter.write(record);
		outputRecordsCounter++;
	}

	@Override
	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public long getOutputRecordCounter() {
		return outputRecordsCounter;
	}

	@Override
	public long getInputRecordCounter() {
		return outputRecordsCounter;
	}

	@Override
	public long getOutputByteCounter() {
		return outputBytesCounter;
	}

	@Override
	public long getInputByteCounter() {
		return outputBytesCounter;
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
    	synchronized (eofMonitor) {
    		eof = true;
    		dataFormatter.close();
    		eofMonitor.notifyAll();
    	}
	}

	@Override
	public boolean isEOF() {
		synchronized (eofMonitor) {
			return eof;
		}
	}

	@Override
	public void free() {
		
	}

	@Override
	public boolean hasData() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Sets the output stream where the incoming records are written.
	 * @param outputStream
	 * @see RemoteEdgeDataTransmitter#preExecute()
	 */
	public void setOutputStream(OutputStream outputStream) {
		dataFormatter.setDataTarget(outputStream);
	}

    @Override
    public void waitForEOF() throws InterruptedException {
    	synchronized (eofMonitor) {
    		while (!eof) {
    			eofMonitor.wait();
    		}
    	}
    }

}
