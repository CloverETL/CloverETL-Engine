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
import java.nio.ByteBuffer;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Simple wrapper around common direct output port. Only additional value is synchronized
 * record writing. It is highly recommended to use writeRecordDirect instead of non-direct version -
 * the synchronization block is significantly shorter if the record serialization is done 
 * out of this output port.
 *  
 * @author Martin Zatopek (martin.zatopek@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 19.8.2009
 */
public class ConcurrentOutputPort implements OutputPortDirect {

	private OutputPortDirect outputPort;
	
	public ConcurrentOutputPort(OutputPortDirect outputPort) {
		this.outputPort = outputPort;
	}

	@Override
	public synchronized void close() throws InterruptedException, IOException {
		outputPort.close();
	}

	@Override
	public void connectWriter(Node _writer, int portNum) {
		outputPort.connectWriter(_writer, portNum);
	}

	@Override
	public synchronized void eof() throws InterruptedException, IOException {
		outputPort.eof();
	}

	@Override
	public synchronized long getByteCounter() {
		return outputPort.getByteCounter();
	}

	@Override
	public DataRecordMetadata getMetadata() {
		return outputPort.getMetadata();
	}

	@Override
	public synchronized long getOutputByteCounter() {
		return outputPort.getOutputByteCounter();
	}

	@Override
	public synchronized int getOutputPortNumber() {
		return outputPort.getOutputPortNumber();
	}

	@Override
	public synchronized long getOutputRecordCounter() {
		return outputPort.getOutputRecordCounter();
	}

	@Override
	public Node getReader() {
		return outputPort.getReader();
	}

	@Override
	public synchronized long getRecordCounter() {
		return outputPort.getRecordCounter();
	}

	@Override
	public synchronized void open() {
		outputPort.open();
	}

	@Override
	public void reset() throws ComponentNotReadyException {
		outputPort.reset();
	}

	@Override
	public synchronized void writeRecord(DataRecord record) throws IOException, InterruptedException {
		outputPort.writeRecord(record);
	}

	@Override
	public synchronized void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		outputPort.writeRecordDirect(record);
	}

	@Override
	@Deprecated
	public synchronized void writeRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public int getUsedMemory() {
		return outputPort.getUsedMemory();
	}

	@Override
	public Edge getEdge() {
		return outputPort.getEdge();
	}
	
}
