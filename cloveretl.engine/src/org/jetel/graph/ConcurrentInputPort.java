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
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 26.11.2012
 */
public class ConcurrentInputPort implements InputPortDirect {

	private InputPortDirect inputPort;
	
	public ConcurrentInputPort(InputPortDirect inputPort) {
		this.inputPort = inputPort;
	}
	
	@Override
	public void connectReader(Node _reader, int portNum) {
		inputPort.connectReader(_reader, portNum);
	}

	@Override
	public synchronized DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
		return inputPort.readRecord(record);
	}

	@Override
	@Deprecated
	public synchronized boolean isOpen() {
		return inputPort.isOpen();
	}

	@Override
	public synchronized boolean isEOF() {
		return inputPort.isEOF();
	}

	@Override
	public DataRecordMetadata getMetadata() {
		return inputPort.getMetadata();
	}

	@Override
	public Node getWriter() {
		return inputPort.getWriter();
	}

	@Override
	@Deprecated
	public synchronized long getRecordCounter() {
		return inputPort.getRecordCounter();
	}

	@Override
	public synchronized long getInputRecordCounter() {
		return inputPort.getInputRecordCounter();
	}

	@Override
	@Deprecated
	public synchronized long getByteCounter() {
		return inputPort.getByteCounter();
	}

	@Override
	public synchronized long getInputByteCounter() {
		return inputPort.getInputByteCounter();
	}

	@Override
	public synchronized boolean hasData() {
		return inputPort.hasData();
	}

	@Override
	public int getInputPortNumber() {
		return inputPort.getInputPortNumber();
	}

	@Override
	public void reset() throws ComponentNotReadyException {
		inputPort.reset();
	}

	@Override
	public int getUsedMemory() {
		return inputPort.getUsedMemory();
	}

	@Override
	public Edge getEdge() {
		return inputPort.getEdge();
	}

	@Override
	public synchronized boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
		return inputPort.readRecordDirect(record);
	}

	@Override
	@Deprecated
	public synchronized boolean readRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
		return inputPort.readRecordDirect(record);
	}

	@Override
	public long getReaderWaitingTime() {
		return inputPort.getReaderWaitingTime();
	}

}
