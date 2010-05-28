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
public class ConcurrentOutputPort implements OutputPort, OutputPortDirect {

	private OutputPortDirect outputPort;
	
	public ConcurrentOutputPort(OutputPortDirect outputPort) {
		this.outputPort = outputPort;
	}

	public void close() throws InterruptedException, IOException {
		outputPort.close();
	}

	public void connectWriter(Node _writer, int portNum) {
		outputPort.connectWriter(_writer, portNum);
	}

	public void eof() throws InterruptedException, IOException {
		outputPort.eof();
	}

	public long getByteCounter() {
		return outputPort.getByteCounter();
	}

	public DataRecordMetadata getMetadata() {
		return outputPort.getMetadata();
	}

	public long getOutputByteCounter() {
		return outputPort.getOutputByteCounter();
	}

	public int getOutputPortNumber() {
		return outputPort.getOutputPortNumber();
	}

	public int getOutputRecordCounter() {
		return outputPort.getOutputRecordCounter();
	}

	public Node getReader() {
		return outputPort.getReader();
	}

	public int getRecordCounter() {
		return outputPort.getRecordCounter();
	}

	public void open() {
		outputPort.open();
	}

	public void reset() throws ComponentNotReadyException {
		outputPort.reset();
	}

	public synchronized void writeRecord(DataRecord record) throws IOException, InterruptedException {
		outputPort.writeRecord(record);
	}

	public synchronized void writeRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
		outputPort.writeRecordDirect(record);
	}
	
}
