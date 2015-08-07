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
package org.jetel.util.stream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.jetel.data.ByteDataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.graph.OutputPort;

/**
 * A utility {@link OutputStream} implementation that writes to a byte data field and emulates a writer writing in
 * streaming mode to the output port.
 * 
 * @author krivanekm (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 6. 8. 2015
 */
public class PortOutputStream extends OutputStream {

	private final ByteBuffer buffer = ByteBuffer.allocate(Defaults.PortReadingWriting.DATA_LENGTH);

	private final OutputPort port;
	private final DataRecord record;
	private final ByteDataField field;

	public PortOutputStream(OutputPort port, DataRecord record, int field) {
		this.port = port;
		this.record = record;
		this.field = (ByteDataField) record.getField(field);
	}

	@Override
	public void write(int b) throws IOException {
		buffer.put((byte) b);
		if (!buffer.hasRemaining()) {
			flush();
		}
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		while (len > 0) {
			int limit = Math.min(buffer.remaining(), len);
			buffer.put(b, off, limit);
			off += limit;
			len -= limit;
			if (!buffer.hasRemaining()) {
				flush();
			}
		}
	}

	@Override
	public void flush() throws IOException {
		if (buffer.position() > 0) {
			buffer.flip();
			field.setValue(buffer);
			try {
				port.writeRecord(record);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
			buffer.clear();
		}
	}

	@Override
	public void close() throws IOException {
		flush();
		field.setValue((Object) null);
		try {
			port.writeRecord(record);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

}
