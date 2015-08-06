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
