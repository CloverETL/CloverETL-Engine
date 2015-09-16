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
package org.jetel.data.formatter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 * TODO functionality of this class should be consolidated into {@link BinaryDataFormatter} class (???)
 * 
 * @author mvarecha (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 5 May 2010
 */
public class ByteBufferFormatter extends AbstractFormatter {

	WritableByteChannel writer;
	CloverBuffer buffer;
	OutputStream backendOutputStream;
	DataRecordMetadata metaData;

	public ByteBufferFormatter() {
	}

	public ByteBufferFormatter(OutputStream outputStream) {
		setDataTarget(outputStream);
	}

	@Override
	public void close() throws IOException {
		if (writer != null && writer.isOpen()) {
			try {
				flush();
			} finally {
				try {
					writer.close();
				} finally {
					if (backendOutputStream != null) {
						backendOutputStream.close();
					}
				}
			}
		}
		buffer.clear();
	}

	@Override
	public void finish() throws IOException {
		flush();
	}

	@Override
	public void flush() throws IOException {
		buffer.flip();
		writer.write(buffer.buf());
		buffer.clear();
	}

	@Override
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		this.metaData = _metadata;
		buffer = CloverBuffer.allocate(Defaults.Record.RECORDS_BUFFER_SIZE);
 	}

	public DataRecordMetadata getMetadata() {
		return this.metaData;
	}
	
	@Override
	public void reset() {
		try {
			close();
		} catch (IOException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public void setDataTarget(Object outputDataTarget) {
		if (outputDataTarget instanceof OutputStream) {
			backendOutputStream = (OutputStream) outputDataTarget;
			writer = Channels.newChannel(backendOutputStream);
		} else
			throw new IllegalArgumentException("OutputStream was expected");
	}

	@Override
	public int write(DataRecord record) throws IOException {
		throw new UnsupportedOperationException("Cannot format DataRecord instances");
	}
	
	public int write(CloverBuffer record) throws IOException {
		int recordSize = record.remaining();
		int lengthSize = ByteBufferUtils.lengthEncoded(recordSize);
		if (recordSize + lengthSize > buffer.remaining()) {
			flush();
		}
		
		// store size of serialized record
        ByteBufferUtils.encodeLength(buffer, recordSize);
        
        // copy serialized record to the buffer
        buffer.put(record);
        
        return recordSize + lengthSize;
	}

	/**
	 * @deprecated use {@link #write(CloverBuffer)} instead
	 */
	@Deprecated
	public int write(ByteBuffer record) throws IOException {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(record);
		int result = write(wrappedBuffer);
		if (wrappedBuffer.buf() != record) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
		return result;
	}

	@Override
	public int writeFooter() throws IOException {
		// no header
		return 0;
	}

	@Override
	public int writeHeader() throws IOException {
		// no footer
		return 0;
	}

}
