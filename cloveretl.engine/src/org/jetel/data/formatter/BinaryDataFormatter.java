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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
 * This is a simple binary formatter which is used to store DataRecord objects into files
 * 
 * DataRecords are stored in their serialzed form
 * Uses java.nio channels and DataRecord.(de)serialize()
 * 
 * @author pnajvar
 * @since 9 Jan 2009
 *
 */
public class BinaryDataFormatter extends AbstractFormatter {

	private WritableByteChannel writer;
	private CloverBuffer buffer;
	private DataRecordMetadata metaData;
	private boolean useDirectBuffers = true;
	
	
	public BinaryDataFormatter() {
		
	}

	public BinaryDataFormatter(OutputStream outputStream) {
		setDataTarget(outputStream);
	}

	public BinaryDataFormatter(WritableByteChannel writableByteChannel) {
		setDataTarget(writableByteChannel);
	}

	public BinaryDataFormatter(File f) {
		setDataTarget(f);
	}
	
	@Override
	public void close() throws IOException {
		try {
			if ((writer != null) && writer.isOpen()) {
				try {
					flush();
				} finally {
					writer.close();
				}
			}
		} finally {
			buffer.clear();
		}
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
		int limitSize = Math.max(Defaults.Record.RECORD_LIMIT_SIZE, Defaults.Record.RECORDS_BUFFER_SIZE);
		buffer = CloverBuffer.allocate(Defaults.Record.RECORDS_BUFFER_SIZE, limitSize, useDirectBuffers);
 	}

	public DataRecordMetadata getMetadata() {
		return this.metaData;
	}
	
	@Override
	public void reset() {
		try {
			close();
		} catch (IOException ioe) {
			throw new JetelRuntimeException(ioe);
		}
	}

	@Override
	public void setDataTarget(Object outputDataTarget) {
		if (outputDataTarget instanceof File) {
			try {
				writer = Channels.newChannel(new FileOutputStream((File) outputDataTarget));
			} catch (FileNotFoundException e) {
				throw new JetelRuntimeException(e);
			}
		} else if (outputDataTarget instanceof OutputStream) {
			writer = Channels.newChannel((OutputStream)outputDataTarget);
		} else if (outputDataTarget instanceof WritableByteChannel) {
			writer = (WritableByteChannel) outputDataTarget;
		}
	}

	@Override
	public int write(DataRecord record) throws IOException {
		int recordSize = record.getSizeSerialized();
		int lengthSize = ByteBufferUtils.lengthEncoded(recordSize);
		if (buffer.remaining() < recordSize + lengthSize) {
			flush();
		}
        ByteBufferUtils.encodeLength(buffer, recordSize);
        record.serialize(buffer);
        
        return recordSize + lengthSize;
	}
	
	public int write(CloverBuffer record) throws IOException {
		int recordSize = record.remaining();
		int lengthSize = ByteBufferUtils.lengthEncoded(recordSize);
		if (buffer.remaining() < recordSize + lengthSize) {
			flush();
		}
        ByteBufferUtils.encodeLength(buffer, recordSize);
        buffer.put(record);
        
        return recordSize + lengthSize;
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

	public boolean isUseDirectBuffers() {
		return useDirectBuffers;
	}

	public void setUseDirectBuffers(boolean useDirectBuffers) {
		this.useDirectBuffers = useDirectBuffers;
	}

}
