/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-08  Javlin Consulting <info@javlin.eu>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
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
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;

/**
 * 
 * @author mvarecha (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5 May 2010
 */
public class ByteBufferFormatter implements Formatter {

	WritableByteChannel writer;
	ByteBuffer buffer;
	OutputStream backendOutputStream;
	DataRecordMetadata metaData;

	public ByteBufferFormatter() {
	}

	public ByteBufferFormatter(OutputStream outputStream) {
		setDataTarget(outputStream);
	}

	public void close() {
		if (writer != null && writer.isOpen()) {
			try {
				flush();
				writer.close();
				if (backendOutputStream != null) {
					backendOutputStream.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		buffer.clear();
	}

	public void finish() throws IOException {
		flush();
	}

	public void flush() throws IOException {
		buffer.flip();
		writer.write(buffer);
		buffer.clear();
	}

	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		this.metaData = _metadata;
		buffer = ByteBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
 	}

	public DataRecordMetadata getMetadata() {
		return this.metaData;
	}
	
	public void reset() {
		close();
	}

	public void setDataTarget(Object outputDataTarget) {
		if (outputDataTarget instanceof OutputStream) {
			backendOutputStream = (OutputStream) outputDataTarget;
			writer = Channels.newChannel(backendOutputStream);
		} else
			throw new IllegalArgumentException("OutputStream was expected");
	}

	public int write(DataRecord record) throws IOException {
		throw new UnsupportedOperationException("Cannot format DataRecord instances");
	}
	
	public int write(ByteBuffer record) throws IOException {
		int recordSize = record.remaining();
		int lengthSize = ByteBufferUtils.lengthEncoded(recordSize);
		if (recordSize + lengthSize > buffer.remaining()){
			flush();
		}
		if (buffer.remaining() < recordSize + lengthSize){
			throw new RuntimeException("The size of data buffer is only " + buffer.limit() + 
					", but record size is " + (recordSize + lengthSize) + ". Set appropriate parameter in defautProperties file.");
		}
		
		// store size of serialized record
        ByteBufferUtils.encodeLength(buffer, recordSize);
        
        // copy serialized record to the buffer
        buffer.put(record);
        
        return recordSize + lengthSize;
	}


	public int writeFooter() throws IOException {
		// no header
		return 0;
	}

	public int writeHeader() throws IOException {
		// no footer
		return 0;
	}

}
