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
package org.jetel.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.graph.InputPort;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Implementation of <code>java.nio.channels.ReadableByteChannel</code> for reading data from input port (thread-safe).
 * 
 * @author Martin Slama (martin.slama@javlin.eu) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created September 21, 2012
 */
public class InputPortReadableChannel implements ReadableByteChannel {

	private final InputPort inputPort;
	private final String fieldName;
	private final String charset;
	
	private boolean eof = false;
	private boolean opened;
	
	private CloverBuffer buffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
	private DataRecord record;
    
	/**
	 * Default constructor.
	 * 
	 * @param inputPort Input port - source of the data to be read.
	 * @param fieldName Name of the field to be read.
	 * @param charset Used charset.
	 * 
	 * @throws IOException 
	 */
    public InputPortReadableChannel(InputPort inputPort, String fieldName, String charset) throws IOException {
    	
    	if (inputPort == null || fieldName == null || charset == null) {
    		throw new IllegalArgumentException("inputPort, fieldName or charset is null"); //$NON-NLS-1$
    	}
    	if (!Charset.isSupported(charset)) {
    		throw new UnsupportedEncodingException("Charset " + charset + " is not supported"); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	
    	this.inputPort = inputPort;
    	this.fieldName = fieldName;
    	this.charset = charset;
    	this.opened = true;
    	
    	record = new DataRecord(inputPort.getMetadata());
    	record.init();
    	
    	//buffer should look like empty at the start of processing
    	buffer.flip();
    }
    
	@Override
	public synchronized boolean isOpen() {
		return opened;
	}
	
	@Override
	public synchronized void close() throws IOException {
		opened = false;
	}
	
	@Override
	public synchronized int read(ByteBuffer dst) throws IOException {
		if (!opened) {
			throw new ClosedChannelException();
		}
		
		readRecord();
		
		//do we have something to return?
		if (eof) {
			return -1;
		}
		
		int bufferRemaining = buffer.remaining();
		int dstRemaining = dst.remaining();
		
		//is the internal buffer bigger than the destination buffer?
		if (bufferRemaining > dstRemaining) {
			//persist the current limit
			int bufferLimit = buffer.limit();
			
			//set limit of the buffer to fit to destination buffer
			buffer.limit(buffer.position() + dstRemaining);

			//fill given buffer
			dst.put(buffer.buf());
			
			//set the limit back to former position
			buffer.limit(bufferLimit);
			
			return dstRemaining;
		} else {
			//fill given buffer
			dst.put(buffer.buf());
			
			return bufferRemaining;
		}
		
	}
	
	/**
	 * @return True if and only if there are no more data to be read from input port, false otherwise.
	 * @throws IOException 
	 */
	public synchronized boolean isEOF() throws IOException {
		if (eof) {
			return true;
		} else {
			//look ahead if the data are available
			readRecord();
			return eof;
		}
	}
	
	/**
	 * Read data record from input port to internal buffer
	 * 
	 * @throws IOException
	 */
	private void readRecord() throws IOException {
		if (!eof && buffer.remaining() == 0) {
			buffer.clear();
			
			try {
				record = inputPort.readRecord(record);
			} catch (InterruptedException e) {
				throw new IOException("Failed to read record from input port.", e); //$NON-NLS-1$
			}
			
			if (record != null) {
				//record was read
				DataField field = record.getField(fieldName);
				Object value = field.getValue();
				if (value != null) {
					//some value read
					buffer.put(value instanceof byte[] ? (byte[]) value : value.toString().getBytes(charset));
				} else {
					eof = true;
				}
			} else {
				//eof reached
				eof = true;
			}
	    	buffer.flip();
		}
	}
	
}
