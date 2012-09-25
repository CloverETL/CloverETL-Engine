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
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.ByteArray;
import org.jetel.graph.InputPort;

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
	
	private ByteArray buffer = new ByteArray();
	private DataRecord record;
    
	/**
	 * Default constructor.
	 * 
	 * @param inputPort Input port - source of the data to be read.
	 * @param fieldName Name of the field to be read.
	 * @param charset Used charset.
	 * 
	 * @throws UnsupportedEncodingException 
	 */
    public InputPortReadableChannel(InputPort inputPort, String fieldName, String charset) throws UnsupportedEncodingException {
    	
    	if (inputPort == null || fieldName == null || charset == null) {
    		throw new IllegalArgumentException("inputPort, fieldName or charset is null"); //$NON-NLS-1$
    	}
    	if (!Charset.isSupported(charset)) {
    		throw new UnsupportedEncodingException("Charset " + charset + " is not supported"); //$NON-NLS-1$ //$NON-NLS-2$
    	}
    	
    	this.inputPort = inputPort;
    	this.fieldName = fieldName;
    	this.charset = charset;
    	record = new DataRecord(inputPort.getMetadata());
    	record.init();
    }
    
    /**
     * Returns always true.
     * 
     * @see InputPortReadableChannel#isEOF()
     * @see InputPortReadableChannel#close()
     */
	@Override
	public synchronized boolean isOpen() {
		//channel is still opened
		return true;
	}
	
	/**
	 * Does nothing.
	 */
	@Override
	public synchronized void close() throws IOException {
		//nothing to do here
	}
	
	@Override
	public synchronized int read(ByteBuffer dst) throws IOException {
		//read data from input port to given buffer
		while (buffer.length() < dst.remaining()) {
			try {
				record = inputPort.readRecord(record);
			} catch (InterruptedException e) {
				throw new IOException("Failed to read record from input port.", e); //$NON-NLS-1$
			}
			if (record == null) {
				//eof reached
				eof = true;
				break;
			}
			DataField field = record.getField(fieldName);
			Object value = field.getValue();
			if (value != null) {
				//some value read
				buffer.append(value instanceof byte[] ? (byte[]) value : value.toString().getBytes(charset));
			} else {
				//there's null at input port -> stop reading (end of record)
				break;
			}
		}
		//count of read bytes
		int read = buffer.length() > dst.limit() ? dst.limit() : buffer.length();
		//fill given buffer
		dst.put(buffer.getValue(new byte[read], read));
		if (read > 0) {
			//clear internal buffer
			buffer = buffer.delete(0, read);
		}
		return read > 0 ? read : -1;
	}
	
	/**
	 * @return True if and only if there are no more data to be read from input port, false otherwise.
	 */
	public synchronized boolean isEOF() {
		return eof && buffer.length() == 0;
	}

}
