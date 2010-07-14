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
package org.jetel.util.bytes;

import java.io.ByteArrayOutputStream;

import org.jetel.data.Defaults;

/**
 * Support for the port writing.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 * (c) Javlin, a.s. (www.javlin.eu)
 */
public class RestrictedByteArrayOutputStream extends ByteArrayOutputStream {

	// max byte array size
	protected int maxArrayLength = Integer.MAX_VALUE;
	
	/**
	 * Creates ByteBufferOutputStream.
	 */
	public RestrictedByteArrayOutputStream() {
		super(Defaults.DataFormatter.FIELD_BUFFER_LENGTH);
	}
	
	/**
	 * Restricts max array length.
	 * @param maxArrayLength
	 */
	public void setMaxArrayLength(int maxArrayLength) {
		this.maxArrayLength = maxArrayLength;
	}
	
	/**
	 * Validates max byte array length.
	 */
	protected void validateBufferSize(int newSize) {
		if (newSize > maxArrayLength) {
    		throw new RuntimeException("The size of data buffer is only " + maxArrayLength + ". Set appropriate parameter in defautProperties file.");
		}
	}

	@Override
    public synchronized void write(byte b[], int off, int len) {
		validateBufferSize(count + len);
    	super.write(b, off, len);
    }

	@Override
    public synchronized void write(int b) {
		validateBufferSize(count + 1);
    	super.write(b);
	}

}

