package org.jetel.util.bytes;

import java.io.ByteArrayOutputStream;

import org.jetel.data.Defaults;

/**
 * Support for the port writing.
 * 
 * @author Jan Ausperger (jan.ausperger@javlin.eu)
 * (c) OpenSys (www.opensys.eu)
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

