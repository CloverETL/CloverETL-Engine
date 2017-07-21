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
package org.jetel.data.parser;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Parser of records from InputStream.
 * Records read from the Stream have simple format: 
 * - length of serialized record, must be decoded by ByteBufferUtils#decodeLength() method.
 * - serialized record, in common clover serialization format 
 * 
 *  TODO functionality of this class should be consolidated into {@link BinaryDataParser} class (???)
 *  
 * @author mvarecha (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 5 May 2010
 */
public class ByteBufferParser extends AbstractParser {

	ReadableByteChannel reader;
	/*
	 * just remember the inputstream we used to create "reader" channel
	 */
	InputStream backendStream;
	DataRecordMetadata metadata;
	CloverBuffer buffer;
	IParserExceptionHandler exceptionHandler;
	/*
	 * aux variable
	 */
	int recordSize;
	/*
	 * Size of read buffer
	 */
	int bufferLimit = -1;
	
	private final static int LEN_SIZE_SPECIFIER = 4;
	
	private boolean eofReached;

	public ByteBufferParser(DataRecordMetadata metadata) {
		this.metadata = metadata;
	}

	public ByteBufferParser(DataRecordMetadata metadata, int bufferLimit) {
		this.metadata = metadata;
		setBufferLimit(bufferLimit);
	}
	
	
	public int getBufferLimit() {
		return bufferLimit;
	}

	public void setBufferLimit(int bufferLimit) {
		this.bufferLimit = bufferLimit;
	}

	public ByteBufferParser(InputStream inputStream) {
		try {
			setDataSource(inputStream);
		} catch (ComponentNotReadyException e) {
			throw new JetelRuntimeException(e);
		}
	}

	public ByteBufferParser(InputStream inputStream, int bufferLimit) {
		this(inputStream);
		this.bufferLimit = bufferLimit;
	}

	public ByteBufferParser(File file) {
		try {
			setDataSource(file);
		} catch (ComponentNotReadyException e) {
			throw new JetelRuntimeException(e);
		}
	}

	@Override
	public void close() {
		if (reader != null && reader.isOpen()) {
			try {
				reader.close();
				if (backendStream != null) {
					backendStream.close();
				}
			} catch (IOException e) {
				throw new JetelRuntimeException(e);
			}
		}
		buffer.clear();
		buffer.limit(0);
	}

	@Override
	public IParserExceptionHandler getExceptionHandler() {
		return this.exceptionHandler;
	}


	@Override
	public DataRecord getNext() throws JetelException {
		throw new UnsupportedOperationException("Cannot deserialize to DataRecord");
	}

	@Override
	public DataRecord getNext(DataRecord record) throws JetelException {
		throw new UnsupportedOperationException("Cannot deserialize to DataRecord");
	}
	
	/**
	 * Reads next data record to specified ByteBuffer.
	 * Returns false if there is no other record to read.
	 * 
	 * @param recordBuffer
	 * @return
	 * @throws JetelException
	 */
	public boolean getNext(CloverBuffer recordBuffer) throws JetelException {
		try {
			if (LEN_SIZE_SPECIFIER > buffer.remaining()) {
				reloadBuffer(LEN_SIZE_SPECIFIER);
				if (buffer.remaining() == 0) {
					return false; //correct end of stream
				}
			}
			
			recordSize = ByteBufferUtils.decodeLength(buffer);

			// check that internal buffer has enough data to read data record
			if (recordSize > buffer.remaining()) {
				reloadBuffer(recordSize);
				if (recordSize > buffer.remaining()) {
					throw new JetelException("Invalid end of data stream.");
				}
			}

			int sourceLimit = buffer.limit();
			buffer.limit(buffer.position() + recordSize);
			
			recordBuffer.clear();
			recordBuffer.put(buffer);
			recordBuffer.flip();
			
			buffer.limit(sourceLimit);
			
			return true;
		} catch (IOException e) {
			throw new JetelException(e);
		} catch (BufferUnderflowException e) {
			throw new JetelException("Invalid end of stream.", e);
		}

	}

	/**
	 * @deprecated use {@link #getNext(CloverBuffer)} instead
	 */
	@Deprecated
	public boolean getNext(ByteBuffer recordBuffer) throws JetelException {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(recordBuffer);
		boolean result = getNext(wrappedBuffer);
		if (wrappedBuffer.buf() != recordBuffer) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
		return result;
	}

	private void reloadBuffer(int requiredSize) throws IOException {
		if (eofReached) {
			return;
		}
		int size;
		buffer.compact();
		//we have to ensure that the buffer is big enough to bear 'requiredSize' bytes
		if (buffer.capacity() < requiredSize) {
			buffer.expand(0, requiredSize);
		}
		do {
			size = reader.read(buffer.buf());
			if (buffer.position() >= requiredSize) {
				break;
			}
			//data are not available, so let the other thread work now, we need to wait for a while
			//unfortunately, the read() method is non-blocking and easily returns no bytes
			Thread.yield();
		} while (size != -1);

		if (size == -1) {
			eofReached = true;
		}
		buffer.flip();
	}

	@Override
	public PolicyType getPolicyType() {
		return null;
	}

	@Override
	public Object getPosition() {
		return null;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata cannot be null");
		}
		int buffSize = bufferLimit > 0 ? Math.min(Defaults.Record.RECORDS_BUFFER_SIZE, bufferLimit)
				: Defaults.Record.RECORDS_BUFFER_SIZE;
		buffer = CloverBuffer.allocateDirect(buffSize);
		buffer.clear();
		buffer.limit(0);

		eofReached = false;
	}

	public DataRecordMetadata getMetadata() {
		return this.metadata;
	}

	@Override
	public void movePosition(Object position) throws IOException {
	}

	@Override
	public void reset() throws ComponentNotReadyException {
		buffer.clear();
		buffer.limit(0);
		close();
		if (backendStream != null) {
			reader = Channels.newChannel(backendStream);
		}
		
		eofReached = false;
	}

	@Override
	public void setDataSource(Object inputDataSource) throws ComponentNotReadyException {
		if (inputDataSource instanceof InputStream) {
			backendStream = (InputStream) inputDataSource;
			reader = Channels.newChannel(backendStream);
		} else 
			throw new IllegalArgumentException("InputStream was expected");
	}

	@Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
		this.exceptionHandler = handler;
	}

	@Override
	public void setReleaseDataSource(boolean releaseInputSource) {
	}

	@Override
	public int skip(int rec) throws JetelException {
		return 0;
	}

	public InputStream getBackendStream() {
		return backendStream;
	}

	@Override
    public void preExecute() throws ComponentNotReadyException {
    }
    
	@Override
    public void postExecute() throws ComponentNotReadyException {    	
    	reset();
    }
    
	@Override
    public void free() {
    	close();
    }

	@Override
	public boolean nextL3Source() {
		return false;
	}
	
}
