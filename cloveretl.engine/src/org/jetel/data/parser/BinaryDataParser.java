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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;

/**
 * A simple class for retrieving records from binary files created by BinaryDataFormatter
 * 
 * Uses channels
 * 
 * @author pnajvar
 * 
 */
public class BinaryDataParser implements Parser {

	ReadableByteChannel reader;
	/*
	 * just remember the inputstream we used to create "reader" channel
	 */
	InputStream backendStream;
	DataRecordMetadata metaData;
	ByteBuffer buffer;
	IParserExceptionHandler exceptionHandler;
	/*
	 * aux variable
	 */
	int recordSize;
	/*
	 * Size of read buffer
	 */
	int bufferLimit = -1;
	/*
	 * Whether an attempt to delete file from the underlying should be made on close()
	 */
	File deleteOnClose;
	
	boolean useDirectBuffers = true;
	
	private final static int LEN_SIZE_SPECIFIER = 4;
	
	private boolean eofReached;

	public BinaryDataParser() {

	}

	public BinaryDataParser(int bufferLimit) {
		setBufferLimit(bufferLimit);
	}
	
	
	public int getBufferLimit() {
		return bufferLimit;
	}

	public void setBufferLimit(int bufferLimit) {
		this.bufferLimit = bufferLimit;
	}

	public BinaryDataParser(InputStream inputStream) {
		try {
			setDataSource(inputStream);
		} catch (ComponentNotReadyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public BinaryDataParser(InputStream inputStream, int bufferLimit) {
		this(inputStream);
		this.bufferLimit = bufferLimit;
	}

	public BinaryDataParser(File file) {
		try {
			setDataSource(file);
		} catch (ComponentNotReadyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void close() {
		if (reader != null && reader.isOpen()) {
			try {
				reader.close();
				if (backendStream != null) {
					backendStream.close();
				}
				if (deleteOnClose != null) {
					deleteOnClose.delete();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		buffer.clear();
		buffer.limit(0);
	}

	public IParserExceptionHandler getExceptionHandler() {
		return this.exceptionHandler;
	}

	public DataRecord getNext() throws JetelException {
		DataRecord record = new DataRecord(metaData);
		record.init();
		return getNext(record);
	}

	public DataRecord getNext(DataRecord record) throws JetelException {
		try {
			if (LEN_SIZE_SPECIFIER > buffer.remaining()) {
				reloadBuffer(LEN_SIZE_SPECIFIER);
				if (buffer.remaining() == 0) {
					return null; //correct end of stream
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

			record.deserialize(buffer);
			
			return record;
		} catch (IOException e) {
			throw new JetelException(e.getMessage(), e);
		} catch (BufferUnderflowException e) {
			throw new JetelException("Invalid end of stream.", e);
		}

	}
	
	private void reloadBuffer(int requiredSize) throws IOException {
		if (eofReached) {
			return;
		}
		int size;
		buffer.compact();
		do {
			size = reader.read(buffer);
			if (buffer.position() > requiredSize) {
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

	public PolicyType getPolicyType() {
		return null;
	}

	public Object getPosition() {
		return null;
	}

	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		if (_metadata == null) {
			throw new ComponentNotReadyException("Metadata cannot be null");
		}
		this.metaData = _metadata;
		int buffSize = bufferLimit > 0 ? Math.min(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE, bufferLimit) : Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE;
		buffer = useDirectBuffers ? ByteBuffer.allocateDirect(buffSize) : ByteBuffer.allocate(buffSize);
//		buffer = ByteBuffer.allocate(buffSize); // for memory consumption testing
		buffer.clear();
		buffer.limit(0);

		eofReached = false;
	}

	public DataRecordMetadata getMetadata() {
		return this.metaData;
	}

	public void movePosition(Object position) throws IOException {
	}

	public void reset() throws ComponentNotReadyException {
		buffer.clear();
		buffer.limit(0);
		close();
		if (backendStream != null) {
			reader = Channels.newChannel(backendStream);
		}
		
		eofReached = false;
	}

	public void setDataSource(Object inputDataSource) throws ComponentNotReadyException {
		if (inputDataSource instanceof File) {
			try {
				backendStream = new FileInputStream((File) inputDataSource);
				reader = Channels.newChannel(backendStream);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (inputDataSource instanceof InputStream) {
			backendStream = (InputStream) inputDataSource;
			reader = Channels.newChannel(backendStream);
		}
	}

	public void setExceptionHandler(IParserExceptionHandler handler) {
		this.exceptionHandler = handler;
	}

	public void setReleaseDataSource(boolean releaseInputSource) {
		// TODO Auto-generated method stub

	}

	public int skip(int rec) throws JetelException {
		// TODO Auto-generated method stub
		return 0;
	}

	public InputStream getBackendStream() {
		return backendStream;
	}

	public File getDeleteOnClose() {
		return deleteOnClose;
	}

	public void setDeleteOnClose(File deleteOnClose) {
		this.deleteOnClose = deleteOnClose;
	}

	public boolean isUseDirectBuffers() {
		return useDirectBuffers;
	}

	public void setUseDirectBuffers(boolean useDirectBuffers) {
		this.useDirectBuffers = useDirectBuffers;
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
	
	
}
