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
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.PolicyType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileUtils;

/**
 * A simple class for retrieving records from binary files created by BinaryDataFormatter
 * 
 * Uses channels
 * 
 * @author pnajvar
 * 
 */
public class BinaryDataParser extends AbstractParser {

	private ReadableByteChannel reader;
	/*
	 * just remember the inputstream we used to create "reader" channel
	 */
	private InputStream backendStream;
	private DataRecordMetadata metadata;
	private CloverBuffer buffer;
	private IParserExceptionHandler exceptionHandler;
	/*
	 * aux variable
	 */
	private int recordSize;
	/*
	 * Size of read buffer
	 */
	private int bufferLimit = -1;
	/*
	 * Whether an attempt to delete file from the underlying should be made on close()
	 */
	private File deleteOnClose;
	
	private boolean useDirectBuffers = true;
	
	private final static int LEN_SIZE_SPECIFIER = 4;
	
	private boolean eofReached;

	private long processedBytes;
	
	/** Which kind of data record deserialisation should be used? */
	private boolean unitaryDeserialization = false;

	public BinaryDataParser(DataRecordMetadata metadata) {
		this.metadata = metadata;
	}

	public BinaryDataParser(DataRecordMetadata metadata, int bufferLimit) {
		this.metadata = metadata;
		setBufferLimit(bufferLimit);
	}
	
	public BinaryDataParser(DataRecordMetadata metadata, InputStream inputStream) {
		this.metadata = metadata;
		setDataSource(inputStream);
	}

	public BinaryDataParser(DataRecordMetadata metadata, InputStream inputStream, int bufferLimit) {
		this(metadata, inputStream);
		this.bufferLimit = bufferLimit;
	}

	public BinaryDataParser(DataRecordMetadata metadata, File file) {
		this.metadata = metadata;
		setDataSource(file);
	}
	
	/**
	 * Sets the parser to deserialise the given data records using {@link DataRecord#deserializeUnitary(CloverBuffer)}
	 * method.
	 * Regular deserialisation is used by default.
	 */
	public void setUnitaryDeserialization(boolean unitaryDeserialization) {
		this.unitaryDeserialization = unitaryDeserialization;
	}
	
	/**
	 * @return true if unitary deserialisation of data records will be used
	 */
	public boolean getUnitaryDeserialization() {
		return unitaryDeserialization;
	}

	public int getBufferLimit() {
		return bufferLimit;
	}

	public void setBufferLimit(int bufferLimit) {
		this.bufferLimit = bufferLimit;
	}

	@Override
	public void close() {
		if (reader != null && reader.isOpen()) {
			try {
				doReleaseDataSource();
			} catch (IOException e) {
				throw new JetelRuntimeException(e);
			}
		} else {
			LogFactory.getLog(BinaryDataParser.class).debug("Reader is already closed when closing parser: " + reader);
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
		DataRecord record = DataRecordFactory.newRecord(metadata);
		return getNext(record);
	}

	@Override
	public DataRecord getNext(DataRecord record){
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
					throw new JetelRuntimeException("Invalid end of data stream.");
				}
			}

			if (unitaryDeserialization) {
				record.deserializeUnitary(buffer);
			} else {
				record.deserialize(buffer);
			}
			
			return record;
		} catch (IOException e) {
			throw new JetelRuntimeException(e);
		} catch (BufferUnderflowException e) {
			throw new JetelRuntimeException("Invalid end of stream.", e);
		}
	}
	
	public boolean getNext(CloverBuffer recordBuffer) {
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
					throw new JetelRuntimeException("Invalid end of data stream.");
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
			throw new JetelRuntimeException(e);
		} catch (BufferUnderflowException e) {
			throw new JetelRuntimeException("Invalid end of stream.", e);
		}
	}
	
	/**
	 * Reads an integer from current data source.
	 * @return read integer value
	 * @throws NoDataAvailableException if no more bytes are available in current data source 
	 */
	public int getNextInt() throws NoDataAvailableException {
		try {
			if (buffer.remaining() < 4) {
				reloadBuffer(4);
				if (buffer.remaining() < 4) {
					throw new NoDataAvailableException();
				}
			}
			
			return buffer.getInt();
		} catch (IOException e) {
			throw new JetelRuntimeException(e);
		}
	}
	
	private void reloadBuffer(int requiredSize) throws IOException {
		if (eofReached) {
			return;
		}
		int size;
		buffer.compact();
		//ensure that the buffer is big enough to bear 'requiredSize' bytes
		if (buffer.capacity() < requiredSize) {
			buffer.expand(0, requiredSize);
		}
		do {
			processedBytes += (size = reader.read(buffer.buf()));
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
	public Long getPosition() {
		return processedBytes;
	}

	@Override
	public void init() {
		if (metadata == null) {
			throw new JetelRuntimeException("Metadata cannot be null");
		}
		int buffSize = bufferLimit > 0 ? Math.min(Defaults.Record.RECORDS_BUFFER_SIZE, bufferLimit)
				: Defaults.Record.RECORDS_BUFFER_SIZE;
		int limitSize = Math.max(Defaults.Record.RECORDS_BUFFER_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		buffer = CloverBuffer.allocate(buffSize, limitSize, useDirectBuffers);
		buffer.clear();
		buffer.limit(0);

		eofReached = false;
		processedBytes = 0;
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
		processedBytes = 0;
	}
	
	private void doReleaseDataSource() throws IOException {
		if (reader != null) {
			FileUtils.closeAll(backendStream, reader);
			if (deleteOnClose != null) {
				if (!deleteOnClose.delete()) {
					LogFactory.getLog(BinaryDataParser.class).error("Failed to delete temp file: " + deleteOnClose.getAbsolutePath());
				} else {
					LogFactory.getLog(BinaryDataParser.class).debug("Temp file deleted: " + deleteOnClose.getAbsolutePath());
					deleteOnClose = null;
				}
			}
		}
	}

	@Override
	protected void releaseDataSource() {
		try {
			doReleaseDataSource();
		} catch (IOException ioe) {
			throw new JetelRuntimeException(ioe);
		}
	}

	@Override
	public void setDataSource(Object inputDataSource) {
		if (releaseDataSource) {
			releaseDataSource();
		}
		if (inputDataSource instanceof File) {
			try {
				backendStream = new FileInputStream((File) inputDataSource);
				reader = Channels.newChannel(backendStream);
			} catch (FileNotFoundException e) {
				throw new JetelRuntimeException(e);
			}
		} else if (inputDataSource instanceof InputStream) {
			backendStream = (InputStream) inputDataSource;
			reader = Channels.newChannel(backendStream);
		} else if (inputDataSource instanceof ReadableByteChannel) {
			reader = (ReadableByteChannel) inputDataSource;
		} else {
			throw new JetelRuntimeException("Unsupported data source type " + inputDataSource.getClass().getName());
		}
		
		eofReached = false;
		processedBytes = 0;
	}

	@Override
	public void setExceptionHandler(IParserExceptionHandler handler) {
		this.exceptionHandler = handler;
	}

	@Override
	public int skip(int rec) throws JetelException {
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

	@Override
	public boolean nextL3Source() {
		return false;
	}

	/**
	 * Is thrown if no more bytes are available in current data source for requested operation.
	 */
	public static class NoDataAvailableException extends Exception {
		private static final long serialVersionUID = -5435696416541293699L;
	}
	
}
