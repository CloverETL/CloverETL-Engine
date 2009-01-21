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
package org.jetel.data.parser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
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
	InputStream backendStream;
	DataRecordMetadata metaData;
	ByteBuffer buffer;
	IParserExceptionHandler exceptionHandler;
	int recordSize;
	boolean opened = false;
	int bufferLimit = -1;
	
	private final static int LEN_SIZE_SPECIFIER = 4;

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
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		buffer.clear();
	}

	public IParserExceptionHandler getExceptionHandler() {
		return this.exceptionHandler;
	}

	public DataRecord getNext() throws JetelException {
		DataRecord record = new DataRecord(metaData);
		record.init();
		return getNext(record);
	}

	long timeTotal;
	long timeReload1;
	long timeReload2;
	long timeDecode;
	long timeDeserialize;
	long _timeStart1;
	long _timeStart2;
	public DataRecord getNext(DataRecord record) throws JetelException {
		_timeStart1 = System.currentTimeMillis();
		try {
			if (!opened) {
				open();
			}
			_timeStart2 = System.currentTimeMillis();
			if (LEN_SIZE_SPECIFIER > buffer.remaining()) {
				reloadBuffer();
				if (buffer.remaining() == 0) {
					return null;
				}
			}
			timeReload1 += System.currentTimeMillis() - _timeStart2;
			
			_timeStart2 = System.currentTimeMillis();
			recordSize = ByteBufferUtils.decodeLength(buffer);
			timeDecode += System.currentTimeMillis() - _timeStart2;

			// check that internal buffer has enough data to read data record
			_timeStart2 = System.currentTimeMillis();
			if (recordSize > buffer.remaining()) {
				reloadBuffer();
				if (recordSize > buffer.remaining()) {
					return null;
				}
			}
			timeReload2 += System.currentTimeMillis() - _timeStart2;

			_timeStart2 = System.currentTimeMillis();
			record.deserialize(buffer);
			timeDeserialize += System.currentTimeMillis() - _timeStart2;
			
			timeTotal += System.currentTimeMillis() - _timeStart1;
			
			return record;
		} catch (IOException e) {
			throw new JetelException(e.getMessage());
		}

	}

	public void printStats() {
		System.out.println("BinaryDataParser stats [" + this.backendStream + "]");
		System.out.println("Total time: " + (timeTotal / 1000.0) + " s");
		System.out.println("  Reload 1st: " + (timeReload1 / 1000.0) + " s");
		System.out.println("  Decode: " + (timeDecode / 1000.0) + " s");
		System.out.println("  Reload 2nd: " + (timeReload2 / 1000.0) + " s");
		System.out.println("  Deserialize: " + (timeDeserialize / 1000.0) + " s");
	}
	
//	ByteBuffer[] buffers;
//	int curBuffer = 0;
//	int buffersCount = 2;
	void reloadBuffer() throws IOException {
//		curBuffer = (curBuffer+1) % buffersCount;
//		byte[] remainder = new byte[buffer.remaining()];
//		buffer.get(remainder);
//		buffer.clear();
//		buffer = buffers[curBuffer];
//		buffer.put(remainder);
		buffer.compact();
		reader.read(buffer);
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
// Multiple buffers in roundrobin
// disabled - complexity without any yield
//		buffers = new ByteBuffer[buffersCount];
//		for(int i = 0; i < buffersCount; i++) {
//			buffers[i] = ByteBuffer.allocateDirect(buffSize);
//		}
//		buffer = buffers[0];
		buffer = ByteBuffer.allocateDirect(buffSize);
//		buffer = ByteBuffer.allocate(buffSize);
	}

	public void movePosition(Object position) throws IOException {
	}

	public void reset() throws ComponentNotReadyException {
		buffer.clear();
		close();
		if (backendStream != null) {
			reader = Channels.newChannel(backendStream);
		}
		opened = false;
	}

	public void open() throws IOException {
		if (!opened && reader != null && reader.isOpen()) {
			buffer.clear();
			reader.read(buffer);
			buffer.flip();
			opened = true;
		}
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

}
