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
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.StringDataField;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;

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
public class BinaryDataFormatter implements Formatter {

	WritableByteChannel writer;
	ByteBuffer buffer;
	OutputStream backendOutputStream;
	DataRecordMetadata metaData;
	/*
	 * Charset name used for encoding/decoding string fields
	 * If empty, no conversion is performed and strings are stored as 2-byte characters 
	 */
	String stringCharset;
	/*
	 * Instance of an encoder used for encoding strings into stringCharset
	 */
	CharsetEncoder stringEncoder;
	/*
	 * Instance of an decoder used for decoding string in stringCharset
	 */
	CharsetDecoder stringDecoder;	
	/*
	 * temporary buffer
	 */
	CharBuffer charBuffer;
	private boolean useDirectBuffers = true;
	
	
	public BinaryDataFormatter() {
		
	}

	public BinaryDataFormatter(OutputStream outputStream) {
		setDataTarget(outputStream);
	}

	public String getStringCharset() {
		return stringCharset;
	}

	public void setStringCharset(String stringCharset) {
		if (stringCharset != null && (! Defaults.Record.USE_FIELDS_NULL_INDICATORS || ! getMetadata().isNullable())) {
			this.stringCharset = stringCharset;
			stringEncoder = Charset.forName(stringCharset).newEncoder();
			charBuffer = CharBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
		} else {
			this.stringCharset = null;
			charBuffer = null;
			stringEncoder = null;
		}
	}

	public BinaryDataFormatter(File f) {
		setDataTarget(f);
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
		buffer = useDirectBuffers ? ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE) : ByteBuffer.allocate(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
 	}

	public void init(DataRecordMetadata _metadata, String charsetName) throws ComponentNotReadyException {
		init(_metadata);
		setStringCharset(charsetName);
	}
	
	
	public DataRecordMetadata getMetadata() {
		return this.metaData;
	}
	
	public void reset() {
		close();
	}

	public void setDataTarget(Object outputDataTarget) {
		if (outputDataTarget instanceof File) {
			try {
				backendOutputStream = new FileOutputStream((File) outputDataTarget); 
				writer = Channels.newChannel(backendOutputStream);
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else if (outputDataTarget instanceof OutputStream) {
			backendOutputStream = (OutputStream)outputDataTarget;
			writer = Channels.newChannel(backendOutputStream);
		}
	}

	public int write(DataRecord record) throws IOException {
		int recordSize = record.getSizeSerialized();
		int lengthSize = ByteBufferUtils.lengthEncoded(recordSize);
		if (buffer.remaining() < recordSize + lengthSize){
			flush();
		}
		if (buffer.remaining() < recordSize + lengthSize){
			throw new RuntimeException("The size of data buffer is only " + buffer.limit() + 
					", but record size is " + (recordSize + lengthSize) + ". Set appropriate parameter in defautProperties file.");
		}
        ByteBufferUtils.encodeLength(buffer, recordSize);
		if (stringCharset != null) {
			serialize(record, buffer);
		} else {
			record.serialize(buffer);
		}
        
        return recordSize + lengthSize;
	}


	
	public void serialize(DataRecord record, ByteBuffer buffer) {
		int numFields = record.getNumFields();
		DataField field;
		for (int i = 0; i < numFields; i++) {
			field = record.getField(i);
			if (field instanceof StringDataField) {
				serialize((StringDataField) field, buffer);
			} else {
				field.serialize(buffer);
			}
		}
	}
	
	public void serialize(StringDataField field, ByteBuffer buffer) {
		final int length = ((StringBuilder)field.getValue()).length();

	    final int byteLength = (int) (stringEncoder.averageBytesPerChar() * (float)length);
	    int realByteLength;
		try {
			int startingPos = buffer.position();
			// lets be optimistic and encode the estimated
			// length of encoded data
			// and assume, that in most cases we at least guess the 1-byte or 2-byte encoded length
			ByteBufferUtils.encodeLength(buffer, byteLength);

			int textPos = buffer.position();
//			synchronized(utfEncoder) {
			charBuffer.clear();
			charBuffer.append((StringBuilder)field.getValue());
			charBuffer.flip();
			stringEncoder.encode(charBuffer, buffer, true);
			realByteLength = buffer.position() - textPos;

			// bad case - but shouldn't happen often
			// bad guess on the edge of 1-byte and 2-byte border
			if ((byteLength <= 255 && realByteLength > 255) 
				||
				(byteLength > 255 && realByteLength <= 255)) {
				// bad bad
				// we have to move data one byte forward or backwards (which is not important)
				byte[] bytes = new byte[realByteLength];
				buffer.get(bytes, textPos, realByteLength);
				buffer.position(startingPos);
				ByteBufferUtils.encodeLength(buffer, realByteLength);
				buffer.put(bytes);
			} else if (byteLength != realByteLength){
				// no need to move data
				// just correct real bytes length
				int currentPos = buffer.position();
				buffer.position(startingPos);
				ByteBufferUtils.encodeLength(buffer, realByteLength);
				buffer.position(currentPos);
			}
			
    	} catch (BufferOverflowException e) {
    		throw new RuntimeException("The size of data buffer is only " + buffer.limit() + ". Set appropriate parameter in defautProperties file.", e);
    	}
	}

	
	public int writeFooter() throws IOException {
		// no header
		return 0;
	}

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
