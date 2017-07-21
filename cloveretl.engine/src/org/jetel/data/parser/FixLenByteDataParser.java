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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Parser for sequence of records represented by fixed count of bytes
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/14/06  
 */
public class FixLenByteDataParser extends FixLenDataParser {

	private int dataPos;
	private int dataLim;
	
	private int remaining; 
		
	/**
	 * Create instance for specified charset.
	 * @param charset
	 */
	public FixLenByteDataParser(DataRecordMetadata metadata, String charset) {
		super(metadata, charset);
	}

	public FixLenByteDataParser(TextParserConfiguration cfg) {
		super(cfg.getMetadata(), cfg.getCharset());
	}

	/**
	 * Create instance for default charset. 
	 */
	public FixLenByteDataParser(DataRecordMetadata metadata) {
		super(metadata, null);
	}

	/**
	 * Returns parser speed for specified configuration. See {@link TextParserFactory#getParser(TextParserConfiguration)}.
	 */
	public static Integer getParserSpeed(TextParserConfiguration cfg){
		for (DataFieldMetadata field : cfg.getMetadata().getFields()) {
			if (!field.isByteBased() && !field.isAutoFilled()) {
				logger.debug("Parser cannot be used for the specified data as they contain char-based field '" + field);
				return null;
			}
		}
		return 60;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.FixLenDataParser3#setDataSource(java.lang.Object)
	 */
	@Override
	public void setDataSource(Object inputDataSource) {
		super.setDataSource(inputDataSource);
		dataPos = 0;
		dataLim = 0;
	}

	/**
	 * Discard bytes for incremental reading.
	 * 
	 * @param bytes
	 * @throws IOException 
	 */
	@Override
	protected void discardBytes(int bytes) throws IOException {
		while (bytes > 0) {
			if (inChannel instanceof FileChannel) {
				((FileChannel)inChannel).position(bytes);
				return;
			}
			byteBuffer.clear();
			if (bytes < Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE) byteBuffer.limit(bytes);
			try {
				inChannel.read(byteBuffer.buf());
			} catch (IOException e) {
				break;
			}
			bytes =- Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE;
		}
		byteBuffer.clear();
		byteBuffer.flip();
	}
	
	/**
	 * Obtains raw data and tries to fill record fields with them.
	 * @param record Output record, cannot be null.
	 * @return null when no more data are available, output record otherwise.
	 * @throws JetelException
	 */
	@Override
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		if (getData(recordLength) != recordLength) {
			if (byteBuffer.remaining() != 0) {
				byteBuffer.position(byteBuffer.remaining());
				throw new BadDataFormatException("Incomplete record data");
			} else {
				return null;
			}			
		}

		int recStart = byteBuffer.position();
		for (fieldIdx = 0; fieldIdx < fieldCnt; fieldIdx++) {
			// skip all fields that are internally filled 
			if (isAutoFilling[fieldIdx]) {
				continue;
			}
			try {
				// set buffer scope to next field
				byteBuffer.position(recStart);	// to avoid exceptions while setting position&limit of the field 
				byteBuffer.limit(recStart + fieldEnd[fieldIdx]);
				byteBuffer.position(recStart + fieldStart[fieldIdx]);

				try {
					record.getField(fieldIdx).fromByteBuffer(byteBuffer, decoder);
				} catch (CharacterCodingException e) { // convert it to bad-format exception
					throw new BadDataFormatException(
							"Invalid characters in data field", byteBuffer.toString());
				}
			} catch (BadDataFormatException e) {
				fillXHandler(record, byteBuffer.toString(), e);
				return record;
			}
		}
		recordIdx++;
		return record;
	}

	/**
	 * Skip records.
	 * @param nRec Number of records to be skipped
	 * @return Number of successfully skipped records.
	 * @throws JetelException
	 */
	@Override
	public int skip(int nRec) throws JetelException {
		int skipped;
		for (skipped = 0; skipped < nRec; skipped++) {
			if (getData(recordLength) != recordLength) {	// end of file reached
				break;
			}
		}
		recordIdx += skipped;
		return skipped;
	}
		
	/**
	 * Reads raw data for one record from input and fills specified
	 * buffer with them. For outBuff==null raw data in input. 
	 * @param outBuf Output buffer to be filled with raw data.
	 * @return size of available data
	 * @throws JetelException
	 */
	private int getData(int dataLen) throws JetelException {
		if (eof) {	// no more data in input channel
			return 0;
		}

		// set buffer scope so that it will cover all unprocessed data
		byteBuffer.limit(dataLim);
		byteBuffer.position(dataPos);

		if (byteBuffer.remaining() < dataLen) {	// need to get more data from channel
			byteBuffer.compact();
			try {
				inChannel.read(byteBuffer.buf());				// write to buffer
				byteBuffer.flip();						// prepare buffer for reading
			} catch (IOException e) {
				throw new JetelException(e);
			}
			dataPos = 0;
			dataLim = byteBuffer.limit();
		}
		if (byteBuffer.remaining() < dataLen) {	// not enough data available
			eof = true;
			remaining = byteBuffer.remaining();
			dataPos += remaining;
        	bytesProcessed += remaining;
		} else {
			dataPos += dataLen;
        	bytesProcessed += dataLen;
		}
		// set scope for requested piece of data
		byteBuffer.limit(dataPos);
		return byteBuffer.remaining();
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#reset()
	 */
	@Override
	public void reset() {
		super.reset();
		dataPos = 0;
		dataLim = 0;
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
