/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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

import java.io.IOException;
import java.nio.charset.CharacterCodingException;

import org.jetel.data.DataRecord;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.JetelException;

/**
 * Parser for sequence of records represented by fixed count of bytes
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/14/06  
 */
public class FixLenByteDataParser extends FixLenDataParser {

	private int dataPos;
	private int dataLim;
	
	/**
	 * Create instance for specified charset.
	 * @param charset
	 */
	public FixLenByteDataParser(String charset) {
		super(charset);
	}

	/**
	 * Create instance for default charset. 
	 */
	public FixLenByteDataParser() {
		super(null);
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.parser.FixLenDataParser3#setDataSource(java.lang.Object)
	 */
	public void setDataSource(Object inputDataSource) {
		super.setDataSource(inputDataSource);
		dataPos = 0;
		dataLim = 0;
	}

	/**
	 * Obtains raw data and tries to fill record fields with them.
	 * @param record Output record, cannot be null.
	 * @return null when no more data are available, output record otherwise.
	 * @throws JetelException
	 */
	protected DataRecord parseNext(DataRecord record) throws JetelException {
		if (getData(recordLength) != recordLength) {
			if (byteBuffer.remaining() != 0) {
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
				inChannel.read(byteBuffer);				// write to buffer
				byteBuffer.flip();						// prepare buffer for reading
			} catch (IOException e) {
				throw new JetelException(e.getMessage());
			}
			dataPos = 0;
			dataLim = byteBuffer.limit();
		}
		if (byteBuffer.remaining() < dataLen) {	// not enough data available
			eof = true;
			dataPos += byteBuffer.remaining();
		} else {
			dataPos += dataLen;
		}
		// set scope for requested piece of data
		byteBuffer.limit(dataPos);
		return byteBuffer.remaining();
	}

}
