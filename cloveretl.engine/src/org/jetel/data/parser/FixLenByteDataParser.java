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
public class FixLenByteDataParser extends FixLenDataParser3 {

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
		for (fieldIdx = 0; fieldIdx < fieldCnt; fieldIdx++) {
			try {
				// set buffer scope to next field
				if (!getNextField()) {
					if (fieldIdx == 0) {	// correct end of data
						return null;
					}
					// incomplete last record
					throw new BadDataFormatException("Incomplete record data");
				}
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
			if (!getData(recordLength)) {	// end of file reached
				break;
			}
		}
		recordIdx += skipped;
		return skipped;
	}
		
	public boolean getNextField() throws JetelException {
		if (!getData(fieldLengths[fieldIdx])) {
			return false;
		}
		return true;
	}

	/**
	 * Reads raw data for one record from input and fills specified
	 * buffer with them. For outBuff==null raw data in input. 
	 * @param outBuf Output buffer to be filled with raw data.
	 * @return false when no more data are available, true otherwise.
	 * @throws JetelException
	 */
	private boolean getData(int dataLen) throws JetelException {
		if (eof) {	// no more data in input channel
			return false;
		}

		// set buffer scope so that it will cover all unprocessed data
		byteBuffer.limit(dataLim);
		byteBuffer.position(dataPos);

		if (byteBuffer.remaining() < dataLen) {	// need to get more data from channel
			byteBuffer.compact();
			try {
				int size = inChannel.read(byteBuffer);	// write to buffer
				byteBuffer.flip();						// prepare buffer for reading
				if (size == -1 || byteBuffer.remaining() < dataLen) {	// not enough data available
					eof = true;
					return false;	// no more data available 
				}
			} catch (IOException e) {
				throw new JetelException(e.getMessage());
			}
			dataPos = 0;
			dataLim = byteBuffer.limit();
		}
		// move beginning of data scope
		dataPos += dataLen;
		// set scope for requested piece of data
		byteBuffer.limit(dataPos);
		return true;
	}

}
