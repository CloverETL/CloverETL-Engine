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
import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Multi-level parser. Parses multi-level input file. The file may contain records
 * of different types. The types are supposed to have fixed-length of records. This applies
 * for each type but the length associated with different types may differ.
 * User-specified type-selector decides which record type definition will
 * be used for each part of input.
 *   
 * @see TypeSelector
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 15/12/06  
 */
public class MultiLevelParser extends FixLenDataParser {

	private int dataPos;
	private int dataLim;

	/**
	 * Record description.
	 */
	private DataRecordMetadata[] metadata = null;
	
	private int[] fieldCnt;
	private int[][] fieldStart;
	private int[][] fieldEnd;
	private int[] recordLength;

	private int seltorDataLen;

	private TypeSelector seltor;
	
	private Properties properties;
	
	private DataRecord[] record;
	
	private int metaIdx;
	
	/**
	 * Create instance for specified charset.
	 * @param charset
	 */
	public MultiLevelParser(String charset, TypeSelector seltor, DataRecordMetadata[] metadata, Properties properties) {
		super(charset);
		this.metadata = metadata;
		this.seltor = seltor;
		this.properties = properties;
	}

	/**
	 * Create instance for default charset. 
	 */
	public MultiLevelParser(TypeSelector seltor, DataRecordMetadata[] metadata, Properties properties) {
		this(null, seltor, metadata, properties);
	}

	/**
	 * Obtains raw data and tries to fill record fields with them.
	 * @param record Output record, cannot be null.
	 * @return null when no more data are available, output record otherwise.
	 * @throws JetelException
	 */
	protected DataRecord parseNext(DataRecord unused) throws JetelException {
		if (unused != null) {	// make it clear that there's no connection between the parameter and return value
			throw new RuntimeException("Incorrect use of a method (program bug)");
		}

		showData(seltorDataLen);
		if (byteBuffer.remaining() == 0 && seltorDataLen != 0) {	// eof
			return null;
		}

		metaIdx = seltor.choose(byteBuffer, decoder);
		if (metaIdx < 0) {
			throw new BadDataFormatException("Unrecognizable data record type");
		}

		if (getData(recordLength[metaIdx]) != recordLength[metaIdx]) {
			if (byteBuffer.remaining() != 0) {
				throw new BadDataFormatException("Incomplete record data");
			}
			return null;
		}

		int recStart = byteBuffer.position();
		for (fieldIdx = 0; fieldIdx < fieldCnt[metaIdx]; fieldIdx++) {
			try {
				// set buffer scope to next field
				byteBuffer.position(recStart);	// to avoid exceptions while setting position&limit of the field 
				byteBuffer.limit(recStart + fieldEnd[metaIdx][fieldIdx]);
				byteBuffer.position(recStart + fieldStart[metaIdx][fieldIdx]);

				try {
					record[metaIdx].getField(fieldIdx).fromByteBuffer(byteBuffer, decoder);
				} catch (CharacterCodingException e) { // convert it to bad-format exception
					throw new BadDataFormatException(
							"Invalid characters in data field", byteBuffer.toString());
				}
			} catch (BadDataFormatException e) {
				fillXHandler(record[metaIdx], byteBuffer.toString(), e);
				return record[metaIdx];
			}
		}
		recordIdx++;
		seltor.presentRecord(record[metaIdx]);
		return record[metaIdx];
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
			showData(seltorDataLen);
			metaIdx = seltor.choose(byteBuffer, decoder);
			if (metaIdx < 0) {
				throw new BadDataFormatException("Unrecognizable data record type");
			}
			if (getData(recordLength[metaIdx]) != recordLength[metaIdx]) {	// end of file reached
				break;
			}
		}
		recordIdx += skipped;
		return skipped;
	}
		
	/**
	 * Sets input buffer scope without consuming the data. 
	 * @param dataLen
	 * @return
	 * @throws JetelException
	 */
	private int showData(int dataLen) throws JetelException {
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
		// set scope for requested piece of data
		if (byteBuffer.remaining() > dataLen) {
			byteBuffer.limit(dataPos + dataLen);
		}
		return byteBuffer.remaining();
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
		int retval = showData(dataLen);
		dataPos += retval;
		if (retval < dataLen) {	// not enough data available
			eof = true;
		}
		return retval;
	}

	/**
	 * Fill bad-format exception handler with relevant data.
	 * @param errorMessage
	 * @param record
	 * @param recordNumber
	 * @param fieldNumber
	 * @param offendingValue
	 * @param exception
	 */
	protected void fillXHandler(DataRecord record, CharSequence offendingValue,
        BadDataFormatException exception) {
		
		exception.setFieldNumber(fieldIdx);
		exception.setRecordNumber(recordIdx);
		
		if (exceptionHandler == null) { // no handler available
			throw new RuntimeException(exception.getMessage());			
		}
		// set handler
		exceptionHandler.populateHandler(exception.getMessage(), record, recordIdx - 1,
				fieldIdx, offendingValue.toString(), exception);
	}
		
	/**
	 * @throws ComponentNotReadyException
	 */
	public void init()	throws ComponentNotReadyException {
		seltorDataLen = seltor.init(metadata, properties);

		fieldCnt = new int[metadata.length];
		recordLength = new int[metadata.length];
		fieldStart = new int[metadata.length][];
		fieldEnd = new int[metadata.length][];
		record = new DataRecord[metadata.length];

		recordIdx = 0;
		fieldIdx = 0;

		for (metaIdx = 0; metaIdx < metadata.length; metaIdx++) {
			if (metadata[metaIdx].getRecType() != DataRecordMetadata.FIXEDLEN_RECORD) {
				throw new ComponentNotReadyException("Fixed length data format expected but not encountered");
			}
			fieldCnt[metaIdx] = metadata[metaIdx].getNumFields();
	
			recordLength[metaIdx] = metadata[metaIdx].getRecordSizeStripAutoFilling();
			isAutoFilling = new boolean[fieldCnt[metaIdx]];
			fieldStart[metaIdx] = new int[fieldCnt[metaIdx]];
			fieldEnd[metaIdx] = new int[fieldCnt[metaIdx]];
			int prevEnd = 0;
			for (int fieldIdx = 0; fieldIdx < metadata[metaIdx].getNumFields(); fieldIdx++) {
				if (isAutoFilling[fieldIdx] = metadata[metaIdx].getField(fieldIdx).getAutoFilling() != null) {
					fieldStart[metaIdx][fieldIdx] = prevEnd;
					fieldEnd[metaIdx][fieldIdx] = prevEnd;
				} else {
					fieldStart[metaIdx][fieldIdx] = prevEnd + metadata[metaIdx].getField(fieldIdx).getShift();
					fieldEnd[metaIdx][fieldIdx] = fieldStart[metaIdx][fieldIdx] + metadata[metaIdx].getField(fieldIdx).getSize();
					prevEnd = fieldEnd[metaIdx][fieldIdx];
					if (fieldStart[metaIdx][fieldIdx] < 0 || fieldEnd[metaIdx][fieldIdx] > recordLength[metaIdx]) {
						throw new ComponentNotReadyException("field boundaries cannot be outside record boundaries");
					}
				}
			}
			record[metaIdx] = new DataRecord(metadata[metaIdx]);
			record[metaIdx].init();
		}
		
		metaIdx = -1;
	}
	
	/**
	 * Release resources.
	 */
	public void close() {
		super.close();
		seltor.finish();
	}

	/**
	 * 
	 * Returns type (index to array of data record metadata) of last data record. 
	 * @return
	 */
	public int getTypeIdx() {
		return metaIdx;
	}

	public DataRecord getNext() throws JetelException {
		return getNext(null);
	}

	public void init(DataRecordMetadata unused) throws ComponentNotReadyException {
		/* in comparison with other parser implementations some methods are a little strange
		** since Parser interface doesn't fit our requirements very well.
		*/
		if (unused != null) {	// make it clear that there's no connection between the parameter and return value
			throw new RuntimeException("Incorrect use of a method (program bug)");
		}
		init();
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.data.parser.Parser#reset()
	 */
	public void reset() {
		super.reset();
	}

	@Override
	protected void discardBytes(int bytes) {
		// TODO Auto-generated method stub
	}
}
