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

import java.io.InputStream;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.parser.BinaryDataParser.NoDataAvailableException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.CloverBuffer;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 28. 11. 2014
 */
public class CloverDebugParser extends CloverDataParser implements DebugParser {
	
	private DataRecordMetadata externalMetadata;
	private DataRecord tmpRecord = null;
	private DataRecord externalRecord = null;

	/**
	 * @param metadata
	 */
	public CloverDebugParser() {
		super(null);
	}
	
	private void readNextRecord() throws NoDataAvailableException {
		try {
			tmpRecord = super.getNext(tmpRecord);
		} catch (JetelException e) {
			throw new NoDataAvailableException();
		}
		if (tmpRecord == null) {
			throw new NoDataAvailableException();
		}
		tmpRecord.getField(0).getValue();
	}

	@Override
	public boolean getNext(CloverBuffer recordBuffer) {
		if (tmpRecord == null) {
			return false;
		}
		recordBuffer.clear();
		getNext(externalRecord);
		externalRecord.serialize(recordBuffer);
		recordBuffer.flip();
		return true;
	}

	@Override
	public DataRecordMetadata getMetadata() {
		return externalMetadata;
	}

	@Override
	public DataRecord getNext(DataRecord record) {
		if (tmpRecord == null) {
			return null;
		}
		for (int i = 0; i < record.getNumFields(); i++) {
			record.getField(i).setValue(tmpRecord.getField(i+1));
		}
		return record;
	}

	@Override
	public long getNextRecordNumber() throws NoDataAvailableException {
		readNextRecord();
		return (Long) tmpRecord.getField(0).getValue();
	}

	@Override
	public void setDataSource(Object in) throws ComponentNotReadyException {
		super.setDataSource(in);
		this.metadata = getVersion().metadata;
		this.externalMetadata = removeFirstField(metadata);
	}

	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		tmpRecord = DataRecordFactory.newRecord(metadata);
		externalRecord = DataRecordFactory.newRecord(externalMetadata);
	}
	
	private static DataRecordMetadata removeFirstField(DataRecordMetadata metadata) {
		DataRecordMetadata result = metadata.duplicate();
		result.delField(0);
		return result;
	}
	
	public static DataRecordMetadata getExternalMetadata(InputStream is) throws ComponentNotReadyException {
		DataRecordMetadata metadata = checkCompatibilityHeader(is, null).metadata;
		return removeFirstField(metadata);
	}

}
