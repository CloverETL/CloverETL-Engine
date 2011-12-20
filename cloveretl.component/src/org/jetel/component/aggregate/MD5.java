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
package org.jetel.component.aggregate;

import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.crypto.Base64;

/**
 * Calculates MD5 on the aggregation group.
 * 
 * Output field type must be String and nullable if input is nullable.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class MD5 extends AggregateFunction {
	private static final String NAME = "md5";
	
	private int loopCount = 0;
	private MessageDigest md5;
	CloverBuffer dataBuffer = CloverBuffer.allocateDirect(Defaults.Record.INITIAL_FIELD_SIZE, Defaults.Record.FIELD_SIZE_LIMIT);
	private CharsetEncoder encoder;

	// Is input nullable?
	private boolean nullableInput;


	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkInputFieldType(DataFieldMetadata inputField) throws AggregationException {
		nullableInput = inputField.isNullable();
		return;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void  checkOutputFieldType(DataFieldMetadata outputField) throws AggregationException {
		if (nullableInput && !outputField.isNullable()) {
			throw new AggregationException(AggregateFunction.ERROR_NULLABLE_BECAUSE_INPUT);
		}
		if (outputField.getType() != DataFieldMetadata.STRING_FIELD) {
			throw new AggregationException(AggregateFunction.ERROR_STRING);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#init()
	 */
	@Override
	public void init() {
		try {
			md5 = MessageDigest.getInstance("MD5");
			md5.reset();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 algorithm is not available", e);
		}
		
        if(charset == null) {
            encoder = Charset.forName(Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER).newEncoder();
        } else {
            encoder = Charset.forName(charset).newEncoder();
        }
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#requiresInputField()
	 */
	@Override
	public boolean requiresInputField() {
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#storeResult(org.jetel.data.DataField)
	 */
	@Override
	public void storeResult(DataField outputField) {
		if (loopCount == 0) {
			outputField.setNull(true);
		} else {
			outputField.setValue(Base64.encodeBytes(md5.digest()));
		}
	}


	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#update(org.jetel.data.DataRecord)
	 */
	@Override
	public void update(DataRecord record) throws Exception {
		DataField input = record.getField(inputFieldIndex);
		if (input.isNull()) {
			return;
		}
		
		dataBuffer.clear();
		input.toByteBuffer(dataBuffer, encoder);
		dataBuffer.flip();
		md5.update(dataBuffer.buf());
		loopCount++;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#getName()
	 */
	@Override
	public String getName() {
		return NAME;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#clear()
	 */
	@Override
	public void clear() {
		md5.reset();
		loopCount = 0;
	}
}
