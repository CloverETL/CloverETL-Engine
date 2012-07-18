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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.StringUtils;

/**
 * Calculates message digest hash from binary representation of the data. Supports only byte and cbyte inputs.
 * 
 * Output field type must be String and nullable if input is nullable.
 * 
 * @author Michal Tomcanyi (michal.tomcanyi@javlin.eu)
 *         (c) Javlin a.s. (www.javlin.eu)
 */
public abstract class MessageDigestFunction extends AggregateFunction {
	private String aggregateFunctionName;
	private String messageDigestName;
	
	private int loopCount = 0;
	private MessageDigest digest;
	CloverBuffer dataBuffer = CloverBuffer.allocateDirect(Defaults.Record.FIELD_INITIAL_SIZE, Defaults.Record.FIELD_LIMIT_SIZE);

	// Is input nullable?
	private boolean nullableInput;

    public MessageDigestFunction(String aggregateFunctionName, String messageDigestName) {
    	this.aggregateFunctionName = aggregateFunctionName;
    	this.messageDigestName = messageDigestName;
    }
    
    // --- END OF ----------------------------------
	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkInputFieldType(DataFieldMetadata inputField) throws AggregationException {
		nullableInput = inputField.isNullable();
		switch (inputField.getDataType()) {
		case BYTE:
		case CBYTE:
			// data type ok
			break;
		default:
			throw new AggregationException(AggregateFunction.ERROR_BYTE);
		}
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
		if (outputField.getDataType() != DataFieldType.STRING) {
			throw new AggregationException(AggregateFunction.ERROR_STRING);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#init()
	 */
	@Override
	public void init() {
		try {
			digest = MessageDigest.getInstance(messageDigestName);
			digest.reset();
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("Underlying JVM does not provide " + messageDigestName + " message digest algorithm", e);
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
			outputField.setValue(StringUtils.bytesToHexString(digest.digest()));
		}
	}


	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#update(org.jetel.data.DataRecord)
	 */
	@Override
	public void update(DataRecord record) throws Exception {
		ByteDataField input = (ByteDataField)record.getField(inputFieldIndex);
		if (input.isNull()) {
			return;
		}
		
		digest.update(input.getValue());
		loopCount++;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#getName()
	 */
	@Override
	public String getName() {
		return aggregateFunctionName;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#clear()
	 */
	@Override
	public void clear() {
		digest.reset();
		loopCount = 0;
	}
}
