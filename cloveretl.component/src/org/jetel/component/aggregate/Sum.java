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

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Calculates the sum of field values.
 * 
 * Input and output field must be Numeric. If input field is nullable, then output field must
 * be nullable too.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class Sum extends AggregateFunction {
	private static final String NAME = "sum";

	// Sum
	private Numeric sum;

	// Is input nullable?
	private boolean nullableInput;

	
	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkInputFieldType(DataFieldMetadata inputField) throws AggregationException {
		nullableInput = inputField.isNullable();
		if (!inputField.isNumeric()) {
			throw new AggregationException(AggregateFunction.ERROR_NUMERIC);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkOutputFieldType(DataFieldMetadata outputField) throws AggregationException {
		if (nullableInput && !outputField.isNullable()) {
			throw new AggregationException(AggregateFunction.ERROR_NULLABLE_BECAUSE_INPUT);
		}
		if (!outputField.isNumeric()){
			throw new AggregationException(AggregateFunction.ERROR_NUMERIC);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#init()
	 */
	@Override
	public void init() {
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
		if (sum == null) {
			outputField.setNull(true);
			return;
		}
		outputField.setValue(sum);
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#update(org.jetel.data.DataRecord)
	 */
	@Override
	public void update(DataRecord record) {
		Numeric input = (Numeric) record.getField(inputFieldIndex);
		if (input.isNull()) {
			return;
		}

		if (sum == null) {
			// Fix of CL-1509: Devise field type from output field metadata -> overflow could possibly be avoided
			// Fix of CL-1508: Factory creates overflow checking Numerics
			sum = (Numeric) AggregateNumericFactory.createDataField(outputFieldMetadata);
			sum.setValue(input);
		} else {
			sum.add(input);
		}
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
		sum = null;
	}
}
