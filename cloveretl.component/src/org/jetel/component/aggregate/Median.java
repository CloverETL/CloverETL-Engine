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
/**
 * 
 */
package org.jetel.component.aggregate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Finds the median. In case there in an even number of values, and the type of the values is 
 * numeric, then the median is calculated as the mean of the 2 middle values. In case there in 
 * an even number of values, and the type of the values is not numeric, then the median is 
 * calculated as the first of the 2 middle values.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Nov 19, 2007
 */
public class Median extends AggregateFunction {
	private static final String NAME = "median";

	// number 2, used to calculate mean
	private static final Numeric TWO = new CloverInteger(2);

	private List<DataField> values = new ArrayList<DataField>();

	// Is input nullable?
	private boolean nullableInput;
	
	// Are the input values numbers?
	private boolean numericInput;

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkInputFieldType(DataFieldMetadata inputField)
			throws AggregationException {
		nullableInput = inputField.isNullable();
		return;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkOutputFieldType(DataFieldMetadata outputField)
			throws AggregationException {
		if (inputFieldMetadata == null) {
			return;
		}

		if (inputFieldMetadata.getType() != outputField.getType()) {
			throw new AggregationException(AggregateFunction.ERROR_OUTPUT_AS_INPUT);
		}
		if (nullableInput && !outputField.isNullable()) {
			throw new AggregationException(AggregateFunction.ERROR_NULLABLE_BECAUSE_INPUT);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#clear()
	 */
	@Override
	public void clear() {
		values.clear();
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#getName()
	 */
	@Override
	public String getName() {
		return NAME;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#init()
	 */
	@Override
	public void init() {
		// do nothing
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
		// handle special cases
		if (values.size() == 0) {
			outputField.setNull(true);
			return;
		}
		if (values.size() == 1) {
			outputField.setValue(values.get(0));
			return;
		}
		
		Collections.sort(values);

		// if there is an even number of values, then choose the value at the N/2 position as median.
		// if there is an odd number of values, choose the middle value as median
		boolean evenValues = values.size() % 2 == 0;
		int medianIndex = evenValues ? values.size() / 2 : (values.size() - 1) / 2 + 1;
		medianIndex--; // the list is indexed from 0
		
		// if the values are numbers, and the number of values is even, calculate the median 
		// as the mean of the 2 middle values.
		if (evenValues && numericInput) {
			Numeric firstValue = (Numeric)values.get(medianIndex);
			Numeric secondValue = (Numeric)values.get(medianIndex + 1);
			
			// calculate the mean
			firstValue.add(secondValue);
			firstValue.div(TWO);
			
			outputField.setValue(firstValue);
			return;
		} 
		
		outputField.setValue(values.get(medianIndex));
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#update(org.jetel.data.DataRecord)
	 */
	@Override
	public void update(DataRecord record) throws Exception {
		DataField input = record.getField(inputFieldIndex);
		numericInput = input.getMetadata().isNumeric();

		if (!input.isNull()) {
			values.add(input.duplicate());
		}
	}
}
