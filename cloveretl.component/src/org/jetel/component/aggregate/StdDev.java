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
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Calculates the standard deviation of an aggregation group. The standard deviation
 * is calculated in a way that is equivalent to the use of the formula:
 * sqrt(1/n * sum_through_x(sqr(x_i - avg(x)))).
 * 
 * Input field must be Numeric, output field must be Numeric and nullable.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 */
public class StdDev extends AggregateFunction {
	private static final String NAME = "stddev";
	
	// Mean
	private double mean = 0;
	// Sum of squared values
	private double sumSquared = 0;
	// Count of fields
	private int count = 0;


	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkInputFieldType(DataFieldMetadata inputField) throws AggregationException {
		if (!inputField.isNumeric()) {
			throw new AggregationException(AggregateFunction.ERROR_NUMERIC);
		}
		return;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkOutputFieldType(DataFieldMetadata outputField) throws AggregationException {
		if (!outputField.isNullable()) {
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
		if (count > 0) {
			outputField.setValue(new CloverDouble(calculateStdDev(mean, sumSquared, count)));
		} else {
			outputField.setNull(true);
		}
	}

	/**
	 * Calculates the standard deviation.
	 * @param expValue expected value (i.e. mean)
	 * @param sumSquared sum of squared values
	 * @param count count of values
	 * @return standard deviation
	 */
	private double calculateStdDev(double expValue, double sumSquared, int count) {
		return Math.sqrt((sumSquared) / count - expValue * expValue);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#update(org.jetel.data.DataRecord)
	 */
	@Override
	public void update(DataRecord record) {
		DataField inputField = record.getField(inputFieldIndex);
		if (inputField.isNull()) {
			return;
		}
		
		double input = ((Numeric) inputField).getDouble();
		
		count++;
		mean = (input - mean) / count + mean; 
		sumSquared += input * input;
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
		mean = 0;
		sumSquared = 0;
		count = 0;
	}
}
