package org.jetel.component.aggregate;

import java.util.HashMap;
import java.util.Map;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Calculates the standard deviation of an aggregation group.
 * 
 * Input field must be Numeric, output field must be Numeric and nullable.
 * 
 * @author Jaroslav Urban
 *
 */
public class StdDev extends AggregateFunction {
	private static final String NAME = "STDDEV";
	
	// Mean
	private double mean;
	// Sum of squared values
	private double sumSquared;
	// Count of fields
	private int count;


	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public boolean checkInputFieldType(DataFieldMetadata inputField) {
		return inputField.isNumeric();
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public boolean checkOutputFieldType(DataFieldMetadata outputField) {
		return (outputField.isNumeric() && outputField.isNullable());
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
		if (count > 1) {
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
}
