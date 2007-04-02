package org.jetel.component.aggregate;

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
	private double mean = 0;
	// Sum of squared values
	private double sumSquared = 0;
	// Count of fields
	private int count = 0;


	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkInputFieldType(DataFieldMetadata inputField) throws AggregateProcessorException {
		if (!inputField.isNumeric()) {
			throw new AggregateProcessorException(AggregateFunction.ERROR_NUMERIC);
		}
		return;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkOutputFieldType(DataFieldMetadata outputField) throws AggregateProcessorException {
		if (!outputField.isNullable()) {
			throw new AggregateProcessorException(AggregateFunction.ERROR_NULLABLE_BECAUSE_INPUT);
		}
		if (!outputField.isNumeric()){
			throw new AggregateProcessorException(AggregateFunction.ERROR_NUMERIC);
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
