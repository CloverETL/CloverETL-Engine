/**
 * 
 */
package org.jetel.component.aggregate;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.Numeric;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Calculates the average value of fields.
 * 
 * Input and output fields must be Numeric. If input field is nullable, then output field must
 * be nullable too.
 * 
 * @author Jaroslav Urban
 *
 */
public class Avg extends AggregateFunction {
	private static final String NAME = "AVG";
	
	// Sum of the fields 
	private Numeric sum;
	// Count of fields
	private int count = 0;

	// Is input nullable?
	private boolean nullableInput;

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkInputFieldType(DataFieldMetadata inputField) throws AggregateProcessorException {
		nullableInput = inputField.isNullable();
		if (!inputField.isNumeric()){
			throw new AggregateProcessorException(AggregateFunction.ERROR_NUMERIC);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkOutputFieldType(DataFieldMetadata outputField) throws AggregateProcessorException {
		if (nullableInput && !outputField.isNullable()) {
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
		if (sum == null) {
			outputField.setNull(true);
			return;
		}
		sum.div(new CloverInteger(count));
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
			sum = input.duplicateNumeric();
		} else {
			sum.add(input);
		}
		count++;
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
		count = 0;
	}
}
