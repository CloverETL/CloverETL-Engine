/**
 * 
 */
package org.jetel.component.aggregate;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Calculates the minimum value of a field.
 * 
 * Output field must be of the same type as the input field. If input field is nullable, then output field must
 * be nullable too.
 * 
 * @author Jaroslav Urban
 *
 */
public class Min extends AggregateFunction {
	private static final String NAME = "MIN";

	private DataField min;

	// Is input nullable?
	private boolean nullableInput;

	
	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkInputFieldType(DataFieldMetadata inputField) throws AggregateProcessorException{
		nullableInput = inputField.isNullable();
		return;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkOutputFieldType(DataFieldMetadata outputField) throws AggregateProcessorException {
		if (inputFieldMetadata.getType() != outputField.getType()) {
			throw new AggregateProcessorException(AggregateFunction.ERROR_OUTPUT_AS_INPUT);
		}
		if (nullableInput && !outputField.isNullable()) {
			throw new AggregateProcessorException(AggregateFunction.ERROR_NULLABLE_BECAUSE_INPUT);
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
		if (min == null) {
			outputField.setNull(true);
			return;
		}
		outputField.copyFrom(min);
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#update(org.jetel.data.DataRecord)
	 */
	@Override
	public void update(DataRecord record) {
		DataField input = record.getField(inputFieldIndex);
		if (input.isNull()) {
			return;
		}
		
		if ((min == null) || (input.compareTo(min) == -1)) {
			min = input.duplicate();
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
		min = null;
	}
}
