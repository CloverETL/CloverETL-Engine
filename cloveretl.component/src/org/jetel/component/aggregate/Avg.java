/**
 * 
 */
package org.jetel.component.aggregate;

import java.util.HashMap;
import java.util.Map;

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
	public boolean checkInputFieldType(DataFieldMetadata inputField) {
		nullableInput = inputField.isNullable();
		return inputField.isNumeric();
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public boolean checkOutputFieldType(DataFieldMetadata outputField) {
		if (nullableInput && !outputField.isNullable()) {
			return false;
		}
		return outputField.isNumeric();
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
			count++;
		}
	}

	@Override
	public String getName() {
		return NAME;
	}
}
