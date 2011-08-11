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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Finds the most most frequently occurring value.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @since Nov 14, 2007
 */
public class Modus extends AggregateFunction {
	private static String NAME = "modus";

	private Map<DataField, Count> counts = new LinkedHashMap<DataField, Count>();
	
	// Is input nullable?
	private boolean nullableInput;

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */
	@Override
	public void checkInputFieldType(DataFieldMetadata inputField)
			throws AggregationException {
		nullableInput = inputField.isNullable();
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
		counts.clear();
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
		int maxCount = 0;
		DataField mode = null;
		
		// find the value with most occurrences
		for (Entry<DataField, Count> countEntry : counts.entrySet()) {
			int count = countEntry.getValue().getValue();
			if (count > maxCount) {
				maxCount = count;
				mode = countEntry.getKey();
			}
		}
		
		if (mode == null) {
			outputField.setNull(true);
		} else {
			outputField.setValue(mode);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#update(org.jetel.data.DataRecord)
	 */
	@Override
	public void update(DataRecord record) throws Exception {
		DataField input = record.getField(inputFieldIndex);

		if (!input.isNull()) {
			Count count = counts.get(input);
			if (count == null) {
				counts.put(input.duplicate(), new Count());
			} else {
				count.increment();
			}
		}
	}

	/**
	 * Count, which can be incremented. Upon creating a new instance, the count value is
	 * set to 1.
	 * 
	 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
	 *         (c) Javlin Consulting (www.javlinconsulting.cz)
	 *
	 * @since Nov 14, 2007
	 */
	private static class Count {
		private int count = 1;
		
		/**
		 * @return value of the count.
		 */
		public int getValue() {
			return count;
		}
		
		/**
		 * Increments the count by 1.
		 */
		public void increment() {
			count++;
		}
	}
}
