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

import java.util.HashSet;
import java.util.Set;

import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldMetadata;

/**
 * Calculates the count of members of an aggregate group with a unique value of a field.
 * 
 * Requires an input field of any type. Output field must be Numeric.
 * 
 * @author Jaroslav Urban (jaroslav.urban@javlinconsulting.cz)
 *         (c) Javlin Consulting (www.javlinconsulting.cz)
 *
 * @created 16.5.2007
 */
public class CountUnique extends AggregateFunction {
	private static final String NAME = "countunique";
	
	private Set<DataField> uniqueValues = new HashSet<DataField>();
	
	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkInputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */

	@Override
	public void checkInputFieldType(DataFieldMetadata inputField)
			throws AggregationException {
		return;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#checkOutputFieldType(org.jetel.metadata.DataFieldMetadata)
	 */

	@Override
	public void checkOutputFieldType(DataFieldMetadata outputField)
			throws AggregationException {
		if (!outputField.isNumeric()) {
			throw new AggregationException(AggregateFunction.ERROR_NUMERIC);
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#clear()
	 */

	@Override
	public void clear() {
		uniqueValues.clear();
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
		outputField.setValue(uniqueValues.size());
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.aggregate.AggregateFunction#update(org.jetel.data.DataRecord)
	 */

	@Override
	public void update(DataRecord record) throws Exception {
		DataField input = record.getField(inputFieldIndex);
		if (input.isNull()) {
			// must do this, because two null fields are not equall
			uniqueValues.add(null);
		} else {
			uniqueValues.add(input.duplicate());
		}
	}

}
