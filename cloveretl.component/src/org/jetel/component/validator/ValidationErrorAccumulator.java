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
package org.jetel.component.validator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Accumulator class for validation errors.
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 20.11.2012
 * @see ValidationError
 * @see ValidationNode#isValid(org.jetel.data.DataRecord, ValidationErrorAccumulator, GraphWrapper)
 */
public class ValidationErrorAccumulator implements Iterable<ValidationError> {
	private List<ValidationError> errors = new ArrayList<ValidationError>();
	
	/**
	 * Add validation error 
	 * @param temp Validation error to be added
	 */
	public void addError(ValidationError temp) {
		errors.add(temp);
	}
	
	/**
	 * Return iterator for all errors
	 */
	@Override
	public Iterator<ValidationError> iterator() {
		return errors.iterator();
	}
	
	/**
	 * Removes all errors from this accumulator
	 */
	public void reset() {
		errors.clear();
	}
	
	/**
	 * @return True if there are no errors, false otherwise
	 */
	public boolean isEmpty() {
		return errors.isEmpty();
	}
}
