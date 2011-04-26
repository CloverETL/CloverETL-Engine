package org.jetel.error;

import org.jetel.data.DataField;
import org.jetel.error.ErrorMappingElement.Error;

/**
 * The class provides support for copying an error message into a clover fields.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public abstract class CopyError {

	// error enum
	protected  Error error;
	
	// target data field
	protected DataField dataField;
	
	/**
	 * Creates copy object.
	 * 
	 * @param error - error enum
	 * @param dataField - target data field
	 */
	protected CopyError(Error error, DataField dataField) {
		this.error = error;
		this.dataField = dataField;
	}
	
	/**
	 * Gets error enum (ERR_MESSAGE, ERR_CODE).
	 * 
	 * @return
	 */
	public Error getError() {
		return error;
	}
	
	/**
	 * Sets value into target data field.
	 * 
	 * @param object
	 */
	public abstract void setValue(Object object);
}
