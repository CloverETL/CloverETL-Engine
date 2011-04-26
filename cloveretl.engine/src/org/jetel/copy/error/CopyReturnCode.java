package org.jetel.copy.error;

import org.jetel.data.DataField;
import org.jetel.mapping.element.ReturnCodeMappingElement.RetCode;

/**
 * The class provides support for copying an return code into a clover fields.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public abstract class CopyReturnCode {

	// retCode enum
	protected RetCode retCode;
	
	// target data field
	protected DataField dataField;
	
	/**
	 * Creates copy object.
	 * 
	 * @param retCode - retCode enum
	 * @param dataField - target data field
	 */
	protected CopyReturnCode(RetCode retCode, DataField dataField) {
		this.retCode = retCode;
		this.dataField = dataField;
	}
	
	/**
	 * Gets retCode enum (RETURN_CODE).
	 * 
	 * @return
	 */
	public RetCode getRetCode() {
		return retCode;
	}
	
	/**
	 * Sets value into target data field.
	 * 
	 * @param object
	 */
	public abstract void setValue(Object object);
}
