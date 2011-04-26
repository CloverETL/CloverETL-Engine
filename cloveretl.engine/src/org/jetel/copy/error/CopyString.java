package org.jetel.copy.error;

import org.jetel.data.DataField;
import org.jetel.data.StringDataField;
import org.jetel.error.CopyError;
import org.jetel.error.ErrorMappingElement.Error;


/**
 * The class provides support for copying an error message into a clover fields.
 * Target field must be StringDataField.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class CopyString extends CopyError {
	
	/**
	 * Creates copy object.
	 * 
	 * @param error - error enum
	 * @param dataField - target data field
	 */
	public CopyString(Error error, DataField datafield) {
		super(error, datafield);
	}

	@Override
	public void setValue(Object object) {
		((StringDataField) dataField).setValue(object);
	}
}
