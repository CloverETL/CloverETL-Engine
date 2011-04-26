package org.jetel.copy.error;

import org.jetel.data.DataField;
import org.jetel.data.IntegerDataField;
import org.jetel.error.CopyError;
import org.jetel.error.ErrorMappingElement.Error;


/**
 * The class support method for creating copy error classes.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 *
 * @created 14.8.2007
 */
public class CopyErrorProvider {

	/**
	 * Creates a copy clover class.
	 * 
	 * @param dataField - target data field
	 * @return - copy clover class
	 */
	public static CopyError createCopyError(Error error, DataField targetDataField) {
		// extension is possible
		if (targetDataField instanceof IntegerDataField)
			return new CopyInteger(error, targetDataField);
		return new CopyString(error, targetDataField);
	}
}
