package org.jetel.copy.error;

import org.jetel.data.DataField;
import org.jetel.mapping.element.ReturnCodeMappingElement.RetCode;

/**
 * The class support method for creating copy retCode classes.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class CopyRetCodeProvider {

	/**
	 * Creates a copy clover class.
	 * 
	 * @param dataField - target data field
	 * @return - copy clover class
	 */
	public static CopyReturnCode createRetCode(RetCode retCode, DataField targetDataField) {
		// extension is possible
		return new CopyInt(retCode, targetDataField);
	}
}
