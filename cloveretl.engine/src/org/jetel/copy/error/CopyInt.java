package org.jetel.copy.error;

import org.jetel.data.DataField;
import org.jetel.data.IntegerDataField;
import org.jetel.mapping.element.ReturnCodeMappingElement.RetCode;


/**
 * The class provides support for copying an retCode into a clover fields.
 * Target field must be IntegerDataField.
 * 
 * @author Jan Ausperger (jan.ausperger@javlinconsulting.cz)
 *         (c) Javlin, a.s. (www.javlin.eu)
 */
public class CopyInt extends CopyReturnCode {
	
	/**
	 * Creates copy object.
	 * 
	 * @param retCode - retCode enum
	 * @param dataField - target data field
	 */
	public CopyInt(RetCode retCode, DataField datafield) {
		super(retCode, datafield);
	}

	@Override
	public void setValue(Object object) {
		((IntegerDataField) dataField).setValue(object);
	}
}
