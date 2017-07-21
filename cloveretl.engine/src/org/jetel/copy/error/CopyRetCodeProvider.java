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
