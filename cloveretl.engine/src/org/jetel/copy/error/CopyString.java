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
