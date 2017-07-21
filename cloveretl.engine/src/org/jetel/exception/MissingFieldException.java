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
package org.jetel.exception;


/**
 * An exception indicating that an attempt was made to access a non-existing field.
 * 
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jul 3, 2012
 */
public class MissingFieldException extends JetelRuntimeException {

	private static final long serialVersionUID = -4595828225287715756L;

	private final boolean output;
	
	private final int recordId;

	private final String fieldName;
	
	/**
	 * @param message
	 */
	public MissingFieldException(String message, boolean output, int recordId, String fieldName) {
		super(message);
		this.fieldName = fieldName;
		this.output = output;
		this.recordId = recordId;
	}

	public boolean isOutput() {
		return output;
	}

	public int getRecordId() {
		return recordId;
	}

	public String getFieldName() {
		return fieldName;
	}
	
}
