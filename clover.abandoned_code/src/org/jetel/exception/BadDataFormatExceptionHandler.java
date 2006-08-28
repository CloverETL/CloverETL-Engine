/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

package org.jetel.exception;

import org.jetel.data.DataRecord;

/**
 * This is a base class for handling exceptions caused by misformatted or 
 * incorrect data.  It implements the default behavior associated with 
 * DataPolicy parameter set to 'Strict'.  It aborts processing when 
 * BadDataFormatException is thrown.
 * 
 * @author maciorowski
 *
 */
public class BadDataFormatExceptionHandler {
	public final static String STRICT = "strict";
	public final static String CONTROLLED = "controlled";
	public final static String LENIENT = "lenient";
	
	private boolean throwException = false;
	
	/**
	 *  Error message of this exception.
	 */
	private String errorMessage = null;
	
	/**
	 *  Data record causing the exception.
	 */
	private DataRecord record = null;
	
	/**
	 *  Field which caused the exception.
	 */
	private int fieldCounter = -1;
	
	/**
	 *  The field content that caused the error. 
	 */
	private String fieldContent = null;
	
	/**
	 *  Data policy type for which the handler was created
	 */
	protected String policyType = STRICT;
	
	/**
	 * It implements the behavior of the handles.
	 */
	public void handleException(DataRecord record) {
		if (isThrowException()) {
			throwException = false;
			throw new
			BadDataFormatException(getErrorMessage());
		}
	}
	
	/**
	 *  Populate the exception.
	 * 
	 *  @param errorMessage Error message for this exception
	 *  @param record Record that caused the exception
	 *  @param fieldCounter Number of the field in the record that
	 caused the execption
	 *  @param stringContent Contents of the field that caused the
	 exception
	 */
	public void populateFieldFailure(
			String errorMessage,
			DataRecord record,
			int fieldCounter,
			String stringContent) {
		setThrowException(true);
		this.errorMessage = errorMessage;
		this.record = record;
		this.fieldCounter = fieldCounter;
		this.fieldContent = stringContent;
	}

	protected void setThrowException(boolean throwException) {
		this.throwException = throwException;
	}

	public boolean isThrowException() {
		return throwException;
	}
	
	/**
	 *  Return error message of this exception 
	 * 
	 *  @return error message
	 */
	public String getErrorMessage() {
		return errorMessage; 
	} 
	
	/**
	 *  Return field number on which the exception occured.
	 * 
	 *  @return field number of exception
	 */
	public int getFieldCounter() {
		return fieldCounter;
	}
	
	/**
	 *  Return the record that caused the exception.
	 * 
	 *  @return record causing the exception
	 */
	public DataRecord getRecord() {
		return record;
	}
	
	/**
	 *  Return the field content (string) causing the exception.
	 * 
	 *  @return field content causing the exception
	 */
	public String getFieldContent() {
		return fieldContent;
	}
	
	/**
	 *  Return name of the field which caused the exception
	 * @return field name causing the exception
	 */
	public String getFieldName(){
		return record.getMetadata().getField(fieldCounter).getName();
	}
	
	/**
	 * Return policy for which the handler was created
	 * @return data policy for which the handler serves
	 */
	public String getPolicyType() {
		return this.policyType;
	}
}
