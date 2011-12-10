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

import org.jetel.data.DataRecord;

/**
 * Basic abstract implementation of IParserExceptionHandler interface.
 * All build-in clover engine handlers extends this class.
 * 
 * @author Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public abstract class AbstractParserExceptionHandler implements IParserExceptionHandler {

	protected boolean exceptionThrowed;

    protected DataRecord record;
    
    protected int recordNumber;
    
    protected String rawRecord;
    
    protected BadDataFormatException exception = null;
    
    @Override
	public void handleException() {
        exceptionThrowed = false;
        handle();
        exception = null;
    }
    
    abstract protected void handle();
    
    @Override
	public void populateHandler(
            String errorMessage,
            DataRecord record,
            int recordNumber,
            int fieldNumber,
            String offendingValue,
            BadDataFormatException exception) {
        this.exceptionThrowed = true;
        this.record = record;
        this.recordNumber = recordNumber;
        exception.setFieldNumber(fieldNumber);
        exception.setOffendingValue(offendingValue);
        exception.setRecordNumber(recordNumber);
        if (this.exception == null) {
        	this.exception = exception;
        }else{
        	this.exception.setNextException(exception);
        }
    }
    
    @Override
	public String getErrorMessage() {
		if (exception == null)	return "";
		StringBuilder errorMess = new StringBuilder();
		for (BadDataFormatException ex : exception) {
			errorMess.append("Field number: ");
			errorMess.append(ex.getFieldNumber() + 1);
			errorMess.append(", offending value: ");
			errorMess.append(ex.getOffendingValue());
			errorMess.append(", message: ");
			errorMess.append(ex.getMessage());
			errorMess.append(".");
		}
		return errorMess.toString();
	}
    
     @Override
	public BadDataFormatException getException() {
    	return exception;
    }

    @Override
	public DataRecord getRecord() {
        return record;
    }
    
    @Override
	public int getRecordNumber() {
        return recordNumber;
    }

    @Override
	public boolean isExceptionThrowed() {
        return exceptionThrowed;
    }
        
    @Override
	public abstract PolicyType getType();

	@Override
	public String getRawRecord() {
		return rawRecord;
	}

	@Override
	public void setRawRecord(String rawRecord) {
		this.rawRecord = rawRecord;
	}

}

