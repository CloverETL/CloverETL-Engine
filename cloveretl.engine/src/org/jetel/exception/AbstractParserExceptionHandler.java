/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
    
    protected BadDataFormatException exception = null;
    
    protected List<ParseException> errors = new ArrayList<ParseException>();
    
    public void handleException() {
        exceptionThrowed = false;
        handle();
    }
    
    abstract protected void handle();
    
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
        errors.add(new ParseException(fieldNumber,offendingValue,errorMessage));
        exception.setFieldNumber(fieldNumber);
        exception.setOffendingValue(offendingValue);
        exception.setRecordNumber(recordNumber);
        if (this.exception == null) {
        	this.exception = exception;
        }else{
        	this.exception.addException(exception);
        }
    }
    
    public String getErrorMessage() {
        StringBuilder errorMess = new StringBuilder();
        ParseException element;
        for (Iterator<ParseException> iter = errors.iterator(); iter.hasNext();) {
        	element = iter.next();
			errorMess.append("Field number: ");
			errorMess.append(element.fieldNumber);
			errorMess.append(", offending value: ");
			errorMess.append(element.offendingValue);
			errorMess.append(", message: ");
			errorMess.append(element.errorMessage);
			errorMess.append(".");
		}
        return errorMess.toString();
    }
    
    public List<ParseException> getErrors() {
    	return errors;
    }
    
    public BadDataFormatException getException() {
    	return exception;
    }

    public DataRecord getRecord() {
        return record;
    }
    
    public int getRecordNumber() {
        return recordNumber;
    }

    public boolean isExceptionThrowed() {
        return exceptionThrowed;
    }
        
    public abstract PolicyType getType();

}

