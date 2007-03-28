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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
        errors.add(new ParseException(fieldNumber,offendingValue,errorMessage,exception));
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

    public DataRecord getRecord() {
        return record;
    }
    
    public int getRecordNumber() {
        return recordNumber;
    }

    public int[] getFieldNumber() {
    	int[] fieldNumber = new int[errors.size()];
    	int i=0;
    	for (Iterator<ParseException> iter = errors.iterator(); iter.hasNext();) {
			fieldNumber[i++] = iter.next().fieldNumber;
		}
        return fieldNumber;
    }
    
    public String[] getOffendingValue() {
    	String[] offendingValues = new String[errors.size()];
    	int i=0;
    	for (Iterator<ParseException> iter = errors.iterator(); iter.hasNext();) {
			offendingValues[i++] = iter.next().offendingValue;
		}
    	return offendingValues;
    }
    
    public Exception[] getException() {
    	Exception[] exceptions = new Exception[errors.size()];
    	int i=0;
    	for (Iterator<ParseException> iter = errors.iterator(); iter.hasNext();) {
			exceptions[i++] = iter.next().exception;
		}
    	return exceptions;
    }

    public boolean isExceptionThrowed() {
        return exceptionThrowed;
    }
    
    
    public String[] getFieldName() {
    	int[] fieldNumber = getFieldNumber();
    	String[] fieldName = new String[fieldNumber.length];
    	for (int i = 0; i < fieldName.length; i++) {
			fieldName[i] = record.getMetadata().getField(fieldNumber[i]).getName(); 
		}
        return fieldName;
    }
    
    public abstract PolicyType getType();

}

class ParseException extends Exception{
	
	int fieldNumber;
	String offendingValue;
	String errorMessage;
	BadDataFormatException exception;

	ParseException(int fieldNumber, String offendingValue, String errorMessage, BadDataFormatException exception) {
		super();
		this.fieldNumber = fieldNumber;
		this.offendingValue = offendingValue;
		this.errorMessage = errorMessage;
		this.exception = exception;
	}
	
	
	
}
