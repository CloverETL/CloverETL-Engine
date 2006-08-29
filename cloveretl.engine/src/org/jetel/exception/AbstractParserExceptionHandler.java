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

    protected String errorMessage;
    
    protected DataRecord record;
    
    protected int recordNumber;
    
    protected int fieldNumber;
    
    protected String offendingValue;
    
    protected BadDataFormatException exception;
    
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
        this.errorMessage = errorMessage;
        this.record = record;
        this.recordNumber = recordNumber;
        this.fieldNumber = fieldNumber;
        this.offendingValue = offendingValue;
        this.exception = exception;
    }
    
    public String getErrorMessage() {
        return errorMessage;
    }

    public DataRecord getRecord() {
        return record;
    }
    
    public int getRecordNumber() {
        return recordNumber;
    }

    public int getFieldNumber() {
        return fieldNumber;
    }
    
    public String getOffendingValue() {
        return offendingValue;
    }
    
    public Exception getException() {
        return exception;
    }

    public boolean isExceptionThrowed() {
        return exceptionThrowed;
    }
    
    
    public String getFieldName() {
        return record.getMetadata().getField(fieldNumber).getName();
    }
    
    public abstract PolicyType getType();

}
