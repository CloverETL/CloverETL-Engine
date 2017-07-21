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
 * Interface for common handling of BadDataFormatException in parsers.
 * @author Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
 * 
 */
public interface IParserExceptionHandler {
    
    /**
     * How to handle exception?
     */
    public void handleException();
    
    /**
     * Sets all internal fields of handler.
     * @param errorMessage description of catched exception
     * @param record data record, where exception was appeared
     * @param recordNumber number of data record
     * @param fieldNumber number of field
     * @param offendingValue non-parsable string
     * @param exception handled BadDataFormatException
     */
    public void populateHandler(
            String errorMessage,
            DataRecord record,
            int recordNumber,
            int fieldNumber,
            String offendingValue,
            BadDataFormatException exception);
    
    public String getErrorMessage();
    
    public BadDataFormatException getException();
    
    public DataRecord getRecord();
    
    public int getRecordNumber();

    public boolean isExceptionThrowed();
    
    public abstract PolicyType getType();

	public String getRawRecord();

	public void setRawRecord(String rawRecord);

}
