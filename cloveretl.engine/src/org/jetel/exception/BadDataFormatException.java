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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetel.util.StringUtils;

/**
 * @author Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class BadDataFormatException extends RuntimeException implements Iterable<BadDataFormatException> {
    
	private CharSequence offendingValue;
    
    private int recordNumber;
    
    private int fieldNumber;
    
    private List<BadDataFormatException> exceptions = new ArrayList<BadDataFormatException>();
    
	public BadDataFormatException() {
		super();
		exceptions.add(this);
	}

	public BadDataFormatException(String message) {
		super(message);
		exceptions.add(this);
	}

    public BadDataFormatException(String message, Throwable cause) {
        super(message, cause);
		exceptions.add(this);
    }
    
	public BadDataFormatException(String message, String offendingValue) {
		super(message);
		this.offendingValue = offendingValue;
		exceptions.add(this);
	}
    
    public BadDataFormatException(String message, String offendingValue, Throwable cause) {
        super(message, cause);
        this.offendingValue = offendingValue;
		exceptions.add(this);
    }

    public void addException(BadDataFormatException next){
    	exceptions.add(next);
    }
    
	public void setOffendingValue(CharSequence offendingValue) {
		this.offendingValue = offendingValue;
	}

	public CharSequence getOffendingValue() {
		return offendingValue;
	}
    
    @Override
    public String getMessage() {
        StringBuffer ret = new StringBuffer();
        ret.append(super.getMessage());
        if(offendingValue != null && offendingValue.length() > 0) {
            ret.append(" : ");
            ret.append(StringUtils.quote(StringUtils.specCharToString(offendingValue)));
        }
        if(recordNumber >= 0) {
            ret.append(" in record # ");
            ret.append(recordNumber);
        }
        
        if(fieldNumber >= 0) {
            ret.append(" in field # ");
            ret.append(fieldNumber);
        }
        
        return ret.toString();
    }

    public int getFieldNumber() {
        return fieldNumber;
    }

    public void setFieldNumber(int fieldNumber) {
        this.fieldNumber = fieldNumber;
    }

    public int getRecordNumber() {
        return recordNumber;
    }

    public void setRecordNumber(int recordNumber) {
        this.recordNumber = recordNumber;
    }

	public Iterator<BadDataFormatException> iterator() {
		return exceptions.iterator();
	}
    
}
