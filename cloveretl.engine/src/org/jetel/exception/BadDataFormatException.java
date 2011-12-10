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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class BadDataFormatException extends RuntimeException implements Iterable<BadDataFormatException>, Iterator<BadDataFormatException> {
    
	/**
	 * 
	 */
	private static final long serialVersionUID = -2277855196444066739L;

	private CharSequence offendingValue;
    
	private CharSequence rawRecord;
	
    private int recordNumber=-1;
    
    private int fieldNumber=-1;
    
    private String additionalMessage;
    
    private BadDataFormatException next = null;
    
	public BadDataFormatException() {
		super();
	}

	public BadDataFormatException(String message) {
		super(message);
	}

    public BadDataFormatException(String message, Throwable cause) {
        super(message, cause);
    }
    
	public BadDataFormatException(String message, CharSequence offendingValue) {
		super(message);
		this.offendingValue = offendingValue;
	}
    
    public BadDataFormatException(String message, CharSequence offendingValue, Throwable cause) {
        super(message, cause);
        this.offendingValue = offendingValue;
    }
    
    public synchronized void setNextException(BadDataFormatException next){
    	BadDataFormatException theEnd = this;
    	while (theEnd.next != null) {
    	    theEnd = theEnd.next;
    	}
    	theEnd.next = next;
    }
    
	public void setOffendingValue(CharSequence offendingValue) {
		this.offendingValue = offendingValue;
	}

	public CharSequence getOffendingValue() {
		return offendingValue;
	}

	public void setRawRecord(CharSequence rawRecord) {
		this.rawRecord = rawRecord;
	}

	public CharSequence getRawRecord() {
		return rawRecord;
	}

	public String getSimpleMessage() {
		return super.getMessage();
	}
	
    @Override
    public String getMessage() {
        StringBuffer ret = new StringBuffer();
        ret.append(super.getMessage());
//        if(offendingValue != null && offendingValue.length() > 0) {
//            ret.append(" : ");
//            ret.append(StringUtils.quote(StringUtils.specCharToString(offendingValue)));
//        }
        if (additionalMessage != null) {
        	ret.append(" ");
        	ret.append(additionalMessage);
        }
        
        if(fieldNumber > -1) {
            ret.append(" in field # ");
            ret.append(fieldNumber + 1);
        }
        
        if(recordNumber > -1) {
            ret.append(" of record # ");
            ret.append(recordNumber);
        }
        
        if(offendingValue != null) {
            ret.append(", value: '");
            ret.append(offendingValue);
            ret.append("'");
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

    public void setAdditionalMessage(String additionalMessage) {
    	this.additionalMessage = additionalMessage;
    }

    public String getAdditionalMessage() {
    	return additionalMessage;
    }

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public BadDataFormatException next() {
		return next;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<BadDataFormatException> iterator() {
		List<BadDataFormatException> exceptions = new ArrayList<BadDataFormatException>();
		BadDataFormatException ex = this;
		do {
			exceptions.add(ex);
			ex = ex.next;
		}while (ex != null);
		return exceptions.iterator();
	}    
}
