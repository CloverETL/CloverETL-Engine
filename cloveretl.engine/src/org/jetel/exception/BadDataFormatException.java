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

/**
 * @author Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class BadDataFormatException extends RuntimeException {
    
	private String offendingValue;
    
	public BadDataFormatException() {
		super();
	}

	public BadDataFormatException(String message) {
		super(message);
	}

    public BadDataFormatException(String message, Throwable cause) {
        super(message, cause);
    }
    
	public BadDataFormatException(String message, String offendingValue) {
		super(message);
		this.offendingValue = offendingValue;
	}
    
    public BadDataFormatException(String message, String offendingValue, Throwable cause) {
        super(message, cause);
        this.offendingValue = offendingValue;
    }

	public void setOffendingValue(String offendingValue) {
		this.offendingValue = offendingValue;
	}

	public String getOffendingValue() {
		return offendingValue;
	}
    
    @Override
    public String getMessage() {
        return super.getMessage() + " : " + offendingValue;
    }
}
