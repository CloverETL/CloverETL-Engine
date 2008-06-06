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

// FILE: c:/projects/jetel/org/jetel/exception/NoMoreDataException.java

package org.jetel.exception;

/** A class that represents general Jetel exception
 * 
 * @see Exception
 * @author D.Pavlis
 */
public class JetelException extends Exception {

	private static final long serialVersionUID = -8049941630255692694L;

	// Attributes

	// Associations

	// Operations
	public JetelException(String message) {
		super(message);
	}

	public JetelException(String message, Throwable cause) {
		super(message, cause);
	}

	@Override
	public String getMessage() {
		if (super.getCause() != null) {
			return super.getMessage() + " caused by: " + super.getCause();
		}
		return super.getMessage();
	}

} /* end class NoMoreDataException */
