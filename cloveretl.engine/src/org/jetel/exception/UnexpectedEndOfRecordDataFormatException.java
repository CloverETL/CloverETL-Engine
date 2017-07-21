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

/**
 * @author jhadrava (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Dec 13, 2010
 */
public class UnexpectedEndOfRecordDataFormatException extends BadDataFormatException {

	/**
	 * 
	 */
	public UnexpectedEndOfRecordDataFormatException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public UnexpectedEndOfRecordDataFormatException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public UnexpectedEndOfRecordDataFormatException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param offendingValue
	 */
	public UnexpectedEndOfRecordDataFormatException(String message, String offendingValue) {
		super(message, offendingValue);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param offendingValue
	 * @param cause
	 */
	public UnexpectedEndOfRecordDataFormatException(String message, String offendingValue, Throwable cause) {
		super(message, offendingValue, cause);
		// TODO Auto-generated constructor stub
	}

}
