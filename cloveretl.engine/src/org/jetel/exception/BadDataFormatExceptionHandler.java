/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 17, 2003
 *  Copyright (C) 2003, 2002  David Pavlis, Wes Maciorowski
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package org.jetel.exception;

import org.jetel.data.DataRecord;

/**
 * This is a base class for handling exceptions caused by misformatted or 
 * incorrect data.  It implements the default behavior associated with 
 * DataPolicy parameter set to 'Strict'.  It aborts processing when 
 * BadDataFormatException is thrown.
 * 
 * @author maciorowski
 *
 */
public class BadDataFormatExceptionHandler {
	public final static String STRICT = "strict";
	public final static String CONTROLLED = "controlled";
	public final static String LENIENT = "lenient";
	
	private boolean throwException = false;
	
	/**
	 * It implements the behavior of the handles.
	 */
	public void handleException(DataRecord record) {
		if(isThrowException())
			throw new BadDataFormatException();
			
	}
	
	/**
	 * @param record
	 * @param fieldCounter
	 * @param string
	 */
	public void populateFieldFailure(DataRecord record, int fieldCounter, String string) {
		setThrowException(true);
	}

	protected void setThrowException(boolean throwException) {
		this.throwException = throwException;
	}

	public boolean isThrowException() {
		return throwException;
	}
}
