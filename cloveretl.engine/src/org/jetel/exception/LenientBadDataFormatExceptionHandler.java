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

import org.jetel.data.DataRecord;

/**
 * Lenient handler sets incorrect data to default values.
 * @author maciorowski
 *
 */
public class LenientBadDataFormatExceptionHandler
	extends BadDataFormatExceptionHandler {

		public LenientBadDataFormatExceptionHandler() {
			this.policyType = BadDataFormatExceptionHandler.LENIENT;
		}
	
		/**
		 * It does nothing as all action in this handler happens in populateFieldFailure.
		 */
		public void handleException(DataRecord record) {
		}

		/**
		 * @param record
		 * @param fieldCounter
		 * @param string
		 */
		public void populateFieldFailure(String errorMessage, DataRecord record, int fieldCounter, String string) {
			record.getField(fieldCounter).setToDefaultValue();
		}
}
