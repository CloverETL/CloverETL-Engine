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

import java.util.LinkedList;
import java.util.Queue;

import org.jetel.data.DataRecord;

/**
 * @author sgerguri (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Dec 7, 2011
 */
public class SpreadsheetParserExceptionHandler extends AbstractParserExceptionHandler {
	
	private PolicyType policyType;
	private Queue<String> offendingCoordinates = new LinkedList<String>();
	
	/*
	 * This class is policy type-agnostic - we are always interested in the errors that we encountered during
	 * spreadsheet parsing. Whether to continue or not is determined at the component (SpreadsheetReader/Writer)
	 * level.
	 */
	public SpreadsheetParserExceptionHandler(PolicyType policyType) {
		this.policyType = policyType;
	}
	
	@Override
	public void populateHandler(
            String errorMessage,
            DataRecord record,
            int recordNumber,
            int fieldNumber,
            String offendingValue,
            BadDataFormatException exception) {
    	
    	super.populateHandler(errorMessage, record, recordNumber, fieldNumber, offendingValue, exception);
    }
	
	public void addOffendingCoordinates(String cellCoordinates) {
		offendingCoordinates.add(cellCoordinates);
	}
	
	public String getNextCoordinates() {
		return offendingCoordinates.poll();
	}

	@Override
	protected void handle() {
		if (policyType != PolicyType.LENIENT && exception != null) {
			BadDataFormatException ex = exception;
			exception = null;
			ex.setRawRecord(getRawRecord());
		    throw ex;
		}
	}

	@Override
	public PolicyType getType() {
		return policyType;
	}

}
