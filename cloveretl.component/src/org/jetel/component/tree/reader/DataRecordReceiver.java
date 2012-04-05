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
package org.jetel.component.tree.reader;

import org.jetel.data.DataRecord;

/**
 * 
 * Contract to accept results of the parsing operation.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5.12.2011
 */
public interface DataRecordReceiver {
	
	/**
	 * Callback to process data record, the record instance will be re-used.
	 * 
	 * @param record
	 * @param port
	 * 
	 * @throws to notify that the parsing should not proceed
	 */
	void receive(DataRecord record, int port) throws AbortParsingException;
	
	/**
	 * Callback to notify that exception occurred while setting data field value.
	 * 
	 * @param e
	 * 
	 * @throws AbortParsingException to notify that the parsing should not proceed
	 */
	void exceptionOccurred(FieldFillingException e) throws AbortParsingException;
}
