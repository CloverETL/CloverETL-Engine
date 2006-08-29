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

// FILE: c:/projects/jetel/org/jetel/data/DataParser.java

package org.jetel.data.parser;

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Interface to input data parsers
 *
 * @author     D.Pavlis
 * @since    March 27, 2002
 * @see        OtherClasses
 */
public interface Parser {

	/**
	 *  An operation that produces next record from Input data or null
	 *
	 * @return                  The Next value
	 * @exception  IOException  Description of Exception
	 * @since                   March 27, 2002
	 */
	public DataRecord getNext() throws JetelException;


	// Operations
	/**
	 *  Initialization of data parser
	 *
	 * @param  in         Description of Parameter
	 * @param  _metadata  Description of Parameter
	 * @since             March 27, 2002
	 */
	public void open(Object inputDataSource, DataRecordMetadata _metadata) throws ComponentNotReadyException;


	/**
	 *  Closing/deinitialization of parser
	 *
	 * @since    May 2, 2002
	 */
	public void close();


	/**
	 * @param record
	 * @return
	 */
	public DataRecord getNext(DataRecord record) throws JetelException;


	/**
	 * @param handler
	 */
	public void setExceptionHandler(IParserExceptionHandler handler);

}
/*
 *  end class DataParser
 */

