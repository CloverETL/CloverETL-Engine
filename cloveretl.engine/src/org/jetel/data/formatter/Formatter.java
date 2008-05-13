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

package org.jetel.data.formatter;
import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Interface to output data formatters
 *
 *@author     David Pavlis
 *@since      December 30, 2002
 *@see        OtherClasses
 */
public interface Formatter {

	// Attributes

	// Associations

	// Operations
	/**
	 *  Initialization of data formatter by given metadata.
	 *
	 *@param  _metadata  Description of the Parameter
	 */
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException;
	
	public void reset();

    /**
     * Sets output data destination. Some of formatters allow to call this method repeatedly.
     * @param outputDataTarget
     */
    public void setDataTarget(Object outputDataTarget);

	/**
	 *  Closing/deinitialization of formatter
	 */
	public void close();


	/**
	 *  Formats data record based on provided metadata
	 *
	 *@param  record           Data record to format and send to output stream
	 *@exception  IOException  Description of the Exception
	 */
	public int write(DataRecord record) throws IOException;

	
	/**
	 *  Formats header based on provided metadata
	 * @throws IOException
	 */
	public int writeHeader() throws IOException;

	
	/**
	 *  Formats footer based on provided metadata
	 * @throws IOException
	 */
	public int writeFooter() throws IOException;


	/**
	 *  Flush any unwritten data into output stream
	 * @throws IOException
	 */
	public void flush() throws IOException;

	/**
	 * This method writes all data (header, body, footer) which are to write, but doesn't close underlying streams.
	 * 
	 * @throws IOException
	 */
	public void finish() throws IOException;
}
/*
 *  end class DataFormatter
 */

