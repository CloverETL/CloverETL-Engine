/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
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

package org.jetel.data;
import java.io.*;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Interface to output data formatters
 *
 *@author     your_name_here
 *@since      December 30, 2002
 *@see        OtherClasses
 */
public interface DataFormatter {

	// Attributes

	// Associations

	// Operations
	/**
	 *  Initialization of data formatter
	 *
	 *@param  out        Description of the Parameter
	 *@param  _metadata  Description of the Parameter
	 */
	public void open(OutputStream out, DataRecordMetadata _metadata);


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
	public void write(DataRecord record) throws IOException;


	/**
	 *  Flush any unwritten data into output stream
	 */
	public void flush();

}
/*
 *  end class DataFormatter
 */

