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
package org.jetel.data.formatter;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.WritableByteChannel;

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
public interface Formatter extends Closeable, Flushable {
	
	/**
	 * This enumeration is used by #getPreferredDataSourceType() method to suggest preferred 
	 * data source type by a {@link Parser}.
	 */
	public enum DataTargetType {
		/** Data target represented by {@link WritableByteChannel} or {@link OutputStream} */
		CHANNEL,
		/** Data target represented by {@link File} */
		FILE,
		/** Data target represented by {@link URI} */
		URI
	}

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
	 * @throws IOException if previous data target throws IOExcpetion on close()
     */
    public void setDataTarget(Object outputDataTarget) throws IOException;

	/**
	 *  Closing/deinitialization of formatter
	 */
	@Override
	public void close() throws IOException;


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
	@Override
	public void flush() throws IOException;

	/**
	 * This method writes all data (header, body, footer) which are to write, but doesn't close underlying streams.
	 * TODO remove this method, {@link #flush()} is good enough, see CL-2196 
	 * @throws IOException
	 */
	public void finish() throws IOException;
	
    /**
     * Formatter can request specific data target type, which is preferred to be passed into {@link #setDataTarget(Object)} method.
     * This is intended just a hint for source provider, so other types of data targets 
     * should be expected.
     */
    public DataTargetType getPreferredDataTargetType();

    /**
     * This method should be used to inform formatter about type of writing to data target.
     * @param append <code>true</code> if append mode of writing is used; <code>false</code> otherwise
     */
    public void setAppend(boolean append);

	/**
	 * @param informs the formatter that appending to a non-empty file is being performed
	 */
	public void setAppendTargetNotEmpty(boolean b);
    
}
/*
 *  end class DataFormatter
 */

