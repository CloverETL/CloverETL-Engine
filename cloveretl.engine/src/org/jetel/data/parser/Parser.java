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
package org.jetel.data.parser;

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.JetelException;
import org.jetel.exception.PolicyType;
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

	/**
	 * Skips specified number of records.
	 * @param nRec Number of records to be skipped. 
	 * @return Number of skipped records.
	 * @throws JetelException
	 */
	public int skip(int nRec) throws JetelException;

	// Operations
	/**
	 *  Initialization of data parser by given metadata.
	 *
	 * @param  _metadata  Description of Parameter
	 * @since             March 27, 2002
	 */
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException;

    /**
     * Sets input data source. Some of parsers allow to call this method repeatedly.
     * 
     * @param inputDataSource
     * @throws IOException if releasing of previous source failed
     */
    public void setDataSource(Object inputDataSource) throws IOException, ComponentNotReadyException;

    /**
     * If releaseInputSource is false, the previous input data source is not released (input stream is not closed).
     * The input data source release is performing into the method 'setDataSource'. Default value is true.
     * 
     * @param releaseInputSource
     */
    public void setReleaseDataSource(boolean releaseInputSource);

	/**
	 *  Closing/deinitialization of parser
	 *
	 * @since    May 2, 2002
	 */
	public void close() throws IOException;


	/**
	 * @param record
	 * @return
	 */
	public DataRecord getNext(DataRecord record) throws JetelException;


	/**
	 * @param handler
	 */
	public void setExceptionHandler(IParserExceptionHandler handler);

    public IParserExceptionHandler getExceptionHandler();
    
    public PolicyType getPolicyType();

    /**
	 * Reset parser for next graph execution. 
     */
	public void reset() throws ComponentNotReadyException;
    
	/**
	 * Gets current position of source file.
	 * 
	 * @return position
	 */
	public Object getPosition();

	/**
	 * Sets position
	 * 
	 * @param position
	 */
	public void movePosition(Object position) throws IOException;

}
/*
 *  end class DataParser
 */

