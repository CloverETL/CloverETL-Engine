/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
package org.jetel.component.normalize;

import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Interface to be implemented by classes implementing normalization, ie decomposition of
 * one input record to several output records.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06  
 * @see org.jetel.component.Normalizer
 *
 */
public interface RecordNormalize {
	/**
	 *  Initializes normalize class/function. This method is called only once at the
	 * beginning of normalization process. Any object allocation/initialization should
	 * happen here.
	 *
	 *@param  parameters	   Global graph parameters and parameters defined specially for the
	 * component which calls this transformation class
	 *@param  sourceMetadata  Metadata describing source data records
	 *@param  targetMetadata   Metadata describing target data record
	 *@return                  True if OK, otherwise False
	 */
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
	throws ComponentNotReadyException;

	/**
	 * @param source Input record
	 * @return Number of output records which will be create from specified input record 
	 */
	public int count(DataRecord source);
	
	/**
	 * 
	 * @param source Input record
	 * @param target Output records
	 * @param idx Sequential number of output record (starting from 0)
	 * @return true for success, false otherwise
	 * @throws TransformException
	 */
	public boolean transform(DataRecord source, DataRecord target, int idx) throws TransformException;

	/**
	 * Releases used resources.
	 */
	public void finished();

	/**
	 *  Returns description of error if one of the methods failed. Can be
	 * also used to get any message produced by transformation.
	 */
	public String getMessage();

	/**
	 * Resets normalizer for next graph execution. 
	 */	
	public void reset();

}
