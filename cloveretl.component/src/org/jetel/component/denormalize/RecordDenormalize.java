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
package org.jetel.component.denormalize;

import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Interface to be implemented by classes implementing denormalization, ie composition of
 * one output record from several input records.
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06  
 * @see org.jetel.component.Denormalizer
 *
 */
public interface RecordDenormalize {
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
	 * Passes one input record to the composing class.
	 * @param inRecord
	 * @return true on success, false otherwise
	 * @throws TransformException
	 */
	public boolean addInputRecord(DataRecord inRecord) throws TransformException;

	/**
	 * Retrieves composed output record.
	 * @param outRecord
	 * @return
	 * @throws TransformException
	 */
	public boolean getOutputRecord(DataRecord outRecord) throws TransformException;

	
	/**
	 * Finalize current round/clean after current round - called after the getOutputRecord method was called for the input record
	 */
	public void clean();
	
	/**
	 * Releases used resources.
	 */
	public void finished();

	/**
	 *  Returns description of error if one of the methods failed. Can be
	 * also used to get any message produced by transformation.
	 *
	 */
	public String getMessage();

	/**
	 * Reset denormalizer for next graph execution.
	 */
	public void reset();

}
