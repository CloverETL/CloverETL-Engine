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

import org.jetel.ctl.CTLCompilable;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Ancestor for all Java code generated from CTL in Denormalizer component.
 * 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>, Javlin (www.javlin.cz)
 * @see org.jetel.component.Denormalizer
 *
 */
public abstract class CTLRecordDenormalize implements RecordDenormalize, CTLCompilable {

	private DataRecord inputRecord;
	private DataRecord outputRecord;

	/**
	 * Initializes global scope via {@link #global()}, then calls user init via {@link #init()}.
	 * All parameters are ignored.
	 * 
	 * @return                  True if OK, otherwise False
	 */
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		global();
		return init();
	}
	
	
	
	/**
	 * Method for CTL compilation. Contains code from global scope
	 * and global variables initialization.
	 * 
	 * @param in	input records
	 * @param out	output records
	 */
	@CTLEntryPoint(
			required = false,
			name = "global"
	)
	public void global() throws ComponentNotReadyException {
		// does nothing
	}
	
	
	/**
	 * Method that contains code from CTL init().
	 * @return true if OK, false otherwise
	 */
	@CTLEntryPoint(
		required = false,
		name = "init"
	)
	protected boolean init() throws ComponentNotReadyException {
		return true;
	}
	
	/**
	 * Passes one input record to the composing class.
	 * @param inRecord
	 * @return < -1 -- fatal error / user defined
	 *           -1 -- error / skip record
	 *         >= 0 -- OK
	 * @throws TransformException
	 */
	@CTLEntryPoint(
			name = "addInputRecord",
			required = true
	)
	public abstract int appendDelegate() throws TransformException;
	
	
	public int append(DataRecord inRecord) throws TransformException {
		inputRecord = inRecord;
		return appendDelegate();
	}

	/**
	 * Retrieves composed output record.
	 * @param outRecord
	 * @return < -1 -- fatal error / user defined
	 *           -1 -- error / skip record
	 *         >= 0 -- OK
	 * @throws TransformException
	 */
	@CTLEntryPoint(
			name = "getOutputRecord",
			required = true
	)
	public abstract int transformDelegate() throws TransformException;
	
	
	public int transform(DataRecord outRecord) throws TransformException {
		outputRecord = outRecord;
		return transformDelegate();
	}


	/**
	 * Finalize current round/clean after current round - called after the transform method was called for the input record
	 */
	@CTLEntryPoint(
			name = "clean",
			required = false
	)
	public void clean() {
		// does nothing
	}
	
	/**
	 * Releases used resources.
	 */
	@CTLEntryPoint(
			name = "finished",
			required = false
	)
	public void finished() {
		// does nothing
	}

	/**
	 *  Returns description of error if one of the methods failed. Can be
	 * also used to get any message produced by transformation.
	 *
	 */
	public String getMessage() {
		return null;
	}

	/**
	 * Reset denormalizer for next graph execution.
	 */
	public void reset() {
		// does nothing
	}

	public DataRecord getInputRecord(int index) {
		return index == 0 ? inputRecord : null;
	}
	
	public DataRecord getOutputRecord(int index) {
		return index == 0 ? outputRecord : null;
	}
	
}
