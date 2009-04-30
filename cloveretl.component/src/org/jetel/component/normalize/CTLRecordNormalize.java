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

import org.jetel.ctl.CTLCompilable;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Ancestor for any Java code generated from CTL code in normalizer.
 * 
 * @author Michal Tomcanyi (michal.tomcanyi@javlin.cz)
 * @see org.jetel.component.Normalizer
 */
public abstract class CTLRecordNormalize implements RecordNormalize, CTLCompilable {

	private DataRecord inputRecord;
	private DataRecord outputRecord;
		
	/**
	 * Releases used resources.
	 */
	@CTLEntryPoint(
			name = "finished",
			required = false
	)
	public void finished() {
		// do nothing
	}


	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#init(org.jetel.graph.TransformationGraph, java.util.Properties, org.jetel.metadata.DataRecordMetadata, org.jetel.metadata.DataRecordMetadata)
	 */
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		global();
		return init();
	}

	
	/**
	 * Method that contains generated CTL code from init().
	 * @return
	 */
	@CTLEntryPoint(
			name = "init",
			required = false
	)
	protected boolean init() throws ComponentNotReadyException {
		return true;
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
		// do nothing
	}
	
	/**
	 * Method that will be populated by generated CTL.
	 * @param source
	 * @return
	 */
	@CTLEntryPoint(
			name = "count",
			required = true
	)
	public abstract int countDelegate();	
	
	
	/**
	 * Implementation of interface method {@link RecordNormalize#count}.
	 * Delegates call to {@link #count(DataRecord[])} used by generated CTL.
	 */
	public int count(DataRecord source) {
		inputRecord = source;
		return countDelegate();
	}
	
	
	@CTLEntryPoint(
			name = "transform",
			parameterNames = {"idx" },
			required = true
	)
	public abstract int transformDelegate(int idx) throws TransformException;
	
	
	/**
	 * Wrapper method to satisfy interface while allowing CTL to generate homogeneous code
	 */
	public int transform(DataRecord source, DataRecord target, int idx) throws TransformException {
		inputRecord = source;
		outputRecord = target;
		return transformDelegate(idx);
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordNormalize#getMessage()
	 */
	public String getMessage() {
		return null;
	}
	
	@CTLEntryPoint(
			name = "clean",
			required = false
	)
	public void clean() {
		// do nothing
	}

	/*
	 * (non-Javadoc)
	 * @see org.jetel.component.normalize.RecordNormalize#reset()
	 */
	public void reset() {
	}
	
	@Override
	public DataRecord getInputRecord(int index) {
		return index == 0 ? inputRecord : null;
	}
	
	@Override
	public DataRecord getOutputRecord(int index) {
		return index == 0 ? outputRecord : null;
	}

}
