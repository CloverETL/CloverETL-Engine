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

import org.apache.commons.logging.Log;
import org.jetel.component.WrapperTL;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Implements denormalization based on TransformLang source specified by user.
 * User defines following functions (asterisk denotes the mandatory ones):<ul>
 * <li>* function addInputRecord()</li>
 * <li>* function getOutputRecord()</li>
 * <li>function init()</li> 
 * <li>function finished()</li>
 * </ul>
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06  
 * @see org.jetel.component.Normalizer
 */
public class RecordDenormalizeTL implements RecordDenormalize {

	private static final String ADDINPUT_FUNCTION_NAME="addInputRecord";
	private static final String GETOUTPUT_FUNCTION_NAME="getOutputRecord";
    private static final String FINISHED_FUNCTION_NAME="finished";
    private static final String INIT_FUNCTION_NAME="init";

    private String errorMessage;
    private WrapperTL wrapper;

    /**Constructor for the DataRecordTransform object */
    public RecordDenormalizeTL(Log logger,String srcCode) {
    	wrapper = new WrapperTL(srcCode, logger);
    }

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordDenormalize#init(java.util.Properties, org.jetel.metadata.DataRecordMetadata, org.jetel.metadata.DataRecordMetadata)
	 */
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		wrapper.setMetadata(new DataRecordMetadata[]{sourceMetadata}, 
				new DataRecordMetadata[]{targetMetadata});
		wrapper.setParameters(parameters);
		wrapper.init();
		try {
			Object result = wrapper.execute(INIT_FUNCTION_NAME);
			return result == null ? true : (Boolean)result;
		} catch (JetelException e) {
			//do nothing: function init is not necessary
		}
		
		wrapper.prepareFunctionExecution(ADDINPUT_FUNCTION_NAME,GETOUTPUT_FUNCTION_NAME);
		
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordDenormalize#addInputRecord(org.jetel.data.DataRecord)
	 */
	public boolean addInputRecord(DataRecord inRecord) {
		Object result = wrapper.executePreparedFunction(0, inRecord);
		return result == null ? true : (Boolean)result;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordDenormalize#getOutputRecord(org.jetel.data.DataRecord)
	 */
	public boolean getOutputRecord(DataRecord outRecord) {
		Object result = wrapper.executePreparedFunction(1, null,
				new DataRecord[]{outRecord});
		return result == null ? true : (Boolean)result;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordDenormalize#finished()
	 */
	public void finished() {
		try {
			wrapper.execute(FINISHED_FUNCTION_NAME);
		} catch (JetelException e) {
			//do nothing: function finished is not necessary
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordDenormalize#getMessage()
	 */
	public String getMessage() {
		return errorMessage;
	}

}
