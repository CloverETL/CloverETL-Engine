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
package org.jetel.component;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @created     March 25, 2009
 * @see         org.jetel.component.RecordGenerate
 */
public class RecordGenerateTL extends RecordTransformCommonTL implements RecordGenerate {

    public static final String GENERATE_FUNCTION_NAME = "generate";
    
    /**Constructor for the DataRecordTransform object */
    public RecordGenerateTL(String srcCode, Log logger) {
    	super(srcCode, logger);
    }

	/**
	 *  Performs any necessary initialization before generate() method is called
	 *
	 * @param  targetMetadata  Array of metadata objects describing source data records
	 * @return                        True if successfull, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] targetRecordsMetadata)
			throws ComponentNotReadyException{
		wrapper.setMetadata(new DataRecordMetadata[]{}, targetRecordsMetadata);
		wrapper.setParameters(parameters);
		if (graph != null){
	        wrapper.setGraph(graph);
		}
		wrapper.init();
		try {
			semiResult = wrapper.execute(INIT_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function init is not necessary
		}
		
		wrapper.prepareFunctionExecution(GENERATE_FUNCTION_NAME);
		
		return semiResult == null ? true : (semiResult==TLBooleanValue.TRUE);
 	}
	
	/**
	 * Generate data for output records.
	 */
	public int generate(DataRecord[] outputRecords) throws TransformException {
		// set the error message to null so that the inherited getMessage() method works correctly if no error occurs
		errorMessage = null;

		semiResult = wrapper.executePreparedFunction(new DataRecord[]{}, outputRecords, null);

		if (semiResult == null || semiResult == TLBooleanValue.TRUE) {
			return ALL;
		}

		if (semiResult.getType().isNumeric()) {
			return semiResult.getNumeric().getInt();
		}

		errorMessage = "Unexpected return result: " + semiResult.toString() + " (" + semiResult.getType().getName() + ")";

		return SKIP;
	}
}

