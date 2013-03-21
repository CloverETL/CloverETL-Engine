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
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;

/**
 *  
 *
 * @author      dpavlis
 * @since       June 25, 2006
 * @revision    $Revision: $
 * @created     June 25, 2006
 * @see         org.jetel.component.RecordTransform
 */
public class RecordTransformTL extends AbstractTransformTL implements RecordTransform {

    public static final String TRANSFORM_FUNCTION_NAME = "transform";
    public static final String TRANSFORM_ON_ERROR_FUNCTION_NAME = "transformOnError";

	private final TLValue[] onErrorArguments = new TLValue[] { new TLStringValue(), new TLStringValue() };

    private int transformFunction;
    private int transformOnErrorFunction;

    /**Constructor for the DataRecordTransform object */
    public RecordTransformTL(String srcCode, Log logger) {
    	super(srcCode, logger);
    }

    /**Constructor for the DataRecordTransform object */
    public RecordTransformTL(String srcCode, Logger logger) {
    	super(srcCode, LogFactory.getLog(logger.getName()));
    }

	/**
	 *  Performs any necessary initialization before transform() method is called
	 *
	 * @param  sourceMetadata  Array of metadata objects describing source data records
	 * @param  targetMetadata  Array of metadata objects describing source data records
	 * @return                        True if successfull, otherwise False
	 */
	@Override
	public boolean init(Properties parameters, DataRecordMetadata[] sourceRecordsMetadata, DataRecordMetadata[] targetRecordsMetadata)
			throws ComponentNotReadyException{
		wrapper.setMetadata(sourceRecordsMetadata, targetRecordsMetadata);
		wrapper.setParameters(parameters);
        wrapper.setGraph(getGraph());
		wrapper.init();

		try {
			semiResult = wrapper.execute(INIT_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function init is not necessary
		}
		
		transformFunction = wrapper.prepareFunctionExecution(TRANSFORM_FUNCTION_NAME);
		transformOnErrorFunction = wrapper.prepareOptionalFunctionExecution(TRANSFORM_ON_ERROR_FUNCTION_NAME);
		
		return semiResult == null ? true : (semiResult==TLBooleanValue.TRUE);
 	}

	@Override
	public int transform(DataRecord[] inputRecords, DataRecord[] outputRecords) throws TransformException {
		return transformImpl(transformFunction, inputRecords, outputRecords, null);
	}

	@Override
	public int transformOnError(Exception exception, DataRecord[] inputRecords, DataRecord[] outputRecords)
			throws TransformException {
		if (transformOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Transform failed!", exception);
		}

		onErrorArguments[0].setValue(ExceptionUtils.getMessage(null, exception));
		onErrorArguments[1].setValue(ExceptionUtils.stackTraceToString(exception));

		return transformImpl(transformOnErrorFunction, inputRecords, outputRecords, onErrorArguments);
	}

	private int transformImpl(int function, DataRecord[] inputRecords, DataRecord[] outputRecords, TLValue[] arguments) {
		// set the error message to null so that the inherited getMessage() method works correctly if no error occurs
		errorMessage = null;

		semiResult = wrapper.executePreparedFunction(function, inputRecords, outputRecords, arguments);

		if (semiResult == null || semiResult == TLBooleanValue.TRUE) {
			return ALL;
		}

		if (semiResult.getType().isNumeric()) {
			return semiResult.getNumeric().getInt();
		}

		errorMessage = "Unexpected return result: " + semiResult.toString() + " (" + semiResult.getType().getName() + ")";

		return SKIP;
	}

	@Override
	public void signal(Object signalObject) {
		// does nothing
	}

	@Override
	public Object getSemiResult() {
		return semiResult;
	}

}
