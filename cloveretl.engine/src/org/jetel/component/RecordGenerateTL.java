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

import org.apache.log4j.Logger;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MiscUtils;

/**
 * @created     March 25, 2009
 * @see         org.jetel.component.RecordGenerate
 */
public class RecordGenerateTL extends AbstractTransformTL implements RecordGenerate {

    public static final String GENERATE_FUNCTION_NAME = "generate";
    public static final String GENERATE_ON_ERROR_FUNCTION_NAME = "generateOnError";

	private final TLValue[] onErrorArguments = new TLValue[] { new TLStringValue(), new TLStringValue() };

    private int generateFunction;
    private int generateOnErrorFunction;

    /**Constructor for the DataRecordTransform object */
    public RecordGenerateTL(String srcCode, Logger logger) {
    	super(srcCode, logger);
    }

	/**
	 *  Performs any necessary initialization before generate() method is called
	 *
	 * @param  targetMetadata  Array of metadata objects describing source data records
	 * @return                        True if successfull, otherwise False
	 */
	@Override
	public boolean init(Properties parameters, DataRecordMetadata[] targetRecordsMetadata)
			throws ComponentNotReadyException{
		wrapper.setMetadata(new DataRecordMetadata[]{}, targetRecordsMetadata);
		wrapper.setParameters(parameters);
        wrapper.setGraph(getGraph());
		wrapper.init();

		try {
			semiResult = wrapper.execute(INIT_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function init is not necessary
		}

		generateFunction = wrapper.prepareFunctionExecution(GENERATE_FUNCTION_NAME);
		generateOnErrorFunction = wrapper.prepareOptionalFunctionExecution(GENERATE_ON_ERROR_FUNCTION_NAME);
		
		return semiResult == null ? true : (semiResult==TLBooleanValue.TRUE);
 	}
	
	/**
	 * Generate data for output records.
	 */
	@Override
	public int generate(DataRecord[] outputRecords) throws TransformException {
		return generateImpl(generateFunction, outputRecords, null);
	}

	@Override
	public int generateOnError(Exception exception, DataRecord[] outputRecords) throws TransformException {
		if (generateOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Generate failed!", exception);
		}

		onErrorArguments[0].setValue(exception.getMessage());
		onErrorArguments[1].setValue(MiscUtils.stackTraceToString(exception));

		return generateImpl(generateOnErrorFunction, outputRecords, onErrorArguments);
	}

	private int generateImpl(int function, DataRecord[] outputRecords, TLValue[] arguments) {
		// set the error message to null so that the inherited getMessage() method works correctly if no error occurs
		errorMessage = null;

		semiResult = wrapper.executePreparedFunction(function, null, outputRecords, arguments);

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
