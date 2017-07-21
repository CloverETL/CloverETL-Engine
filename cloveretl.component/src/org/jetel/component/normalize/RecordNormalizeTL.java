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
package org.jetel.component.normalize;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.jetel.component.AbstractTransformTL;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;

/**
 * Implements normalization based on TransformLang source specified by user. User defines following functions (asterisk
 * denotes the mandatory ones):
 * <ul>
 * <li>* function count()</li>
 * <li>* function transform(idx)</li>
 * <li>function init()</li>
 * <li>function finished()</li>
 * </ul>
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 11/21/06
 * @see org.jetel.component.Normalizer
 */
public class RecordNormalizeTL extends AbstractTransformTL implements RecordNormalize {

	public static final String COUNT_FUNCTION_NAME = "count";
	public static final String COUNT_ON_ERROR_FUNCTION_NAME = "countOnError";
	public static final String TRANSFORM_FUNCTION_NAME = "transform";
	public static final String TRANSFORM_ON_ERROR_FUNCTION_NAME = "transformOnError";
	public static final String CLEAN_FUNCTION_NAME = "clean";

	public static final String IDX_PARAM_NAME = "idx";

	private final DataRecord[] sourceRec = new DataRecord[1];
	private final DataRecord[] targetRec = new DataRecord[1];

	private final TLValue[] countOnErrorArguments = new TLValue[] { new TLStringValue(), new TLStringValue() };
	private final TLValue[] transformArguments = new TLValue[] { TLValue.create(TLValueType.INTEGER) };
	private final TLValue[] transformOnErrorArguments = new TLValue[] { countOnErrorArguments[0],
			countOnErrorArguments[1],  transformArguments[0] };

	private int countFunction;
	private int countOnErrorFunction;
	private int transformFunction;
	private int transformOnErrorFunction;
	private int cleanFunction;

	/** Constructor for the DataRecordTransform object */
	public RecordNormalizeTL(Logger logger, String srcCode) {
		super(srcCode, logger);
	}

	@Override
	public boolean init(Properties parameters, DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		wrapper.setMetadata(new DataRecordMetadata[] { sourceMetadata }, new DataRecordMetadata[] { targetMetadata });
		wrapper.setParameters(parameters);
        wrapper.setGraph(getGraph());
		wrapper.init();

		TLValue result = null;

		try {
			result = wrapper.execute(INIT_FUNCTION_NAME, null);
		} catch (JetelException e) {
			// do nothing, optional function is not declared
		}

		countFunction = wrapper.prepareFunctionExecution(COUNT_FUNCTION_NAME);
		countOnErrorFunction = wrapper.prepareOptionalFunctionExecution(COUNT_ON_ERROR_FUNCTION_NAME);
		transformFunction = wrapper.prepareFunctionExecution(TRANSFORM_FUNCTION_NAME);
		transformOnErrorFunction = wrapper.prepareOptionalFunctionExecution(TRANSFORM_ON_ERROR_FUNCTION_NAME);
		cleanFunction = wrapper.prepareOptionalFunctionExecution(CLEAN_FUNCTION_NAME);

		return (result == null || result == TLBooleanValue.TRUE);
	}

	@Override
	public int count(DataRecord source) {
		return countImpl(countFunction, COUNT_FUNCTION_NAME, source, null);
	}

	@Override
	public int countOnError(Exception exception, DataRecord source) throws TransformException {
		if (countOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Normalization failed!", exception);
		}

		countOnErrorArguments[0].setValue(ExceptionUtils.exceptionChainToMessage(null, exception));
		countOnErrorArguments[1].setValue(ExceptionUtils.stackTraceToString(exception));

		return countImpl(countOnErrorFunction, COUNT_ON_ERROR_FUNCTION_NAME, source, countOnErrorArguments);
	}

	private int countImpl(int function, String functionName, DataRecord source, TLValue[] arguments) {
		TLValue value = wrapper.executePreparedFunction(function, source, arguments);

		if (value.type.isNumeric()) {
			return ((TLNumericValue<?>) value).getInt();
		}

		throw new RuntimeException(functionName + "() function does not return integer value!");
	}

	@Override
	public int transform(DataRecord source, DataRecord target, int idx) throws TransformException {
		transformArguments[0].getNumeric().setValue(idx);

		return transformImpl(transformFunction, source, target, transformArguments);
	}

	@Override
	public int transformOnError(Exception exception, DataRecord source, DataRecord target, int idx)
			throws TransformException {
		if (transformOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Normalization failed!", exception);
		}

		transformOnErrorArguments[0].setValue(ExceptionUtils.exceptionChainToMessage(null, exception));
		transformOnErrorArguments[1].setValue(ExceptionUtils.stackTraceToString(exception));
		transformOnErrorArguments[2].getNumeric().setValue(idx);

		return transformImpl(transformOnErrorFunction, source, target, transformOnErrorArguments);
	}

	private int transformImpl(int function, DataRecord source, DataRecord target, TLValue[] arguments) {
		// set the error message to null so that the getMessage() method works correctly if no error occurs
		errorMessage = null;

		sourceRec[0] = source;
		targetRec[0] = target;

		TLValue result = wrapper.executePreparedFunction(function, sourceRec, targetRec, arguments);

		if (result == null || result == TLBooleanValue.TRUE) {
			return 0;
		}

		if (result.getType().isNumeric()) {
			return result.getNumeric().getInt();
		}

		errorMessage = "Unexpected return result: " + result.toString() + " (" + result.getType().getName() + ")";

		return -1;
	}

	@Override
	public void clean() {
		if (cleanFunction != -1) {
			wrapper.executePreparedFunction(cleanFunction);
		}
	}

}
