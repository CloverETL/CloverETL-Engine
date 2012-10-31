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
import org.jetel.ctl.CTLAbstractTransformAdapter;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MiscUtils;

/**
 * Implements normalization based on TransformLang source specified by user.
 * User defines following functions (asterisk denotes the mandatory ones):
 * <ul>
 * <li>* function count()</li>
 * <li>* function transform(idx)</li>
 * <li>function init()</li>
 * <li>function finished()</li>
 * </ul>
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting
 *         (www.javlinconsulting.cz)
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 * @since 11/21/06
 * @see org.jetel.component.Normalizer
 */
public final class CTLRecordNormalizeAdapter extends CTLAbstractTransformAdapter implements RecordNormalize {

	private final DataRecord[] sourceRec = new DataRecord[1];
	private final DataRecord[] targetRec = new DataRecord[1];

	private final Object[] countOnErrorArguments = new Object[2];
	private final Object[] transformArguments = new Object[1];
	private final Object[] transformOnErrorArguments = new Object[3];

	private CLVFFunctionDeclaration countFunction;
	private CLVFFunctionDeclaration countOnErrorFunction;
	private CLVFFunctionDeclaration transformFunction;
	private CLVFFunctionDeclaration transformOnErrorFunction;
	private CLVFFunctionDeclaration cleanFunction;

    /**
     * Constructs a <code>CTLRecordNormalizeAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLRecordNormalizeAdapter(TransformLangExecutor executor, Logger logger) {
		super(executor, logger);
	}

	@Override
	public boolean init(Properties parameters, DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
        // initialize global scope and call user initialization function
		super.init();

		countFunction = executor.getFunction(RecordNormalizeTL.COUNT_FUNCTION_NAME);
		countOnErrorFunction = executor.getFunction(RecordNormalizeTL.COUNT_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING);
		transformFunction = executor.getFunction(RecordNormalizeTL.TRANSFORM_FUNCTION_NAME, TLTypePrimitive.INTEGER);
		transformOnErrorFunction = executor.getFunction(RecordNormalizeTL.TRANSFORM_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING, TLTypePrimitive.INTEGER);
		cleanFunction = executor.getFunction(RecordNormalizeTL.CLEAN_FUNCTION_NAME);

		if (countFunction == null) {
			throw new ComponentNotReadyException(RecordNormalizeTL.COUNT_FUNCTION_NAME + " function is not defined");
		}

		if (transformFunction  == null) {
			throw new ComponentNotReadyException(RecordNormalizeTL.TRANSFORM_FUNCTION_NAME + " function is not defined");
		}

		return true;
 	}

	@Override
	public int count(DataRecord source) {
		return countImpl(countFunction, source, NO_ARGUMENTS);
	}

	@Override
	public int countOnError(Exception exception, DataRecord source) throws TransformException {
		if (countOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Normalizer failed!", exception);
		}

		countOnErrorArguments[0] = exception.getMessage();
		countOnErrorArguments[1] = MiscUtils.stackTraceToString(exception);

		return countImpl(countOnErrorFunction, source, countOnErrorArguments);
	}

	private int countImpl(CLVFFunctionDeclaration function, DataRecord source, Object[] arguments) {
		sourceRec[0] = source;

		final Object retVal = executor.executeFunction(function, arguments, sourceRec, NO_DATA_RECORDS);

		if (retVal == null || !(retVal instanceof Integer)) {
			throw new TransformLangExecutorRuntimeException(function.getName() + "() function must return 'int'");
		}

		return (Integer) retVal;
	}

	@Override
	public int transform(DataRecord source, DataRecord target, int idx)
			throws TransformException {
		transformArguments[0] = idx;

		return transformImpl(transformFunction, source, target, transformArguments);
	}

	@Override
	public int transformOnError(Exception exception, DataRecord source, DataRecord target, int idx)
			throws TransformException {
		if (transformOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Normalizer failed!", exception);
		}

		transformOnErrorArguments[0] = exception.getMessage();
		transformOnErrorArguments[1] = MiscUtils.stackTraceToString(exception);
		transformOnErrorArguments[2] = idx;

		return transformImpl(transformOnErrorFunction, source, target, transformOnErrorArguments);
	}

	private int transformImpl(CLVFFunctionDeclaration function, DataRecord source, DataRecord target, Object[] arguments) {
		sourceRec[0] = source;
		targetRec[0] = target;

		Object result = executor.executeFunction(function, arguments, sourceRec, targetRec);

		if (result == null || !(result instanceof Integer)) {
			throw new TransformLangExecutorRuntimeException(function.getName() + "() function must return 'int'");
		}

		return (Integer) result;
	}

	@Override
	public void clean() {
		if (cleanFunction == null) {
			return;
		}

		try {
			executor.executeFunction(cleanFunction, NO_ARGUMENTS);
		} catch (TransformLangExecutorRuntimeException exception) {
			logger.warn("Failed to execute " + cleanFunction.getName() + "() function: " + exception.getMessage());
		}
	}

}
