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
package org.jetel.component.denormalize;

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
import org.jetel.util.ExceptionUtils;

/**
 * Implements denormalization based on TransformLang source specified by user. User defines following functions
 * (asterisk denotes the mandatory ones):
 * <ul>
 * <li>* function append()</li>
 * <li>* function transform()</li>
 * <li>function init()</li>
 * <li>function finished()</li>
 * </ul>
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 * @since 11/21/06
 * @see org.jetel.component.Normalizer
 */
public class CTLRecordDenormalizeAdapter extends CTLAbstractTransformAdapter implements RecordDenormalize {

	protected final DataRecord[] inputRecords = new DataRecord[1];
	protected final DataRecord[] outputRecords = new DataRecord[1];

	private final Object[] onErrorArguments = new Object[2];

	protected CLVFFunctionDeclaration appendFunction;
	protected CLVFFunctionDeclaration appendOnErrorFunction;
	protected CLVFFunctionDeclaration transformFunction;
	protected CLVFFunctionDeclaration transformOnErrorFunction;
	protected CLVFFunctionDeclaration cleanFunction;

	/**
     * Constructs a <code>CTLRecordDenormalizeAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLRecordDenormalizeAdapter(TransformLangExecutor executor, Logger logger) {
		super(executor, logger);
	}

	@Override
	public boolean init(Properties parameters, DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		// initialize global scope and call user initialization function
		super.init();

		appendFunction = executor.getFunction(RecordDenormalizeTL.APPEND_FUNCTION_NAME);
		appendOnErrorFunction = executor.getFunction(RecordDenormalizeTL.APPEND_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING);
		transformFunction = executor.getFunction(RecordDenormalizeTL.TRANSFORM_FUNCTION_NAME);
		transformOnErrorFunction = executor.getFunction(RecordDenormalizeTL.TRANSFORM_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING);
		cleanFunction = executor.getFunction(RecordDenormalizeTL.CLEAN_FUNCTION_NAME);

		if (appendFunction == null) {
			throw new ComponentNotReadyException(RecordDenormalizeTL.APPEND_FUNCTION_NAME + " function must be defined");
		}
		if (transformFunction == null) {
			throw new ComponentNotReadyException(RecordDenormalizeTL.TRANSFORM_FUNCTION_NAME + " function must be defined");
		}

		return true;
	}

	@Override
	public int append(DataRecord inRecord) throws TransformException {
		return appendImpl(appendFunction, inRecord, NO_ARGUMENTS);
	}

	@Override
	public int appendOnError(Exception exception, DataRecord inRecord) throws TransformException {
		if (appendOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Denormalization failed!", exception);
		}

		onErrorArguments[0] = ExceptionUtils.exceptionChainToMessage(null, exception);
		onErrorArguments[1] = ExceptionUtils.stackTraceToString(exception);

		return appendImpl(appendOnErrorFunction, inRecord, onErrorArguments);
	}

	private int appendImpl(CLVFFunctionDeclaration function, DataRecord inRecord, Object[] arguments) {
		inputRecords[0] = inRecord;

		Object result = executor.executeFunction(function, arguments, inputRecords, NO_DATA_RECORDS);

		if (result == null || !(result instanceof Integer)) {
			throw new TransformLangExecutorRuntimeException(function.getName() + "() function must return 'int'");
		}

		return (Integer) result;
	}

	@Override
	public int transform(DataRecord outRecord) throws TransformException {
		return transformImpl(transformFunction, outRecord, NO_ARGUMENTS);
	}

	@Override
	public int transformOnError(Exception exception, DataRecord outRecord) throws TransformException {
		if (transformOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Denormalization failed!", exception);
		}

		onErrorArguments[0] = ExceptionUtils.exceptionChainToMessage(null, exception);
		onErrorArguments[1] = ExceptionUtils.stackTraceToString(exception);

		return transformImpl(transformOnErrorFunction, outRecord, onErrorArguments);
	}

	private int transformImpl(CLVFFunctionDeclaration function, DataRecord outRecord, Object[] arguments) {
		outputRecords[0] = outRecord;

		Object result = executor.executeFunction(function, arguments, NO_DATA_RECORDS, outputRecords);

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
