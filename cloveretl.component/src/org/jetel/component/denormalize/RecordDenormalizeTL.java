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
import org.jetel.component.AbstractTransformTL;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MiscUtils;

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
 * @since 11/21/06
 * @see org.jetel.component.Normalizer
 */
public class RecordDenormalizeTL extends AbstractTransformTL implements RecordDenormalize {

	public static final String APPEND_FUNCTION_NAME = "append";
	public static final String APPEND_ON_ERROR_FUNCTION_NAME = "appendOnError";
	public static final String TRANSFORM_FUNCTION_NAME = "transform";
	public static final String TRANSFORM_ON_ERROR_FUNCTION_NAME = "transformOnError";
	public static final String CLEAN_FUNCTION_NAME = "clean";

	private final DataRecord[] outRec = new DataRecord[1];

	protected final TLValue[] onErrorArguments = new TLValue[] { 
			new TLStringValue(), new TLStringValue(), TLValue.create(TLValueType.INTEGER)};

	protected int appendFunction;
	protected int appendOnErrorFunction;
	protected int transformFunction;
	protected int transformOnErrorFunction;
	protected int cleanFunction;

	/** Constructor for the DataRecordTransform object */
	public RecordDenormalizeTL(Logger logger, String srcCode) {
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

		appendFunction = wrapper.prepareFunctionExecution(APPEND_FUNCTION_NAME);
		appendOnErrorFunction = wrapper.prepareOptionalFunctionExecution(APPEND_ON_ERROR_FUNCTION_NAME);
		transformFunction = wrapper.prepareFunctionExecution(TRANSFORM_FUNCTION_NAME);
		transformOnErrorFunction = wrapper.prepareOptionalFunctionExecution(TRANSFORM_ON_ERROR_FUNCTION_NAME);
		cleanFunction = wrapper.prepareOptionalFunctionExecution(CLEAN_FUNCTION_NAME);

		return result == null ? true : result == TLBooleanValue.TRUE;
	}

	@Override
	public int append(DataRecord inRecord) throws TransformException {
		return appendImpl(appendFunction, inRecord, null);
	}

	@Override
	public int appendOnError(Exception exception, DataRecord inRecord) throws TransformException {
		if (appendOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Denormalization failed!", exception);
		}

		onErrorArguments[0].setValue(exception.getMessage());
		onErrorArguments[1].setValue(MiscUtils.stackTraceToString(exception));

		return appendImpl(appendOnErrorFunction, inRecord, onErrorArguments);
	}

	private int appendImpl(int function, DataRecord inRecord, TLValue[] arguments) {
		return convertResult(wrapper.executePreparedFunction(function, inRecord, arguments));
	}

	@Override
	public int transform(DataRecord outRecord) throws TransformException {
		return transformImpl(transformFunction, outRecord, null);
	}

	@Override
	public int transformOnError(Exception exception, DataRecord outRecord) throws TransformException {
		if (transformOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Denormalization failed!", exception);
		}

		onErrorArguments[0].setValue(exception.getMessage());
		onErrorArguments[1].setValue(MiscUtils.stackTraceToString(exception));

		return transformImpl(transformFunction, outRecord, onErrorArguments);
	}

	private int transformImpl(int function, DataRecord outRecord, TLValue[] arguments) {
		this.outRec[0] = outRecord;

		return convertResult(wrapper.executePreparedFunction(function, null, outRec, arguments));
	}

	private int convertResult(TLValue result) {
		// set the error message to null so that the getMessage() method works correctly if no error occurs
		errorMessage = null;

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
