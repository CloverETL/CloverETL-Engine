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
 * @author dpavlis
 * @since June 25, 2006
 * @revision $Revision: $
 * @created June 25, 2006
 * @see org.jetel.component.RecordTransform
 */
public final class CTLRecordTransformAdapter extends CTLAbstractTransformAdapter implements RecordTransform {

	private final Object[] onErrorArguments = new Object[2];

	private CLVFFunctionDeclaration transformFunction;
	private CLVFFunctionDeclaration transformOnErrorFunction;

    /**
     * Constructs a <code>CTLRecordTransformAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLRecordTransformAdapter(TransformLangExecutor executor, Log logger) {
		super(executor, logger);
	}

    /**
     * Constructs a <code>CTLRecordTransformAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLRecordTransformAdapter(TransformLangExecutor executor, Logger logger) {
		super(executor, LogFactory.getLog(logger.getName()));
	}

	/**
	 * Performs any necessary initialization before transform() method is called
	 * 
	 * @param sourceMetadata
	 *            Array of metadata objects describing source data records
	 * @param targetMetadata
	 *            Array of metadata objects describing source data records
	 * @return True if successful, otherwise False
	 */
	@Override
	public boolean init(Properties parameters, DataRecordMetadata[] sourceRecordsMetadata,
			DataRecordMetadata[] targetRecordsMetadata) throws ComponentNotReadyException {
        // initialize global scope and call user initialization function
		super.init();

		transformFunction = executor.getFunction(RecordTransformTL.TRANSFORM_FUNCTION_NAME);
		transformOnErrorFunction = executor.getFunction(RecordTransformTL.TRANSFORM_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING);

		if (transformFunction == null) {
			throw new ComponentNotReadyException(RecordTransformTL.TRANSFORM_FUNCTION_NAME + " function must be defined");
		}

		return true;
	}

	@Override
	public int transform(DataRecord[] inputRecords, DataRecord[] outputRecords) throws TransformException {
		return transformImpl(transformFunction, inputRecords, outputRecords, NO_ARGUMENTS);
	}

	@Override
	public int transformOnError(Exception exception, DataRecord[] inputRecords, DataRecord[] outputRecords)
			throws TransformException {
		if (transformOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Transform failed!", exception);
		}

		onErrorArguments[0] = ExceptionUtils.exceptionChainToMessage(null, exception);
		onErrorArguments[1] = ExceptionUtils.stackTraceToString(exception);

		return transformImpl(transformOnErrorFunction, inputRecords, outputRecords, onErrorArguments);
	}

	private int transformImpl(CLVFFunctionDeclaration function, DataRecord[] inputRecords, DataRecord[] outputRecords,
			Object[] arguments) {
		Object result = executor.executeFunction(function, arguments, inputRecords, outputRecords);

		if (result == null || result instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException(function.getName() + "() function must return 'int'");
		}

		return (Integer) result;
	}

	@Override
	public void signal(Object signalObject) {
		// does nothing
	}

	@Override
	public Object getSemiResult() {
		return null;
	}

}
