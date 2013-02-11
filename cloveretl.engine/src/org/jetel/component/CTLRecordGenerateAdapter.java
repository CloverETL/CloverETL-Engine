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
 * @created March 25, 2009
 * @see org.jetel.component.RecordGenerate
 */
public class CTLRecordGenerateAdapter extends CTLAbstractTransformAdapter implements RecordGenerate {

	private final Object[] onErrorArguments = new Object[2];

	private CLVFFunctionDeclaration generateFunction;
	private CLVFFunctionDeclaration generateOnErrorFunction;

    /**
     * Constructs a <code>CTLRecordGenerateAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLRecordGenerateAdapter(TransformLangExecutor executor, Logger logger) {
		super(executor, logger);
	}

	/**
	 * Performs any necessary initialization before generate() method is called
	 * 
	 * @param targetMetadata
	 *            Array of metadata objects describing source data records
	 * @return True if successfull, otherwise False
	 */
	@Override
	public boolean init(Properties parameters, DataRecordMetadata[] targetRecordsMetadata)
			throws ComponentNotReadyException {
        // initialize global scope and call user initialization function
		super.init();

		generateFunction = executor.getFunction(RecordGenerateTL.GENERATE_FUNCTION_NAME);
		generateOnErrorFunction = executor.getFunction(RecordGenerateTL.GENERATE_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING);

		if (generateFunction  == null) {
			throw new ComponentNotReadyException(RecordGenerateTL.GENERATE_FUNCTION_NAME + " function is not defined");
		}

		return true;
	}

	/**
	 * Generate data for output records.
	 */
	@Override
	public int generate(DataRecord[] outputRecords) throws TransformException {
		return generateImpl(generateFunction, outputRecords, NO_ARGUMENTS);
	}

	@Override
	public int generateOnError(Exception exception, DataRecord[] outputRecords) throws TransformException {
		if (generateOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Generate failed!", exception);
		}

		onErrorArguments[0] = ExceptionUtils.exceptionChainToMessage(null, exception);
		onErrorArguments[1] = ExceptionUtils.stackTraceToString(exception);

		return generateImpl(generateOnErrorFunction, outputRecords, onErrorArguments);
	}

	private int generateImpl(CLVFFunctionDeclaration function, DataRecord[] outputRecords, Object[] arguments) {
		Object result = executor.executeFunction(function, arguments, NO_DATA_RECORDS, outputRecords);

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
