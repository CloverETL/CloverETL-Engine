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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.jetel.ctl.CTLAbstractTransformAdapter;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.DataRecord;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.ExceptionUtils;

/**
 * Implementation of {@link GenericTransform} interface which is used for CTL2.
 * Currently not used since GenericComponent doesn't support CTL.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 1. 2015
 */
public final class CTLGenericTransformAdapter extends CTLAbstractTransformAdapter implements GenericTransform {

	private final Object[] onErrorArguments = new Object[2];

	private CLVFFunctionDeclaration executeFunction;
	private CLVFFunctionDeclaration executeOnErrorFunction;
	private CLVFFunctionDeclaration freeFunction;

	private DataRecord[] inputRecords;
	
	private DataRecord[] outputRecords;
	
    /**
     * Constructs a <code>CTLRecordTransformAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLGenericTransformAdapter(TransformLangExecutor executor, Log logger) {
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
	public CTLGenericTransformAdapter(TransformLangExecutor executor, Logger logger) {
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
	public final void init() {
        // initialize global scope and call user initialization function
		try {
			super.init();
		} catch (Exception e) {
			throw new JetelRuntimeException("GenericComponent failed!", e);
		}
		
		executeFunction = executor.getFunction(CTLGenericTransform.EXECUTE_FUNCTION_NAME);
		executeOnErrorFunction = executor.getFunction(CTLGenericTransform.EXECUTE_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING);
		
		freeFunction = executor.getFunction(CTLGenericTransform.FREE_FUNCTION_NAME);

		if (executeFunction == null) {
			throw new JetelRuntimeException(CTLGenericTransform.EXECUTE_FUNCTION_NAME + " function must be defined");
		}
	}

	@Override
	public void execute() {
		executor.executeFunction(executeFunction, NO_ARGUMENTS, inputRecords, outputRecords);
	}

	@Override
	public void executeOnError(Exception exception) {
		if (executeOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new JetelRuntimeException("GenericComponent failed!", exception);
		}

		onErrorArguments[0] = TransformUtils.getMessage(exception);
		onErrorArguments[1] = ExceptionUtils.stackTraceToString(exception);

		executor.executeFunction(executeOnErrorFunction, onErrorArguments, inputRecords, outputRecords);
	}

	@Override
	public void free() {
		if (freeFunction != null) {
			executor.executeFunction(freeFunction, NO_ARGUMENTS, inputRecords, outputRecords);
		}
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		// no action, only java is supported in GenericComponent for now
		return status;
	}

}
