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

import org.jetel.ctl.CTLAbstractTransform;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.TransformException;
import org.jetel.graph.Node;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;

/**
 * Implementation of {@link GenericTransform} interface which is used for CTL2 compile mode.
 * Currently not used since GenericComponent doesn't support CTL.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 5. 1. 2015
 */
public abstract class CTLGenericTransform extends CTLAbstractTransform implements GenericTransform {

    public static final String EXECUTE_FUNCTION_NAME = "execute";

    public static final String EXECUTE_ON_ERROR_FUNCTION_NAME = "executeOnError";
    
    public static final String FREE_FUNCTION_NAME = "free";

	/** Input data records used for transform, or <code>null</code> if not accessible. */
	private DataRecord[] inputRecords = null;
	/** Output data records used for transform, or <code>null</code> if not accessible. */
	private DataRecord[] outputRecords = null;

	@Override
	public final void init() {
		try {
			globalScopeInit();

			if (!initDelegate()) {
				throw new JetelRuntimeException("GenericComponent initialisation failed.");
			}
			
			Node component = getNode();
			//prepare input records
			DataRecordMetadata[] inMetadata = component.getInMetadataArray();
			inputRecords = new DataRecord[inMetadata.length];
			int i = 0;
			for (DataRecordMetadata metadata : inMetadata) {
				inputRecords[i] = DataRecordFactory.newRecord(metadata);
				i++;
			}
			//prepare output records
			DataRecordMetadata[] outMetadata = component.getOutMetadataArray();
			outputRecords = new DataRecord[outMetadata.length];
			i = 0;
			for (DataRecordMetadata metadata : outMetadata) {
				outputRecords[i] = DataRecordFactory.newRecord(metadata);
				i++;
			}
		} catch (Exception e) {
			throw new JetelRuntimeException("GenericComponent initialisation failed.", e);
		}
	}

	/**
	 * Called by {@link #init(Properties, DataRecordMetadata[], DataRecordMetadata[])} to perform user-specific
	 * initialization defined in the CTL transform. The default implementation does nothing, may be overridden
	 * by the generated transform class.
	 *
	 * @return <code>true</code> on success, <code>false</code> otherwise
	 *
	 * @throws ComponentNotReadyException if the initialization fails
	 */
	@CTLEntryPoint(name = INIT_FUNCTION_NAME, required = false)
	protected Boolean initDelegate() throws ComponentNotReadyException {
		// does nothing and succeeds by default, may be overridden by generated transform classes
		return true;
	}

	@Override
	public final void execute() {
		try {
			executeDelegate();
		} catch (Exception exception) {
			// the exception may be thrown by lookups, sequences, etc.
			throw new JetelRuntimeException("Generated execute class threw an exception!", exception);
		}
	}

	/**
	 * Called by {@link #transform(DataRecord[], DataRecord[])} to transform data records in a user-specific way
	 * defined in the CTL transform. Has to be overridden by the generated transform class.
	 *
	 * @throws ComponentNotReadyException if some internal initialization failed
	 * @throws TransformException if an error occurred
	 */
	@CTLEntryPoint(name = EXECUTE_FUNCTION_NAME, required = true)
	protected abstract void executeDelegate();

	@Override
	public void executeOnError(Exception exception) {
		try {
			executeOnErrorDelegate(TransformUtils.getMessage(exception), ExceptionUtils.stackTraceToString(exception));
		} catch (UnsupportedOperationException ex) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new JetelRuntimeException("GenericComponent failed!", exception);
		} catch (Exception ex) {
			// the exception may be thrown by lookups, sequences, etc.
			throw new JetelRuntimeException("Generated transform class threw an exception!", ex);
		}
	}

	/**
	 * Called by {@link #transformOnError(Exception, DataRecord[], DataRecord[])} to transform data records in
	 * a user-specific way defined in the CTL transform. May be overridden by the generated transform class.
	 * Throws <code>UnsupportedOperationException</code> by default.
	 *
	 * @param errorMessage an error message of the error message that occurred
	 * @param stackTrace a stack trace of the error message that occurred
	 *
	 * @throws ComponentNotReadyException if some internal initialization failed
	 * @throws TransformException if an error occurred
	 */
	@CTLEntryPoint(name = EXECUTE_ON_ERROR_FUNCTION_NAME, parameterNames = {
			ERROR_MESSAGE_PARAM_NAME, STACK_TRACE_PARAM_NAME }, required = false)
	protected void executeOnErrorDelegate(String errorMessage, String stackTrace) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected final DataRecord getInputRecord(int index) {
		if (inputRecords == null) {
			throw new TransformLangExecutorRuntimeException(INPUT_RECORDS_NOT_ACCESSIBLE);
		}

		if (index < 0 || index >= inputRecords.length) {
			throw new TransformLangExecutorRuntimeException(new Object[] { index }, INPUT_RECORD_NOT_DEFINED);
		}

		return inputRecords[index];
	}

	@Override
	protected final DataRecord getOutputRecord(int index) {
		if (outputRecords == null) {
			throw new TransformLangExecutorRuntimeException(OUTPUT_RECORDS_NOT_ACCESSIBLE);
		}

		if (index < 0 || index >= outputRecords.length) {
			throw new TransformLangExecutorRuntimeException(new Object[] { index }, OUTPUT_RECORD_NOT_DEFINED);
		}

		return outputRecords[index];
	}

	@Override
	public void free() {
		if (!freeDelegate()) {
			throw new JetelRuntimeException("GenericComponent free() failed.");
		}
	}
	
	@CTLEntryPoint(name = FREE_FUNCTION_NAME, required = false)
	protected Boolean freeDelegate() {
		// does nothing and succeeds by default, may be overridden by generated transform classes
		return true;
	}
	
	
}
