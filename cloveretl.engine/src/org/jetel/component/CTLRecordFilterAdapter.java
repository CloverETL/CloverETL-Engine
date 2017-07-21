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
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;

public class CTLRecordFilterAdapter implements RecordFilter {

	public static final String ISVALID_FUNCTION_NAME = "isValid";
	private static final Object[] EMPTY_ARGUMENTS = new Object[0];

	private CLVFFunctionDeclaration valid;
	
	private DataRecord[] sourceRec;
	private Log logger;
	private TransformLangExecutor executor;
	
	/** Constructor for the DataRecordTransform object */
	public CTLRecordFilterAdapter(TransformLangExecutor executor, Log logger) {
		this.logger = logger;
		this.executor = executor;
		sourceRec = new DataRecord[1];
	}

	@Override
	public void setGraph(TransformationGraph graph) {
		// do nothing
	}

	@Override
	public void init() throws ComponentNotReadyException {
		
		// we will be running in one-function-a-time so we need global scope active
		executor.keepGlobalScope();
		executor.init();
		
		this.valid = executor.getFunction(ISVALID_FUNCTION_NAME);
		
		if (valid == null) {
			throw new ComponentNotReadyException(ISVALID_FUNCTION_NAME + " function is not defined");
		}

		try {
			global();
		} catch (TransformLangExecutorRuntimeException e) {
			if (logger != null) {
				logger.warn("Failed to initialize global scope", e);
			}
			throw new ComponentNotReadyException("Failed to initialize global scope", e);
		}
		
 	}

	public void global() {
			// execute code in global scope
			executor.execute();
	}
	
	@Override
	public boolean isValid(DataRecord source) {
		sourceRec[0] = source;
		// pass source as function parameter, but also as a global record
		final Object retVal = executor.executeFunction(valid, EMPTY_ARGUMENTS, sourceRec, null);
		if (retVal == null || retVal instanceof Boolean == false) {
			throw new TransformLangExecutorRuntimeException(ISVALID_FUNCTION_NAME + "() function must return 'boolean'");
		}
		return (Boolean)retVal;
	}
	

}
