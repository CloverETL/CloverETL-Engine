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
package org.jetel.graph.parameter;

import org.jetel.ctl.CTLAbstractTransform;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;

/**
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 30. 4. 2014
 */
public abstract class CTLGraphParameterValueTransform extends CTLAbstractTransform implements GraphParameterValueFunction {

	@Override
	public void init() throws ComponentNotReadyException {
		globalScopeInit();
		initDelegate();
	}
	
	@CTLEntryPoint(name = INIT_FUNCTION_NAME, parameterNames = {}, required = false)
	protected void initDelegate() throws ComponentNotReadyException {
	}

	@CTLEntryPoint(name = GET_PARAMETER_VALUE_FUNCTION_NAME, required = true)
	protected abstract String getParameterValueDelegate() throws ComponentNotReadyException, TransformException;
	
	@Override
	public String getValue() throws TransformException {
		String result;
		try {
			result = getParameterValueDelegate();
		} catch (ComponentNotReadyException exception) {
			// the exception may be thrown by lookups, sequences, etc.
			throw new TransformException("Generated transform class threw an exception!", exception);
		}
		return result;
	}

	@Override
	protected DataRecord getInputRecord(int index) {
		throw new TransformLangExecutorRuntimeException(INPUT_RECORDS_NOT_ACCESSIBLE);
	}

	@Override
	protected DataRecord getOutputRecord(int index) {
		throw new TransformLangExecutorRuntimeException(OUTPUT_RECORDS_NOT_ACCESSIBLE);
	}

}
