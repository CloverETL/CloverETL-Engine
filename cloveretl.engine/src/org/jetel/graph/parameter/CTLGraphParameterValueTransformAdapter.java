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

import org.apache.log4j.Logger;
import org.jetel.ctl.CTLAbstractTransformAdapter;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.exception.ComponentNotReadyException;

/**
 * Class for executing graph parameter value function written in CTL
 * 
 * @author Raszyk (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 29. 4. 2014
 */
public class CTLGraphParameterValueTransformAdapter extends CTLAbstractTransformAdapter implements GraphParameterValueFunction {

	private CLVFFunctionDeclaration getParameterValueFunction;
	
	public CTLGraphParameterValueTransformAdapter(TransformLangExecutor executor, Logger logger) {
		super(executor, logger);
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		getParameterValueFunction = executor.getFunction(GET_PARAMETER_VALUE_FUNCTION_NAME);
		
		if (getParameterValueFunction == null) {
			throw new ComponentNotReadyException(GET_PARAMETER_VALUE_FUNCTION_NAME + " is not defined");
		}
	}

	@Override
	public String getParameterValue() {
		Object parameterValue = executor.executeFunction(getParameterValueFunction, NO_ARGUMENTS, NO_DATA_RECORDS, NO_DATA_RECORDS);
		
		if (parameterValue == null || !(parameterValue instanceof String)) {
			throw new TransformLangExecutorRuntimeException(getParameterValueFunction.getName() + "() function must return 'string'");
		}
		
		String stringParameterValue = (String) parameterValue;
		
		return stringParameterValue;
	}

}
