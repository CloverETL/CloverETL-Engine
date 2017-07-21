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
package org.jetel.ctl.debug.condition;

import org.apache.commons.lang.ObjectUtils;
import org.jetel.ctl.DebugTransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.SimpleNode;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.7.2016
 */
public class ValueChangeCondition extends CTLExpressionCondition {

	private Object prevValue;
	private Object curValue;
	
	public ValueChangeCondition(String expression) {
		super(expression);
	}

	@Override
	public boolean isFulFilled() {
		return !ObjectUtils.equals(prevValue, curValue);
	}

	@Override
	public void evaluate(DebugTransformLangExecutor executor, SimpleNode context) throws TransformLangExecutorRuntimeException {
		this.prevValue = curValue;
		this.curValue = executor.evaluateExpression(getExpression(executor, context), EXPRESSION_TIMEOUT_NS);
	}
}
