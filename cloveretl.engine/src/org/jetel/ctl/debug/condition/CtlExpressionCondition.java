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

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.jetel.ctl.DebugTransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.Node;
import org.jetel.ctl.ASTnode.SimpleNode;
import org.jetel.ctl.debug.CTLExpressionHelper;

/**
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.7.2016
 */
public abstract class CtlExpressionCondition implements Condition {

	protected static final long EXPRESSION_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
	
	protected String expression;
	private List<Node> nodes;
	
	public CtlExpressionCondition(String expression) {
		super();
		this.expression = expression;
	}
	
	protected List<Node> getExpression(DebugTransformLangExecutor executor, SimpleNode context) throws TransformLangExecutorRuntimeException {
		if (nodes == null) {
			nodes = CTLExpressionHelper.compileExpression(expression, executor, context);
		}
		return nodes;
	}
}
