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
package org.jetel.interpreter;

import org.jetel.interpreter.ASTnode.CLVFStartExpression;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Represents a tool for evaluation of simple CTL expressions.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 17th August 2009
 * @since 17th August 2009
 */
public class CTLExpressionEvaluator {

	/**
	 * Evaluates a given expression in interpreted mode and returns its result as a string. This method may be called
	 * repeatedly for different expressions.
	 *
	 * @param expression an expression to be evaluated
	 *
	 * @return the result of evaluation of the expression
	 *
	 * @throws NullPointerException if the given expression is <code>null</code>
	 * @throws ParseException if the given expression is invalid
	 */
	public String evaluate(String expression) throws ParseException {
		TransformLangParser parser = new TransformLangParser((DataRecordMetadata) null, expression);

		// validate the expression to check that its syntax is correct
		CLVFStartExpression parseTree = parser.StartExpression();
		parseTree.init();

		// syntax is correct, execute the expression to be able to get its result
		TransformLangExecutor executor = new TransformLangExecutor();
		executor.visit(parseTree, null);

		return String.valueOf(executor.getResult());
	}

}
