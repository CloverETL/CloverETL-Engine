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
package org.jetel.ctl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.graph.TransformationGraph;

/**
 * Represents a tool for evaluation of simple CTL expressions.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 3rd August 2009
 * @since 29th July 2009
 */
public class CTLExpressionEvaluator {

	/** the logger for this class */
	private static final Log logger = LogFactory.getLog(CTLExpressionEvaluator.class);

	/** transform language compiler used to compile expressions */
	private final ITLCompiler compiler;

	/**
	 * Constructs a standalone <code>CTLExpressionEvaluator</code>. References to any graph entities within the CTL
	 * expressions will be considered as errors.
	 */
	public CTLExpressionEvaluator() {
		// TLCompiler is used to ensure that expressions are evaluated in interpreted mode
		this.compiler = new TLCompiler();
	}

	/**
	 * Constructs a <code>CTLExpressionEvaluator</code> for the given transformation graph. This allows the CTL
	 * expressions to contain references to any graph entities.
	 *
	 * @param graph a transformation graph to be used
	 *
	 * @throws NullPointerException if the given transformation graph is <code>null</code>
	 */
	public CTLExpressionEvaluator(TransformationGraph graph) {
		// TLCompiler is used to ensure that expressions are evaluated in interpreted mode
		this.compiler = new TLCompiler(graph, null, null);
	}

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
		// validate the expression to check that its syntax is correct
		compiler.validateExpression(expression);

		// in case of invalid syntax, log errors/warnings and stop any further processing
		if (compiler.errorCount() != 0) {
			for (ErrorMessage errorMessage : compiler.getDiagnosticMessages()) {
				if (errorMessage.getErrorLevel() == ErrorMessage.ErrorLevel.ERROR) {
					logger.error(errorMessage);
				} else {
					logger.warn(errorMessage);
				}
			}

			throw new ParseException("The expression contains " + compiler.errorCount() + " error(s)!");
		}

		// log any warnings that might be present
		for (ErrorMessage errorMessage : compiler.getDiagnosticMessages()) {
			logger.warn(errorMessage);
		}

		// syntax is correct, execute the expression to be able to get its result
		TransformLangExecutor executor = (TransformLangExecutor) compiler.getCompiledCode();
		executor.execute();

		return String.valueOf(executor.getResult());
	}

}
