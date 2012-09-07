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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.data.TLValue;

/**
 * Base class of all TL wrapper classes.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 22nd June 2010
 * @created 11th June 2010
 */
public abstract class AbstractTransformTL implements Transform {

    /** the name of the init() function in CTL */
	public static final String INIT_FUNCTION_NAME = "init";
    /** the name of the preExecute() function in CTL */
	public static final String PRE_EXECUTE_FUNCTION_NAME = "preExecute";
    /** the name of the postExecute() function in CTL */
	public static final String POST_EXECUTE_FUNCTION_NAME = "postExecute";
    /** the name of the getMessage() function in CTL */
	public static final String GET_MESSAGE_FUNCTION_NAME = "getMessage";

    /** the name of the finished() function in CTL */
	public static final String FINISHED_FUNCTION_NAME = "finished";
    /** the name of the reset() function in CTL */
	public static final String RESET_FUNCTION_NAME = "reset";

    /** the name of the errorMessage param used in CTL */
    public static final String ERROR_MESSAGE_PARAM_NAME = "errorMessage";
    /** the name of the stackTrace param used in CTL */
    public static final String STACK_TRACE_PARAM_NAME = "stackTrace";

    /** A graph node associated with this CTL transform used to query graph, LUTs, sequences, etc.. */
	private Node node;

    /** the TL wrapper used to execute all the functions */
	protected WrapperTL wrapper;
    /** the logger for this instance */
    protected Log logger;

    /** run-time error message that might be set by extending classes */
    protected String errorMessage;
    /** the semi result */
    protected TLValue semiResult;

    /**
     * Creates an instance of the <code>AbstractTransformTL</code> class.
     *
     * @param sourceCode the source code of the transformation
     * @param logger the logger to be used by this TL wrapper
     */
    public AbstractTransformTL(String sourceCode, Log logger) {
		this.wrapper = new WrapperTL(sourceCode, logger);
		this.logger = logger;
    }

    /**
     * Creates an instance of the <code>AbstractTransformTL</code> class.
     *
     * @param sourceCode the source code of the transformation
     * @param logger the logger to be used by this TL wrapper
     */
    public AbstractTransformTL(String sourceCode, Logger logger) {
    	this(sourceCode, LogFactory.getLog(logger.getName()));
    }

    @Override
	public void setNode(Node node) {
		this.node = node;
	}

	@Override
	public Node getNode() {
		return node;
	}

	@Override
	public TransformationGraph getGraph() {
		return (node != null) ? node.getGraph() : null;
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		semiResult = null;

		try {
			semiResult = wrapper.execute(PRE_EXECUTE_FUNCTION_NAME, null);
		} catch (JetelException e) {
			// do nothing, the preExecute() function is optional
		}
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		semiResult = null;

		try {
			semiResult = wrapper.execute(POST_EXECUTE_FUNCTION_NAME, null);
		} catch (JetelException e) {
			// do nothing, the postExecute() function is optional
		}
	}

	@Override
	public String getMessage() {
		semiResult = null;

		if (errorMessage != null) {
			return errorMessage;
		}

		TLValue result = null;

		try {
			result = wrapper.execute(GET_MESSAGE_FUNCTION_NAME, null);
        } catch (JetelException exception) {
			// do nothing, the getMessage() function is optional
        }

        return ((result != null) ? result.toString() : null);
	}

	/**
	 * @deprecated Use {@link #postExecute()} method.
	 */
	@Deprecated
	@Override
	public void finished(){
		semiResult = null;

		try {
			semiResult = wrapper.execute(FINISHED_FUNCTION_NAME, null);
			logger.warn("CTL function " + FINISHED_FUNCTION_NAME + "() is deprecated, use "
					+ POST_EXECUTE_FUNCTION_NAME + "() instead!");
		} catch (JetelException e) {
			// do nothing, the finished() function is optional
		}
	}

	/**
	 * @deprecated Use {@link #preExecute()} method.
	 */
	@Deprecated
	@Override
	public void reset() {
		semiResult = null;

		try {
			semiResult = wrapper.execute(RESET_FUNCTION_NAME, null);
			logger.warn("CTL function " + RESET_FUNCTION_NAME + "() is deprecated, use "
					+ PRE_EXECUTE_FUNCTION_NAME + "() instead!");
		} catch (JetelException e) {
			// do nothing, the reset() function is optional
		}
	}

}
