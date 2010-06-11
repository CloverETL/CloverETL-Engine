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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.data.TLValue;

/**
 *  
 *
 * @author      dpavlis
 * @since       June 25, 2006
 * @revision    $Revision: $
 * @created     June 25, 2006
 * @see         org.jetel.component.RecordTransform
 */
public class RecordTransformCommonTL {

    public static final String FINISHED_FUNCTION_NAME="finished";
    public static final String INIT_FUNCTION_NAME="init";
    public static final String RESET_FUNCTION_NAME="reset";
    public static final String GET_MESSAGE_FUNCTION_NAME="getMessage";
    public static final String POST_EXECUTE_FUNCTION_NAME = "postExecute";
    public static final String PRE_EXECUTE_FUNCTION_NAME = "preExecute";
    
    protected TransformationGraph graph;
    protected Log logger;

    protected String errorMessage;
	 
	protected WrapperTL wrapper;
	protected TLValue semiResult;

    /**Constructor for the DataRecordTransform object */
    public RecordTransformCommonTL(String srcCode, Log logger) {
        this.logger=logger;
        wrapper = new WrapperTL(srcCode, logger);
    }

	/**
	 * @return an error message if one of the methods failed or if the corresponding CTL function returned one
	 */
	public String getMessage() {
		if (errorMessage != null) {
			return errorMessage;
		}

		TLValue result = null;

		try {
			result = wrapper.execute(GET_MESSAGE_FUNCTION_NAME, null);
        } catch (JetelException exception) {
            // OK, don't do anything, function getMessage() is not necessary
        }

        return ((result != null) ? result.toString() : null);
	}
	
	public void signal(Object signalObject){
	}

	public Object getSemiResult(){
		return semiResult;
	}
	
	/**
	 * Use postExecute method.
	 */
	@Deprecated
	public void finished(){
        // execute finished transformFunction
		semiResult = null;
		try {
			semiResult = wrapper.execute(FINISHED_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function finished is not necessary
		}
	}

    public void setGraph(TransformationGraph graph) {
        this.graph = graph;
    }

    public TransformationGraph getGraph() {
        return graph;
    }

	/**
	 * Use preExecute method.
	 */
	@Deprecated
	public void reset() {
        // execute reset transformFunction
		semiResult = null;
		try {
			semiResult = wrapper.execute(RESET_FUNCTION_NAME,null);
		} catch (JetelException e) {
			//do nothing: function reset is not necessary
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordGenerate#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
        // execute postExecute transformFunction
		semiResult = null;
		try {
			semiResult = wrapper.execute(PRE_EXECUTE_FUNCTION_NAME, null);
		} catch (JetelException e) {
			//do nothing: function postExecute is not necessary
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordGenerate#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute() throws ComponentNotReadyException {
        // execute postExecute transformFunction
		semiResult = null;
		try {
			semiResult = wrapper.execute(POST_EXECUTE_FUNCTION_NAME, null);
		} catch (JetelException e) {
			//do nothing: function postExecute is not necessary
		}
	}
}

