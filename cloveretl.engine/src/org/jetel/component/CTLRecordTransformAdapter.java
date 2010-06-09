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

import org.apache.commons.logging.Log;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  
 *
 * @author      dpavlis
 * @since       June 25, 2006
 * @revision    $Revision: $
 * @created     June 25, 2006
 * @see         org.jetel.component.RecordTransform
 */

public final class CTLRecordTransformAdapter implements RecordTransform {

    public static final String TRANSFORM_FUNCTION_NAME="transform";
    public static final String GENERATE_FUNCTION_NAME="generate";
    public static final String FINISHED_FUNCTION_NAME="finished";
    public static final String INIT_FUNCTION_NAME="init";
    public static final String RESET_FUNCTION_NAME="reset";
    
    private static final Object[] EMPTY_ARGUMENTS = new Object[0];
    
    private TransformationGraph graph;
    private Log logger;

    private String errorMessage;
	 
	private boolean semiResult;
	private final TransformLangExecutor executor;
	private CLVFFunctionDeclaration init;
	private CLVFFunctionDeclaration transform;
	private CLVFFunctionDeclaration finished;
	private CLVFFunctionDeclaration generate;
	private CLVFFunctionDeclaration reset;
	
	
	
    /**Constructor for the DataRecordTransform object */
    public CTLRecordTransformAdapter(TransformLangExecutor executor, Log logger) {
        this.logger=logger;
        this.executor = executor;
    }

	/**
	 *  Performs any necessary initialization before transform() method is called
	 *
	 * @param  sourceMetadata  Array of metadata objects describing source data records
	 * @param  targetMetadata  Array of metadata objects describing source data records
	 * @return                        True if successful, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] sourceRecordsMetadata, DataRecordMetadata[] targetRecordsMetadata)
			throws ComponentNotReadyException{

		// we will be running in one-function-a-time so we need global scope active
		executor.keepGlobalScope();
		executor.init();
		
		this.init = executor.getFunction(INIT_FUNCTION_NAME);
		this.transform = executor.getFunction(TRANSFORM_FUNCTION_NAME);
		this.finished = executor.getFunction(FINISHED_FUNCTION_NAME);
		this.generate = executor.getFunction(GENERATE_FUNCTION_NAME);
		this.reset = executor.getFunction(RESET_FUNCTION_NAME);
		
		if (transform == null && generate == null) {
			throw new ComponentNotReadyException( 
					GENERATE_FUNCTION_NAME + " or " + TRANSFORM_FUNCTION_NAME 
					+ " function must be defined");
		}
		
		boolean retVal = true;
		try {
			global();
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to initialize global scope: " + e.getMessage());
			retVal = false;
		}
		
		try {
			// initialize global scope
			// call user-init function afterwards
			if (init != null) {
				executor.executeFunction(init, EMPTY_ARGUMENTS);
			}
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to execute " + INIT_FUNCTION_NAME + "() function: " + e.getMessage());
			retVal = false;
		}
		
		return retVal;
 	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
	}
	
	public void global() {
			// execute code in global scope
			executor.execute();
	}
	
	public int transform(DataRecord[] inputRecords, DataRecord[] outputRecords) throws TransformException {
		final Object retVal = executor.executeFunction(transform, EMPTY_ARGUMENTS, inputRecords, outputRecords);
		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException("transform() function must return 'int'");
		}
		return (Integer)retVal;
	}


	/**
	 *  Returns description of error if one of the methods failed
	 *
	 * @return    Error message
	 * @since     April 18, 2002
	 */
	public String getMessage() {
		return errorMessage;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#signal()
	 * In this implementation does nothing.
	 */
	public void signal(Object signalObject){
		
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#getSemiResult()
	 */
	public Object getSemiResult(){
		return semiResult;
	}
	
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordTransform#finished()
	 */
	public void finished(){
		if (finished == null){
			return;
		}
        // execute finished transformFunction
		try {
			executor.executeFunction(finished,EMPTY_ARGUMENTS);
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to execute " + FINISHED_FUNCTION_NAME + "() function: " + e.getMessage());
		}
	}
	
    /* (non-Javadoc)
     * @see org.jetel.component.RecordTransform#setGraph(org.jetel.graph.TransformationGraph)
     */
    public void setGraph(TransformationGraph graph) {
        this.graph = graph;
    }

    /* (non-Javadoc)
     * @see org.jetel.component.RecordTransform#getGraph()
     */
    public TransformationGraph getGraph() {
        return graph;
    }

    /*
     * (non-Javadoc)
     * @see org.jetel.component.RecordTransform#reset()
     */
	public void reset() {
		if (reset == null) {
			return;
		}
		
        // execute reset transformFunction
		try {
			executor.executeFunction(reset,EMPTY_ARGUMENTS);
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to execute " + RESET_FUNCTION_NAME + "() function: " + e.getMessage());
		}
	}
	
}

