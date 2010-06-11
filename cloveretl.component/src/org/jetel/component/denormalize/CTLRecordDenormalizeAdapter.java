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
package org.jetel.component.denormalize;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Implements denormalization based on TransformLang source specified by user.
 * User defines following functions (asterisk denotes the mandatory ones):<ul>
 * <li>* function append()</li>
 * <li>* function transform()</li>
 * <li>function init()</li> 
 * <li>function finished()</li>
 * </ul>
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 * @since 11/21/06  
 * @see org.jetel.component.Normalizer
 */
public class CTLRecordDenormalizeAdapter implements RecordDenormalize {

	private static final String APPEND_FUNCTION_NAME="append";
	private static final String TRANSFORM_FUNCTION_NAME="transform";
    private static final String FINISHED_FUNCTION_NAME="finished";
    private static final String INIT_FUNCTION_NAME="init";
    private static final String CLEAN_FUNCTION_NAME="clean";
	private static final String ADDINPUT_FUNCTION_NAME="append";
	private static final String GETOUTPUT_FUNCTION_NAME="transform";
	private static final Object[] EMPTY_ARGUMENTS = new Object[0];
	
    private String errorMessage;
	private Log logger;
	private TransformLangExecutor executor;
	
	private CLVFFunctionDeclaration init;
	private CLVFFunctionDeclaration transform;
	private CLVFFunctionDeclaration finished;
	private CLVFFunctionDeclaration append;
	private CLVFFunctionDeclaration clean;
	private CLVFFunctionDeclaration addInput;
	private CLVFFunctionDeclaration getOutput;

	private DataRecord[] inputRecords = new DataRecord[1];
	private DataRecord[] outputRecords = new DataRecord[1];
	private TransformationGraph graph;
	
	
    /**Constructor for the DataRecordTransform object */
    public CTLRecordDenormalizeAdapter(TransformLangExecutor executor, Log logger) {
    	this.executor = executor;
    	this.logger = logger;
    }

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordDenormalize#init(java.util.Properties, org.jetel.metadata.DataRecordMetadata, org.jetel.metadata.DataRecordMetadata)
	 */
	public boolean init(Properties parameters,
			DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		
		// we will be running in one-function-a-time so we need global scope active
		executor.keepGlobalScope();
		executor.init();
		
		this.init = executor.getFunction(INIT_FUNCTION_NAME);
		this.transform = executor.getFunction(TRANSFORM_FUNCTION_NAME);
		this.finished = executor.getFunction(FINISHED_FUNCTION_NAME);
		this.append = executor.getFunction(APPEND_FUNCTION_NAME);
		this.clean = executor.getFunction(CLEAN_FUNCTION_NAME);
		this.addInput = executor.getFunction(ADDINPUT_FUNCTION_NAME);
		this.getOutput = executor.getFunction(GETOUTPUT_FUNCTION_NAME);
		
		if (addInput == null) {
			throw new ComponentNotReadyException(ADDINPUT_FUNCTION_NAME + " function must be defined");
		}
		if (getOutput == null) {
			throw new ComponentNotReadyException(GETOUTPUT_FUNCTION_NAME + " function must be defined");
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
	 * @see org.jetel.component.denormalize.RecordDenormalize#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.denormalize.RecordDenormalize#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute() throws ComponentNotReadyException {
	}
	
	public void global() {
			// execute code in global scope
			executor.execute();
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordDenormalize#append(org.jetel.data.DataRecord)
	 */
	public int append(DataRecord inRecord) {
		inputRecords[0] = inRecord;
		final CLVFFunctionDeclaration f = append == null ? addInput : append;
		final Object retVal = executor.executeFunction(f, EMPTY_ARGUMENTS,  inputRecords, null);
		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException("append() function must return 'int'");
		}
		return (Integer)retVal;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordDenormalize#transform(org.jetel.data.DataRecord)
	 */
	public int transform(DataRecord outRecord) {
		outputRecords[0]=outRecord;
		final CLVFFunctionDeclaration f = transform == null ? getOutput: transform;
		final Object retVal = executor.executeFunction(f, EMPTY_ARGUMENTS, null, outputRecords);
		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException("transform() function must return 'int'");
		}
		return (Integer)retVal;
	}

	public void clean(){
		if (clean == null) { 
			return;
		} 
		try {
			executor.executeFunction(clean,EMPTY_ARGUMENTS);
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to execute " + CLEAN_FUNCTION_NAME + "() function: " + e.getMessage());
		}
	}
	
	/**
	 * Use postExecuste method.
	 */
	@Deprecated
	@CTLEntryPoint(
			name = "finished",
			required = true
	)
	public void finished() {
		if (finished == null) {
			return; 
		}
		
		try {
			executor.executeFunction(finished, EMPTY_ARGUMENTS);
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to execute " + INIT_FUNCTION_NAME + "() function: " + e.getMessage());
		}
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.RecordDenormalize#getMessage()
	 */
	public String getMessage() {
		return errorMessage;
	}

	/**
	 * Use preExecute method.
	 */
	@Deprecated
	public void reset() {
		errorMessage = null;
	}

	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}
	
	public TransformationGraph getGraph() {
		return graph;
	}
	
}
