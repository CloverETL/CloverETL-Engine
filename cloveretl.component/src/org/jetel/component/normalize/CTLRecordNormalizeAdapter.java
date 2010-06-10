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
package org.jetel.component.normalize;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Implements normalization based on TransformLang source specified by user.
 * User defines following functions (asterisk denotes the mandatory ones):
 * <ul>
 * <li>* function count()</li>
 * <li>* function transform(idx)</li>
 * <li>function init()</li>
 * <li>function finished()</li>
 * </ul>
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting
 *         (www.javlinconsulting.cz)
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 * @since 11/21/06
 * @see org.jetel.component.Normalizer
 */
public class CTLRecordNormalizeAdapter implements RecordNormalize {

	private static final String INIT_FUNCTION_NAME = "init";
	private static final String COUNT_FUNCTION_NAME = "count";
	private static final String TRANSFORM_FUNCTION_NAME = "transform";
	private static final String FINISHED_FUNCTION_NAME = "finished";
	private static final String CLEAN_FUNCTION_NAME = "clean";
	private static final Object[] EMPTY_ARGUMENTS = new Object[0];

	private CLVFFunctionDeclaration init;
	private CLVFFunctionDeclaration count;
	private CLVFFunctionDeclaration transform;
	private CLVFFunctionDeclaration clean;
	private CLVFFunctionDeclaration finished;
	
	private String errorMessage;
	private Integer[] counter;
	private DataRecord[] sourceRec;
	private DataRecord[] targetRec;
	private Log logger;
	private TransformLangExecutor executor;
	private TransformationGraph graph;
	
	/** Constructor for the DataRecordTransform object */
	public CTLRecordNormalizeAdapter(TransformLangExecutor executor, Log logger) {
		this.logger = logger;
		this.executor = executor;
		counter = new Integer[1];
		sourceRec = new DataRecord[1];
		targetRec = new DataRecord[1];
	}

	public TransformationGraph getGraph() {
		return graph;
	}
	
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordNormalize#init(java.util.Properties,
	 *      org.jetel.metadata.DataRecordMetadata,
	 *      org.jetel.metadata.DataRecordMetadata)
	 */
	public boolean init(Properties parameters, DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
	throws ComponentNotReadyException {
		
		// we will be running in one-function-a-time so we need global scope active
		executor.keepGlobalScope();
		executor.init();
		
		this.init = executor.getFunction(INIT_FUNCTION_NAME);
		this.count = executor.getFunction(COUNT_FUNCTION_NAME);
		this.transform = executor.getFunction(TRANSFORM_FUNCTION_NAME, TLTypePrimitive.INTEGER);
		this.clean = executor.getFunction(CLEAN_FUNCTION_NAME);
		this.finished = executor.getFunction(FINISHED_FUNCTION_NAME);
		
		if (count == null) {
			throw new ComponentNotReadyException(COUNT_FUNCTION_NAME + " function is not defined");
		}

		if (transform  == null) {
			throw new ComponentNotReadyException(TRANSFORM_FUNCTION_NAME + " function is not defined");
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
	 * @see org.jetel.component.normalize.RecordNormalize#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.normalize.RecordNormalize#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
	}

	public void global() {
			// execute code in global scope
			executor.execute();
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordNormalize#count(org.jetel.data.DataRecord)
	 */
	public int count(DataRecord source) {
		// pass source as function parameter, but also as a global record
		final Object retVal = executor.executeFunction(count, EMPTY_ARGUMENTS, new DataRecord[]{source}, null);
		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException(COUNT_FUNCTION_NAME + "() function must return 'int'");
		}
		return (Integer)retVal;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordNormalize#transform(org.jetel.data.DataRecord,
	 *      org.jetel.data.DataRecord, int)
	 */
	public int transform(DataRecord source, DataRecord target, int idx)
			throws TransformException {
		counter[0] = idx;
		sourceRec[0] = source;
		targetRec[0] = target;
		final Object retVal = executor.executeFunction(transform, new Object[]{idx}, sourceRec, targetRec);
		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException(TRANSFORM_FUNCTION_NAME + "() function must return 'int'");
		}
		return (Integer)retVal;
	}

	public void clean() {
		if (clean == null) {
			return;
		}
			try {
				// we will be running in one-function-a-time so we need global scope active
				executor.executeFunction(clean, EMPTY_ARGUMENTS);
			} catch (TransformLangExecutorRuntimeException e) {
				logger.warn("Failed to execute " + CLEAN_FUNCTION_NAME + "() function: " + e.getMessage());
			}
	}

	/**
	 * Use postExecute method.
	 */
	@Deprecated
	public void finished() {
		if (finished == null) {
			return;
		}
		
		try {
			// we will be running in one-function-a-time so we need global scope active
			executor.executeFunction(finished, EMPTY_ARGUMENTS);
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to execute " + FINISHED_FUNCTION_NAME + "() function: " + e.getMessage());
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.component.RecordNormalize#getMessage()
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

}
