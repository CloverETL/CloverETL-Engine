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
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @created March 25, 2009
 * @see org.jetel.component.RecordGenerate
 */
public class CTLRecordGenerateAdapter implements RecordGenerate {

	private static final String INIT_FUNCTION_NAME = "init";
	private static final String GENERATE_FUNCTION_NAME = "generate";
	private static final String FINISHED_FUNCTION_NAME = "finished";

	private static final Object[] EMPTY_ARGUMENTS = new Object[0];

	private CLVFFunctionDeclaration init;
	private CLVFFunctionDeclaration generate;
	private CLVFFunctionDeclaration finished;

	private DataRecord[] targetRec;
	private Log logger;
	private TransformLangExecutor executor;
	private TransformationGraph graph;

	/** Constructor for the DataRecordGenerate object */
	public CTLRecordGenerateAdapter(TransformLangExecutor executor, Log logger) {
		this.logger = logger;
		this.executor = executor;
	}

	/**
	 * Performs any necessary initialization before generate() method is called
	 * 
	 * @param targetMetadata
	 *            Array of metadata objects describing source data records
	 * @return True if successfull, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] targetRecordsMetadata)
			throws ComponentNotReadyException {

		// we will be running in one-function-a-time so we need global scope active
		executor.keepGlobalScope();
		executor.init();
		
		this.init = executor.getFunction(INIT_FUNCTION_NAME);
		this.generate = executor.getFunction(GENERATE_FUNCTION_NAME);
		this.finished = executor.getFunction(FINISHED_FUNCTION_NAME);
		
		if (generate  == null) {
			throw new ComponentNotReadyException(GENERATE_FUNCTION_NAME + " function is not defined");
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
	 * @see org.jetel.component.RecordGenerate#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.RecordGenerate#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute() throws ComponentNotReadyException {
	}
	
	/** Method represents code in the global scope */
	public void global() {
		// execute code in global scope
		executor.execute();
	}
	
	/**
	 * Generate data for output records.
	 */
	public int generate(DataRecord[] outputRecords) throws TransformException {
		targetRec = outputRecords;
		final Object retVal = executor.executeFunction(generate, EMPTY_ARGUMENTS, null, targetRec);
		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException(GENERATE_FUNCTION_NAME + "() function must return 'int'");
		}
		return (Integer)retVal;
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
	public TransformationGraph getGraph() {
		return graph;
	}

	public String getMessage() {
		return null;
	}

	public Object getSemiResult() {
		return null;
	}

	/**
	 * Use preExecute method.
	 */
	@Deprecated
	public void reset() {
		// nothing to do
	}

	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public void signal(Object signalObject) {
		// nothing to do
	}
}
