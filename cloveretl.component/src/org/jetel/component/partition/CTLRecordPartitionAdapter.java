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
package org.jetel.component.partition;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;

/**
 * Class for executing partition function written in CloverETL language
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) 
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz> ;
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Nov 30, 2006
 *
 */
public class CTLRecordPartitionAdapter implements PartitionFunction {

    public static final String INIT_FUNCTION_NAME="init";
    public static final String GETOUTPUTPORT_FUNCTION_NAME="getOutputPort";
    private static final Object[] EMPTY_ARGUMENTS = new Object[0];
    
    private CLVFFunctionDeclaration init;
    private CLVFFunctionDeclaration getOuputPort;
    
	private TransformationGraph graph;

	
	private Log logger;
	private TransformLangExecutor executor;
	private final DataRecord[] inputRecords = new DataRecord[1];
	
    /**
     * @param srcCode code written in CloverETL language
     * @param metadata
     * @param parameters
     * @param logger
     */
    public CTLRecordPartitionAdapter(TransformLangExecutor executor, Log logger) {
        this.logger = logger;
        this.executor = executor;
    }
    
    /* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#getOutputPort(org.jetel.data.DataRecord)
	 */
	public int getOutputPort(DataRecord record) {
		inputRecords[0] = record;
		final Object retVal = executor.executeFunction(getOuputPort, EMPTY_ARGUMENTS, inputRecords, null);
		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException("getOutputPort() function must return 'int'");
		}
		return (Integer)retVal;
	}

	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#init(int, org.jetel.data.RecordKey)
	 */
	public void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException{
		
		// we will be running in one-function-a-time so we need global scope active
		executor.keepGlobalScope();
		executor.init();
		
		this.init = executor.getFunction(INIT_FUNCTION_NAME, TLTypePrimitive.INTEGER);
		this.getOuputPort= executor.getFunction(GETOUTPUTPORT_FUNCTION_NAME);
		
		if (getOuputPort == null ) {
			throw new ComponentNotReadyException(GETOUTPUTPORT_FUNCTION_NAME	+ " is not defined");
		}
		
		try {
			global();
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to initialize global scope: " + e.getMessage());
		}
		
		try {
			// initialize global scope
			// call user-init function afterwards
			if (init != null) {
				executor.executeFunction(init, new Object[]{numPartitions});
			}
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to execute " + INIT_FUNCTION_NAME + "() function: " + e.getMessage());
		}
		
 	}

	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#preExecute()
	 */
	public void preExecute() throws ComponentNotReadyException {
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.component.partition.PartitionFunction#postExecute(org.jetel.graph.TransactionMethod)
	 */
	public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
	}

	public void global() {
			// execute code in global scope
			executor.execute();
	}
	
	public TransformationGraph getGraph() {
		return graph;
	}

	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public int getOutputPort(ByteBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

	public boolean supportsDirectRecord() {
		return false;
	}

}
