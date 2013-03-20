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
import java.util.Properties;

import org.apache.log4j.Logger;
import org.jetel.ctl.CTLAbstractTransformAdapter;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Class for executing partition function written in CloverETL language
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz)
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz> ; (c) JavlinConsulting s.r.o. www.javlinconsulting.cz
 * 
 * @since Nov 30, 2006
 * 
 */
public final class CTLRecordPartitionAdapter extends CTLAbstractTransformAdapter implements PartitionFunction {

	private final DataRecord[] inputRecords = new DataRecord[1];

	private final Object[] onErrorArguments = new Object[2];

	private CLVFFunctionDeclaration getOuputPortFunction;
	private CLVFFunctionDeclaration getOuputPortOnErrorFunction;

    /**
     * Constructs a <code>CTLRecordPartitionAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLRecordPartitionAdapter(TransformLangExecutor executor, Logger logger) {
		super(executor, logger);
	}

	@Override
	public boolean supportsDirectRecord() {
		return false;
	}

	@Override
	@Deprecated
	public void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException {
		init(numPartitions, partitionKey, null, null);
	}

	@Override
	public void init(int numPartitions, RecordKey partitionKey, Properties parameters, DataRecordMetadata metadata) throws ComponentNotReadyException {
        // initialize global scope and call user initialization function
		super.init(numPartitions);

		getOuputPortFunction = executor.getFunction(PartitionTL.GET_OUTPUT_PORT_FUNCTION_NAME);
		getOuputPortOnErrorFunction = executor.getFunction(PartitionTL.GET_OUTPUT_PORT_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING);

		if (getOuputPortFunction == null) {
			throw new ComponentNotReadyException(PartitionTL.GET_OUTPUT_PORT_FUNCTION_NAME + " is not defined");
		}
	}

	@Override
	public int getOutputPort(DataRecord record) throws TransformException {
		return getOutputPortImpl(getOuputPortFunction, record, NO_ARGUMENTS);
	}

	@Override
	public int getOutputPortOnError(Exception exception, DataRecord record) throws TransformException {
		if (getOuputPortOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Partitioning failed!", exception);
		}

		onErrorArguments[0] = ExceptionUtils.getMessage(null, exception);
		onErrorArguments[1] = ExceptionUtils.stackTraceToString(exception);

		return getOutputPortImpl(getOuputPortOnErrorFunction, record, onErrorArguments);
	}

	private int getOutputPortImpl(CLVFFunctionDeclaration function, DataRecord record, Object[] arguments) {
		inputRecords[0] = record;

		Object result = executor.executeFunction(function, arguments, inputRecords, NO_DATA_RECORDS);

		if (result == null || !(result instanceof Integer)) {
			throw new TransformLangExecutorRuntimeException(function.getName() + "() function must return 'int'");
		}

		return (Integer) result;
	}

	@Override
	public int getOutputPort(CloverBuffer directRecord) throws TransformException {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated
	public int getOutputPort(ByteBuffer directRecord) throws TransformException {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public int getOutputPortOnError(Exception exception, CloverBuffer directRecord) throws TransformException {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated
	public int getOutputPortOnError(Exception exception, ByteBuffer directRecord) throws TransformException {
		throw new UnsupportedOperationException();
	}

}
