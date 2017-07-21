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
import org.jetel.component.AbstractTransformTL;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.interpreter.data.TLNumericValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Class for executing partition function written in CloverETL language
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; (c) JavlinConsulting s.r.o. www.javlinconsulting.cz
 * 
 * @since Nov 30, 2006
 */
public class PartitionTL extends AbstractTransformTL implements PartitionFunction {

	public static final String GET_OUTPUT_PORT_FUNCTION_NAME = "getOutputPort";
	public static final String GET_OUTPUT_PORT_ON_ERROR_FUNCTION_NAME = "getOutputPortOnError";

	public static final String PARTITION_COUNT_PARAM_NAME = "partitionCount";

	private final TLValue[] onErrorArguments = new TLValue[] { new TLStringValue(), new TLStringValue() };

	private int getOutputPortFunction;
	private int getOutputPortOnErrorFunction;

	/**
	 * @param srcCode code written in CloverETL language
	 * @param metadata
	 * @param parameters
	 * @param logger
	 */
	public PartitionTL(String srcCode, Logger logger) {
		super(srcCode, logger);
	}

	@Override
	@Deprecated
	public void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException {
		init(numPartitions, partitionKey, null, null);
	}
	
	@Override
	public void init(int numPartitions, RecordKey partitionKey, Properties parameters, DataRecordMetadata metadata) throws ComponentNotReadyException {
		wrapper.setMatadata(metadata);
		wrapper.setParameters(parameters);
        wrapper.setGraph(getGraph());
		wrapper.init();

		TLValue params[] = new TLValue[] { TLValue.create(TLValueType.INTEGER) };
		params[0].getNumeric().setValue(numPartitions);

		try {
			wrapper.execute(INIT_FUNCTION_NAME, params);
		} catch (JetelException e) {
			// do nothing, optional function is not declared
		}

		getOutputPortFunction = wrapper.prepareFunctionExecution(GET_OUTPUT_PORT_FUNCTION_NAME);
		getOutputPortOnErrorFunction = wrapper.prepareOptionalFunctionExecution(GET_OUTPUT_PORT_ON_ERROR_FUNCTION_NAME);
	}

	@Override
	public boolean supportsDirectRecord() {
		return false;
	}

	@Override
	public int getOutputPort(DataRecord record) {
		return getOutputPortImpl(getOutputPortFunction, GET_OUTPUT_PORT_FUNCTION_NAME, record, null);
	}

	@Override
	public int getOutputPortOnError(Exception exception, DataRecord record) throws TransformException {
		if (getOutputPortOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Partitioning failed!", exception);
		}

		onErrorArguments[0].setValue(ExceptionUtils.exceptionChainToMessage(null, exception));
		onErrorArguments[1].setValue(ExceptionUtils.stackTraceToString(exception));

		return getOutputPortImpl(getOutputPortOnErrorFunction, GET_OUTPUT_PORT_ON_ERROR_FUNCTION_NAME,
				record, onErrorArguments);
	}

	private int getOutputPortImpl(int function, String functionName, DataRecord record, TLValue[] arguments) {
		TLValue result = wrapper.executePreparedFunction(function, record, arguments);

		if (result.type.isNumeric()) {
			return ((TLNumericValue<?>) result).getInt();
		}

		throw new RuntimeException(functionName + "() function does not return integer value!");
	}

	@Override
	@Deprecated
	public int getOutputPort(ByteBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getOutputPort(CloverBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

	@Override
	@Deprecated
	public int getOutputPortOnError(Exception exception, ByteBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int getOutputPortOnError(Exception exception, CloverBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

}
