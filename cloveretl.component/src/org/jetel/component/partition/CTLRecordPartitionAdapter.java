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
import org.jetel.ctl.CTLAbstractTransformAdapter;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;

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

	public static final String GETOUTPUTPORT_FUNCTION_NAME = "getOutputPort";

	private CLVFFunctionDeclaration getOuputPort;

	private final DataRecord[] inputRecords = new DataRecord[1];

    /**
     * Constructs a <code>CTLRecordPartitionAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLRecordPartitionAdapter(TransformLangExecutor executor, Log logger) {
		super(executor, logger);
	}

	public boolean supportsDirectRecord() {
		return false;
	}

	public void init(int numPartitions, RecordKey partitionKey) throws ComponentNotReadyException {
        // initialize global scope and call user initialization function
		super.init();

		this.getOuputPort = executor.getFunction(GETOUTPUTPORT_FUNCTION_NAME);

		if (getOuputPort == null) {
			throw new ComponentNotReadyException(GETOUTPUTPORT_FUNCTION_NAME + " is not defined");
		}
	}

	public int getOutputPort(DataRecord record) {
		inputRecords[0] = record;

		final Object retVal = executor.executeFunction(getOuputPort, NO_ARGUMENTS, inputRecords, null);

		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException("getOutputPort() function must return 'int'");
		}

		return (Integer) retVal;
	}

	public int getOutputPort(ByteBuffer directRecord) {
		throw new UnsupportedOperationException();
	}

}
