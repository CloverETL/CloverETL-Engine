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
import org.jetel.ctl.CTLAbstractTransformAdapter;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author dpavlis
 * @since June 25, 2006
 * @revision $Revision: $
 * @created June 25, 2006
 * @see org.jetel.component.RecordTransform
 */
public final class CTLRecordTransformAdapter extends CTLAbstractTransformAdapter implements RecordTransform {

	public static final String TRANSFORM_FUNCTION_NAME = "transform";
	public static final String GENERATE_FUNCTION_NAME = "generate";
	public static final String FINISHED_FUNCTION_NAME = "finished";
	public static final String INIT_FUNCTION_NAME = "init";
	public static final String RESET_FUNCTION_NAME = "reset";

	private CLVFFunctionDeclaration transform;

    /**
     * Constructs a <code>CTLRecordTransformAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLRecordTransformAdapter(TransformLangExecutor executor, Log logger) {
		super(executor, logger);
	}

	/**
	 * Performs any necessary initialization before transform() method is called
	 * 
	 * @param sourceMetadata
	 *            Array of metadata objects describing source data records
	 * @param targetMetadata
	 *            Array of metadata objects describing source data records
	 * @return True if successful, otherwise False
	 */
	public boolean init(Properties parameters, DataRecordMetadata[] sourceRecordsMetadata,
			DataRecordMetadata[] targetRecordsMetadata) throws ComponentNotReadyException {
        // initialize global scope and call user initialization function
		super.init();

		this.transform = executor.getFunction(TRANSFORM_FUNCTION_NAME);

		if (transform == null) {
			throw new ComponentNotReadyException(TRANSFORM_FUNCTION_NAME + " function must be defined");
		}

		return true;
	}

	public int transform(DataRecord[] inputRecords, DataRecord[] outputRecords) throws TransformException {
		final Object retVal = executor.executeFunction(transform, NO_ARGUMENTS, inputRecords, outputRecords);

		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException("transform() function must return 'int'");
		}

		return (Integer) retVal;
	}

	public void signal(Object signalObject) {
		// does nothing
	}

	public Object getSemiResult() {
		return null;
	}

}
