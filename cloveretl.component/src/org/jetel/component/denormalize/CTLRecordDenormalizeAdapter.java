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
import org.jetel.ctl.CTLAbstractTransformAdapter;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Implements denormalization based on TransformLang source specified by user. User defines following functions
 * (asterisk denotes the mandatory ones):
 * <ul>
 * <li>* function append()</li>
 * <li>* function transform()</li>
 * <li>function init()</li>
 * <li>function finished()</li>
 * </ul>
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @author Michal Tomcanyi <michal.tomcanyi@javlin.cz>
 * @since 11/21/06
 * @see org.jetel.component.Normalizer
 */
public class CTLRecordDenormalizeAdapter extends CTLAbstractTransformAdapter implements RecordDenormalize {

	private static final String APPEND_FUNCTION_NAME = "append";
	private static final String TRANSFORM_FUNCTION_NAME = "transform";
	private static final String CLEAN_FUNCTION_NAME = "clean";

	private CLVFFunctionDeclaration append;
	private CLVFFunctionDeclaration transform;
	private CLVFFunctionDeclaration clean;

	private DataRecord[] inputRecords = new DataRecord[1];
	private DataRecord[] outputRecords = new DataRecord[1];

    /**
     * Constructs a <code>CTLRecordDenormalizeAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLRecordDenormalizeAdapter(TransformLangExecutor executor, Log logger) {
		super(executor, logger);
	}

	public boolean init(Properties parameters, DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		// initialize global scope and call user initialization function
		super.init();

		this.append = executor.getFunction(APPEND_FUNCTION_NAME);
		this.transform = executor.getFunction(TRANSFORM_FUNCTION_NAME);
		this.clean = executor.getFunction(CLEAN_FUNCTION_NAME);

		if (append == null) {
			throw new ComponentNotReadyException(APPEND_FUNCTION_NAME + " function must be defined");
		}
		if (transform == null) {
			throw new ComponentNotReadyException(TRANSFORM_FUNCTION_NAME + " function must be defined");
		}

		return true;
	}

	public int append(DataRecord inRecord) {
		inputRecords[0] = inRecord;

		final Object retVal = executor.executeFunction(append, NO_ARGUMENTS, inputRecords, NO_DATA_RECORDS);

		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException("append() function must return 'int'");
		}

		return (Integer) retVal;
	}

	public int transform(DataRecord outRecord) {
		outputRecords[0] = outRecord;

		final Object retVal = executor.executeFunction(transform, NO_ARGUMENTS, NO_DATA_RECORDS, outputRecords);

		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException("transform() function must return 'int'");
		}

		return (Integer) retVal;
	}

	public void clean() {
		if (clean == null) {
			return;
		}

		try {
			executor.executeFunction(clean, NO_ARGUMENTS);
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to execute " + CLEAN_FUNCTION_NAME + "() function: " + e.getMessage());
		}
	}

}
