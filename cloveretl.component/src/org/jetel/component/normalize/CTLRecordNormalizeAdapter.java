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
import org.jetel.ctl.CTLAbstractTransformAdapter;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
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
public final class CTLRecordNormalizeAdapter extends CTLAbstractTransformAdapter implements RecordNormalize {

	private static final String COUNT_FUNCTION_NAME = "count";
	private static final String TRANSFORM_FUNCTION_NAME = "transform";
	private static final String CLEAN_FUNCTION_NAME = "clean";

	private CLVFFunctionDeclaration count;
	private CLVFFunctionDeclaration transform;
	private CLVFFunctionDeclaration clean;
	
	private final Integer[] counter = new Integer[1];
	private final DataRecord[] sourceRec = new DataRecord[1];
	private final DataRecord[] targetRec = new DataRecord[1];
	
    /**
     * Constructs a <code>CTLRecordNormalizeAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
	public CTLRecordNormalizeAdapter(TransformLangExecutor executor, Log logger) {
		super(executor, logger);
	}

	public boolean init(Properties parameters, DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
        // initialize global scope and call user initialization function
		super.init();

		this.count = executor.getFunction(COUNT_FUNCTION_NAME);
		this.transform = executor.getFunction(TRANSFORM_FUNCTION_NAME, TLTypePrimitive.INTEGER);
		this.clean = executor.getFunction(CLEAN_FUNCTION_NAME);
		
		if (count == null) {
			throw new ComponentNotReadyException(COUNT_FUNCTION_NAME + " function is not defined");
		}

		if (transform  == null) {
			throw new ComponentNotReadyException(TRANSFORM_FUNCTION_NAME + " function is not defined");
		}

		return true;
 	}

	public int count(DataRecord source) {
		sourceRec[0] = source;

		final Object retVal = executor.executeFunction(count, NO_ARGUMENTS, sourceRec, NO_DATA_RECORDS);

		if (retVal == null || retVal instanceof Integer == false) {
			throw new TransformLangExecutorRuntimeException(COUNT_FUNCTION_NAME + "() function must return 'int'");
		}

		return (Integer)retVal;
	}

	public int transform(DataRecord source, DataRecord target, int idx)
			throws TransformException {
		counter[0] = idx;
		sourceRec[0] = source;
		targetRec[0] = target;

		final Object retVal = executor.executeFunction(transform, counter, sourceRec, targetRec);

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
			executor.executeFunction(clean, NO_ARGUMENTS);
		} catch (TransformLangExecutorRuntimeException e) {
			logger.warn("Failed to execute " + CLEAN_FUNCTION_NAME + "() function: " + e.getMessage());
		}
	}

}
