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
import org.jetel.component.AbstractTransformTL;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLValue;
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
 * @since 11/21/06
 * @see org.jetel.component.Normalizer
 */
public class RecordDenormalizeTL extends AbstractTransformTL implements RecordDenormalize {

	private static final String APPEND_FUNCTION_NAME = "append";
	private static final String TRANSFORM_FUNCTION_NAME = "transform";
	private static final String CLEAN_FUNCTION_NAME = "clean";

	private int appendFunctionIdentifier;
	private int transformFunctionIdentifier;
	private int cleanFunctionIdentifier;

	private DataRecord[] outRec;

	/** Constructor for the DataRecordTransform object */
	public RecordDenormalizeTL(Log logger, String srcCode, TransformationGraph graph) {
		super(srcCode, logger);

		outRec = new DataRecord[1];
	}

	public boolean init(Properties parameters, DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata)
			throws ComponentNotReadyException {
		wrapper.setMetadata(new DataRecordMetadata[] { sourceMetadata }, new DataRecordMetadata[] { targetMetadata });
		wrapper.setParameters(parameters);
        wrapper.setGraph(getGraph());
		wrapper.init();

		TLValue result = null;

		try {
			result = wrapper.execute(INIT_FUNCTION_NAME, null);
		} catch (JetelException e) {
			// do nothing: function init is not necessary
		}

		appendFunctionIdentifier = wrapper.prepareFunctionExecution(APPEND_FUNCTION_NAME);
		transformFunctionIdentifier = wrapper.prepareFunctionExecution(TRANSFORM_FUNCTION_NAME);

		try {
			cleanFunctionIdentifier = wrapper.prepareFunctionExecution(CLEAN_FUNCTION_NAME);
		} catch (Exception ex) {
			// do nothing
			cleanFunctionIdentifier = -1;
		}

		return result == null ? true : result == TLBooleanValue.TRUE;
	}

	public int append(DataRecord inRecord) {
		return transformResult(wrapper.executePreparedFunction(appendFunctionIdentifier, inRecord, null));
	}

	public int transform(DataRecord outRecord) {
		this.outRec[0] = outRecord;

		return transformResult(wrapper.executePreparedFunction(transformFunctionIdentifier, null, this.outRec, null));
	}

	private int transformResult(TLValue result) {
		// set the error message to null so that the getMessage() method works correctly if no error occurs
		errorMessage = null;

		if (result == null || result == TLBooleanValue.TRUE) {
			return 0;
		}

		if (result.getType().isNumeric()) {
			return result.getNumeric().getInt();
		}

		errorMessage = "Unexpected return result: " + result.toString() + " (" + result.getType().getName() + ")";

		return -1;
	}

	public void clean() {
		if (cleanFunctionIdentifier != -1) {
			wrapper.executePreparedFunction(cleanFunctionIdentifier);
		}
	}

}
