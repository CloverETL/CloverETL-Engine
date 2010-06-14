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
package org.jetel.component.rollup;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.component.AbstractTransformTL;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLRecordValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * An implementation of the {@link RecordRollup} interface for the transformation language. Serves as a wrapper
 * around the CTL code written by the user.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 14th June 2010
 * @since 28th April 2009
 */
public class RecordRollupTL extends AbstractTransformTL implements RecordRollup {

    /** the name of the initGroup() function in CTL */
    public static final String FUNCTION_INIT_GROUP_NAME = "initGroup";
    /** the name of the updateGroup() function in CTL */
    public static final String FUNCTION_UPDATE_GROUP_NAME = "updateGroup";
    /** the name of the finishGroup() function in CTL */
    public static final String FUNCTION_FINISH_GROUP_NAME = "finishGroup";
    /** the name of the updateTransform() function in CTL */
    public static final String FUNCTION_UPDATE_TRANSFORM_NAME = "updateTransform";
    /** the name of the transform() function in CTL */
    public static final String FUNCTION_TRANSFORM_NAME = "transform";

    /** the ID of the prepared initGroup() function */
    private int functionInitGroupId;
    /** the ID of the prepared updateGroup() function */
    private int functionUpdateGroupId;
    /** the ID of the prepared finishGroup() function */
    private int functionFinishGroupId;
    /** the ID of the prepared updateTransform() function */
    private int functionUpdateTransformId;
    /** the ID of the prepared transform() function */
    private int functionTransformId;

    /** temporary data record used instead of null group accumulator */
    private DataRecord emptyRecord;

    /**
     * Creates an instance of the <code>RecordRollupTL</code> class.
     *
     * @param sourceCode the source code of the transformation
     * @param logger the logger to be used by this TL wrapper
     */
    public RecordRollupTL(String sourceCode, Log logger) {
    	super(sourceCode, logger);
    }

    public void init(Properties parameters, DataRecordMetadata inputMetadata, DataRecordMetadata accumulatorMetadata,
            DataRecordMetadata[] outputMetadata) throws ComponentNotReadyException {
        wrapper.setParameters(parameters);
        wrapper.setMetadata(new DataRecordMetadata[] { inputMetadata }, outputMetadata);
        wrapper.setGraph(getGraph());
        wrapper.init();

        try {
            wrapper.execute(INIT_FUNCTION_NAME, null);
        } catch (JetelException exception) {
            // OK, don't do anything, function init() is not necessary
        }

        functionInitGroupId = wrapper.prepareFunctionExecution(FUNCTION_INIT_GROUP_NAME);
        functionUpdateGroupId = wrapper.prepareFunctionExecution(FUNCTION_UPDATE_GROUP_NAME);
        functionFinishGroupId = wrapper.prepareFunctionExecution(FUNCTION_FINISH_GROUP_NAME);
        functionUpdateTransformId = wrapper.prepareFunctionExecution(FUNCTION_UPDATE_TRANSFORM_NAME);
        functionTransformId = wrapper.prepareFunctionExecution(FUNCTION_TRANSFORM_NAME);

        // prepare empty group accumulator - it is used in case no group accumulator is required
        emptyRecord = new DataRecord(new DataRecordMetadata("emptyGroupAccumulator"));
        emptyRecord.init();
        emptyRecord.reset();
    }

    public void initGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        // if group accumulator is empty we use an empty record for better error reporting in scope of CTL
        if (groupAccumulator == null) {
        	groupAccumulator = emptyRecord;
        }

        wrapper.executePreparedFunction(functionInitGroupId, inputRecord,
                new TLValue[] { new TLRecordValue(groupAccumulator) });
    }

    public boolean updateGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        return executeGroupFunction(functionUpdateGroupId, inputRecord, groupAccumulator, false);
    }

    public boolean finishGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        return executeGroupFunction(functionFinishGroupId, inputRecord, groupAccumulator, true);
    }

    private boolean executeGroupFunction(int functionId, DataRecord inputRecord, DataRecord groupAccumulator,
            boolean defaultReturnValue) {
		// set the error message to null so that the inherited getMessage() method works correctly if no error occurs
		errorMessage = null;

        // if group accumulator is empty we use an empty record for better error reporting in scope of CTL
        if (groupAccumulator == null) {
            groupAccumulator = emptyRecord;
        }

        TLValue result = wrapper.executePreparedFunction(functionId, inputRecord,
                new TLValue[] { new TLRecordValue(groupAccumulator) });

        if (result != null) {
            if (result.getType() == TLValueType.BOOLEAN) {
                return (result == TLBooleanValue.TRUE);
            }

            errorMessage = "Unexpected return result: " + result + " (" + result.getType() + ")";
        }

        return defaultReturnValue;
    }

    public int updateTransform(int counter, DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
            throws TransformException {
        return executeTransformFunction(functionUpdateTransformId, counter, inputRecord, groupAccumulator, outputRecords);
    }

    public int transform(int counter, DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
            throws TransformException {
        return executeTransformFunction(functionTransformId, counter, inputRecord, groupAccumulator, outputRecords);
    }

    private int executeTransformFunction(int functionId, int counter, DataRecord inputRecord, DataRecord groupAccumulator,
            DataRecord[] outputRecords) {
		// set the error message to null so that the inherited getMessage() method works correctly if no error occurs
		errorMessage = null;

        TLValue counterTL = TLValue.create(TLValueType.INTEGER);
        counterTL.setValue(counter);

        // if group accumulator is empty we use an empty record for better error reporting in scope of CTL
        if (groupAccumulator == null) {
            groupAccumulator = emptyRecord;
        }

        TLValue result = wrapper.executePreparedFunction(functionId, new DataRecord[] { inputRecord },
                outputRecords, new TLValue[] { counterTL, new TLRecordValue(groupAccumulator) });

        if (result != null) {
            if (result.getType().isNumeric()) {
                return result.getNumeric().getInt();
            }

            errorMessage = "Unexpected return result: " + result + " (" + result.getType() + ")";
        }

        return SKIP;
    }

}
