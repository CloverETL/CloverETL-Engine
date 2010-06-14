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
import org.jetel.component.Rollup;
import org.jetel.ctl.CTLAbstractTransformAdapter;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.Concatenate;

/**
 * An implementation of the {@link RecordRollup} interface for interpretation of CTL code written by the user.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 11th June 2010
 * @created 20th April 2010
 *
 * @see RecordRollup
 * @see Rollup
 */
public final class CTLRecordRollupAdapter extends CTLAbstractTransformAdapter implements RecordRollup {

    /** an array for arguments passed to *group() functions -- for optimization purposes */
    private final Object[] groupArguments = new Object[1];
    /** an array for arguments passed to *transform() functions -- for optimization purposes */
    private final Object[] transformArguments = new Object[2];
    /** an array for input data records passed to CTL functions -- for optimization purposes */
    private final DataRecord[] inputRecords = new DataRecord[1];

    /** the CTL declaration of the required initGroup() function */
    private CLVFFunctionDeclaration functionInitGroup;
    /** the CTL declaration of the required updateGroup() function */
    private CLVFFunctionDeclaration functionUpdateGroup;
    /** the CTL declaration of the required finishGroup() function */
    private CLVFFunctionDeclaration functionFinishGroup;
    /** the CTL declaration of the required updateTransform() function */
    private CLVFFunctionDeclaration functionUpdateTransform;
    /** the CTL declaration of the required transform() function */
    private CLVFFunctionDeclaration functionTransform;

    /** empty data record used instead of null group accumulator for better error reporting in scope of CTL */
    private DataRecord emptyRecord;

    /**
     * Constructs a <code>CTLRecordRollupAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
    public CTLRecordRollupAdapter(TransformLangExecutor executor, Log logger) {
		super(executor, logger);
	}

    public void init(Properties parameters, DataRecordMetadata inputMetadata, DataRecordMetadata accumulatorMetadata,
            DataRecordMetadata[] outputMetadata) throws ComponentNotReadyException {
        // initialize global scope and call user initialization function
		super.init();

		// initialize required CTL functions
		functionInitGroup = executor.getFunction(RecordRollupTL.FUNCTION_INIT_GROUP_NAME, TLType.RECORD);
		functionUpdateGroup = executor.getFunction(RecordRollupTL.FUNCTION_UPDATE_GROUP_NAME, TLType.RECORD);
		functionFinishGroup = executor.getFunction(RecordRollupTL.FUNCTION_FINISH_GROUP_NAME, TLType.RECORD);
		functionUpdateTransform = executor.getFunction(RecordRollupTL.FUNCTION_UPDATE_TRANSFORM_NAME, TLTypePrimitive.INTEGER, TLType.RECORD);
		functionTransform = executor.getFunction(RecordRollupTL.FUNCTION_TRANSFORM_NAME, TLTypePrimitive.INTEGER, TLType.RECORD);

		// check if all required functions are present, otherwise we cannot continue
		checkRequiredFunctions();

		// prepare an empty data record to be used instead of a null group accumulator
        emptyRecord = new DataRecord(new DataRecordMetadata("emptyGroupAccumulator"));
        emptyRecord.init();
        emptyRecord.reset();
    }

    private void checkRequiredFunctions() throws ComponentNotReadyException {
    	Concatenate missingFunctions = new Concatenate(", ");

    	if (functionInitGroup == null) {
    		missingFunctions.append(RecordRollupTL.FUNCTION_INIT_GROUP_NAME + "()");
    	}

    	if (functionUpdateGroup == null) {
    		missingFunctions.append(RecordRollupTL.FUNCTION_UPDATE_GROUP_NAME + "()");
    	}

    	if (functionFinishGroup == null) {
    		missingFunctions.append(RecordRollupTL.FUNCTION_FINISH_GROUP_NAME + "()");
    	}

    	if (functionUpdateTransform == null) {
    		missingFunctions.append(RecordRollupTL.FUNCTION_UPDATE_TRANSFORM_NAME + "()");
    	}

    	if (functionTransform == null) {
    		missingFunctions.append(RecordRollupTL.FUNCTION_TRANSFORM_NAME + "()");
    	}

    	if (!missingFunctions.isEmpty()) {
    		throw new ComponentNotReadyException("Required function(s) " + missingFunctions.toString() + " are missing!");
    	}
    }

    public void initGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        // if group accumulator is empty, use an empty record for better error reporting in scope of CTL
    	groupArguments[0] = (groupAccumulator != null) ? groupAccumulator : emptyRecord;
    	inputRecords[0] = inputRecord;

    	executor.executeFunction(functionInitGroup, groupArguments, inputRecords, NO_DATA_RECORDS);
    }

    public boolean updateGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        return executeGroupFunction(functionUpdateGroup, inputRecord, groupAccumulator);
    }

    public boolean finishGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        return executeGroupFunction(functionFinishGroup, inputRecord, groupAccumulator);
    }

    private boolean executeGroupFunction(CLVFFunctionDeclaration function, DataRecord inputRecord,
    		DataRecord groupAccumulator) throws TransformException {
        // if group accumulator is empty, use an empty record for better error reporting in scope of CTL
    	groupArguments[0] = (groupAccumulator != null) ? groupAccumulator : emptyRecord;
    	inputRecords[0] = inputRecord;

        Object result = executor.executeFunction(function, groupArguments, inputRecords, NO_DATA_RECORDS);

        if (!(result instanceof Boolean)) {
            throw new TransformLangExecutorRuntimeException(function.getName() + "() function must return a boolean!");
        }

        return (Boolean) result;
    }

    public int updateTransform(int counter, DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
    		throws TransformException {
    	return executeTransformFunction(functionUpdateTransform, counter, inputRecord, groupAccumulator, outputRecords);
    }

    public int transform(int counter, DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
    		throws TransformException {
    	return executeTransformFunction(functionTransform, counter, inputRecord, groupAccumulator, outputRecords);
    }

    private int executeTransformFunction(CLVFFunctionDeclaration function, int counter, DataRecord inputRecord,
    		DataRecord groupAccumulator, DataRecord[] outputRecords) throws TransformException {
        // if group accumulator is empty, use an empty record for better error reporting in scope of CTL
    	transformArguments[0] = counter;
    	transformArguments[1] = (groupAccumulator != null) ? groupAccumulator : emptyRecord;
    	inputRecords[0] = inputRecord;

		Object result = executor.executeFunction(function, transformArguments, inputRecords, outputRecords);

        if (!(result instanceof Integer)) {
            throw new TransformLangExecutorRuntimeException(function.getName() + "() function must return an int!");
        }

        return (Integer) result;
    }

}
