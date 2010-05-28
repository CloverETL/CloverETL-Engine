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

import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.Concatenate;

/**
 * An implementation of the {@link RecordRollup} interface for interpretation of CTL code written by the user.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 20th April 2010
 * @created 20th April 2010
 *
 * @see RecordRollup
 * @see Rollup
 */
public final class CTLRecordRollupAdapter implements RecordRollup {

	/** an empty array of arguments used for calls to functions without any arguments */
    private static final Object[] NO_ARGUMENTS = new Object[0];
	/** an empty array of data records used for calls to functions that do not access to any data records */
    private static final DataRecord[] NO_DATA_RECORDS = new DataRecord[0];

    /** the CTL executor to be used for this rollup transform */
    private final TransformLangExecutor executor;

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

    /** the CTL declaration of the optional getMessage() function */
    private CLVFFunctionDeclaration functionGetMessage;
    /** the CTL declaration of the optional finished() function */
    private CLVFFunctionDeclaration functionFinished;

    /** empty data record used instead of null group accumulator for better error reporting in scope of CTL */
    private DataRecord emptyRecord;

    /**
     * Constructs a <code>CTLRecordRollupAdapter</code> for a given CTL executor.
     *
     * @param executor the CTL executor to be used for this rollup transform, may not be <code>null</code>
     *
     * @throws NullPointerException if the executor is <code>null</code>
     */
    public CTLRecordRollupAdapter(TransformLangExecutor executor) {
		if (executor == null) {
			throw new NullPointerException("executor");
		}

		this.executor = executor;
	}

	/**
	 * Calls to this method are ignored, associating a graph is meaningless in case of CTL.
	 */
	public void setGraph(TransformationGraph graph) {
		// do nothing
	}

	/**
	 * @return always <code>null</code>, no graph can be associated with this rollup transform
	 */
	public TransformationGraph getGraph() {
		return null;
	}

    public void init(Properties parameters, DataRecordMetadata inputMetadata, DataRecordMetadata accumulatorMetadata,
            DataRecordMetadata[] outputMetadata) throws ComponentNotReadyException {
		// we will be calling one function at a time so we need global scope active
		executor.keepGlobalScope();
		executor.init();

		// initialize required CTL functions
		functionInitGroup = executor.getFunction(RecordRollupTL.FUNCTION_INIT_GROUP_NAME, TLType.RECORD);
		functionUpdateGroup = executor.getFunction(RecordRollupTL.FUNCTION_UPDATE_GROUP_NAME, TLType.RECORD);
		functionFinishGroup = executor.getFunction(RecordRollupTL.FUNCTION_FINISH_GROUP_NAME, TLType.RECORD);
		functionUpdateTransform = executor.getFunction(RecordRollupTL.FUNCTION_UPDATE_TRANSFORM_NAME, TLTypePrimitive.INTEGER, TLType.RECORD);
		functionTransform = executor.getFunction(RecordRollupTL.FUNCTION_TRANSFORM_NAME, TLTypePrimitive.INTEGER, TLType.RECORD);

		// check if all required functions are present, otherwise we cannot continue
		checkRequiredFunctions();

		// initialize optional CTL functions
		functionGetMessage = executor.getFunction(RecordRollupTL.FUNCTION_GET_MESSAGE_NAME);
		functionFinished = executor.getFunction(RecordRollupTL.FUNCTION_FINISHED_NAME);

		// prepare an empty data record to be used instead of a null group accumulator
        emptyRecord = new DataRecord(new DataRecordMetadata("emptyGroupAccumulator"));
        emptyRecord.init();
        emptyRecord.reset();

        // initialize global scope and call user initialization function
        init();
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

    private void init() throws ComponentNotReadyException {
		try {
			executor.execute();
		} catch (TransformLangExecutorRuntimeException exception) {
			throw new ComponentNotReadyException("Failed to initialize global scope!", exception);
		}

		CLVFFunctionDeclaration functionInit = executor.getFunction(RecordRollupTL.FUNCTION_INIT_NAME);

		if (functionInit != null) {
			try {
				executor.executeFunction(functionInit, NO_ARGUMENTS);
			} catch (TransformLangExecutorRuntimeException exception) {
				throw new ComponentNotReadyException("Execution of " + functionInit.getName()
						+ "() function failed!", exception);
			}
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

    public String getMessage() {
    	if (functionGetMessage == null) {
    		return null;
    	}

		Object result = executor.executeFunction(functionGetMessage, NO_ARGUMENTS, NO_DATA_RECORDS, NO_DATA_RECORDS);

        if (!(result instanceof String)) {
            throw new TransformLangExecutorRuntimeException(functionGetMessage.getName()
            		+ "() function must return a string!");
        }

        return (String) result;
    }

    public void finished() {
    	if (functionFinished != null) {
			executor.executeFunction(functionFinished, NO_ARGUMENTS);
    	}
    }

    public void reset() throws ComponentNotReadyException {
    	// nothing to do here
    }

}
