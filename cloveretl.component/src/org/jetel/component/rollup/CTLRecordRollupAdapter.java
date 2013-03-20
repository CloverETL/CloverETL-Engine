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

import org.apache.log4j.Logger;
import org.jetel.component.Rollup;
import org.jetel.ctl.CTLAbstractTransformAdapter;
import org.jetel.ctl.TransformLangExecutor;
import org.jetel.ctl.TransformLangExecutorRuntimeException;
import org.jetel.ctl.ASTnode.CLVFFunctionDeclaration;
import org.jetel.ctl.data.TLType;
import org.jetel.ctl.data.TLTypePrimitive;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.string.Concatenate;

/**
 * An implementation of the {@link RecordRollup} interface for interpretation of CTL code written by the user.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 22nd June 2010
 * @created 20th April 2010
 *
 * @see RecordRollup
 * @see Rollup
 */
public final class CTLRecordRollupAdapter extends CTLAbstractTransformAdapter implements RecordRollup {

    /** an array for input data records passed to CTL functions -- for optimization purposes */
    private final DataRecord[] inputRecords = new DataRecord[1];
    /** empty data record used instead of null group accumulator for better error reporting in scope of CTL */
    private final DataRecord emptyRecord = DataRecordFactory.newRecord(new DataRecordMetadata("emptyGroupAccumulator"));

    /** an array for arguments passed to *group() functions -- for optimization purposes */
    private final Object[] groupArguments = new Object[1];
    /** an array for arguments passed to *groupOnError() functions -- for optimization purposes */
    private final Object[] groupOnErrorArguments = new Object[3];
    /** an array for arguments passed to *transform() functions -- for optimization purposes */
    private final Object[] transformArguments = new Object[2];
    /** an array for arguments passed to *transformOnError() functions -- for optimization purposes */
    private final Object[] transformOnErrorArguments = new Object[4];

    /** the CTL declaration of the required initGroup() function */
    private CLVFFunctionDeclaration initGroupFunction;
    /** the CTL declaration of the required initGroupOnError() function */
    private CLVFFunctionDeclaration initGroupOnErrorFunction;
    /** the CTL declaration of the required updateGroup() function */
    private CLVFFunctionDeclaration updateGroupFunction;
    /** the CTL declaration of the required updateGroupOnError() function */
    private CLVFFunctionDeclaration updateGroupOnErrorFunction;
    /** the CTL declaration of the required finishGroup() function */
    private CLVFFunctionDeclaration finishGroupFunction;
    /** the CTL declaration of the required finishGroupOnError() function */
    private CLVFFunctionDeclaration finishGroupOnErrorFunction;
    /** the CTL declaration of the required updateTransform() function */
    private CLVFFunctionDeclaration updateTransformFunction;
    /** the CTL declaration of the required updateTransformOnError() function */
    private CLVFFunctionDeclaration updateTransformOnErrorFunction;
    /** the CTL declaration of the required transform() function */
    private CLVFFunctionDeclaration transformFunction;
    /** the CTL declaration of the required transformOnError() function */
    private CLVFFunctionDeclaration transformOnErrorFunction;

    /**
     * Constructs a <code>CTLRecordRollupAdapter</code> for a given CTL executor and logger.
     *
     * @param executor the CTL executor to be used by this transform adapter, may not be <code>null</code>
     * @param executor the logger to be used by this transform adapter, may not be <code>null</code>
     *
     * @throws NullPointerException if either the executor or the logger is <code>null</code>
     */
    public CTLRecordRollupAdapter(TransformLangExecutor executor, Logger logger) {
		super(executor, logger);
	}

    @Override
    public void init(Properties parameters, DataRecordMetadata inputMetadata, DataRecordMetadata accumulatorMetadata,
            DataRecordMetadata[] outputMetadata) throws ComponentNotReadyException {
        // initialize global scope and call user initialization function
		super.init();

		// initialize both required and optional CTL functions
		initGroupFunction = executor.getFunction(RecordRollupTL.INIT_GROUP_FUNCTION_NAME, TLType.RECORD);
		initGroupOnErrorFunction = executor.getFunction(RecordRollupTL.INIT_GROUP_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING, TLType.RECORD);
		updateGroupFunction = executor.getFunction(RecordRollupTL.UPDATE_GROUP_FUNCTION_NAME, TLType.RECORD);
		updateGroupOnErrorFunction = executor.getFunction(RecordRollupTL.UPDATE_GROUP_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING, TLType.RECORD);
		finishGroupFunction = executor.getFunction(RecordRollupTL.FINISH_GROUP_FUNCTION_NAME, TLType.RECORD);
		finishGroupOnErrorFunction = executor.getFunction(RecordRollupTL.FINISH_GROUP_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING, TLType.RECORD);
		updateTransformFunction = executor.getFunction(RecordRollupTL.UPDATE_TRANSFORM_FUNCTION_NAME,
				TLTypePrimitive.INTEGER, TLType.RECORD);
		updateTransformOnErrorFunction = executor.getFunction(RecordRollupTL.UPDATE_TRANSFORM_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING, TLTypePrimitive.INTEGER, TLType.RECORD);
		transformFunction = executor.getFunction(RecordRollupTL.TRANSFORM_FUNCTION_NAME,
				TLTypePrimitive.INTEGER, TLType.RECORD);
		transformOnErrorFunction = executor.getFunction(RecordRollupTL.TRANSFORM_ON_ERROR_FUNCTION_NAME,
				TLTypePrimitive.STRING, TLTypePrimitive.STRING, TLTypePrimitive.INTEGER, TLType.RECORD);

		// check if all required functions are present, otherwise we cannot continue
		checkRequiredFunctions();

		// initialize an empty data record to be used instead of a null group accumulator
        emptyRecord.init();
        emptyRecord.reset();
    }

    private void checkRequiredFunctions() throws ComponentNotReadyException {
    	Concatenate missingFunctions = new Concatenate(", ");

    	if (initGroupFunction == null) {
    		missingFunctions.append(RecordRollupTL.INIT_GROUP_FUNCTION_NAME + "()");
    	}

    	if (updateGroupFunction == null) {
    		missingFunctions.append(RecordRollupTL.UPDATE_GROUP_FUNCTION_NAME + "()");
    	}

    	if (finishGroupFunction == null) {
    		missingFunctions.append(RecordRollupTL.FINISH_GROUP_FUNCTION_NAME + "()");
    	}

    	if (updateTransformFunction == null) {
    		missingFunctions.append(RecordRollupTL.UPDATE_TRANSFORM_FUNCTION_NAME + "()");
    	}

    	if (transformFunction == null) {
    		missingFunctions.append(RecordRollupTL.TRANSFORM_FUNCTION_NAME + "()");
    	}

    	if (!missingFunctions.isEmpty()) {
    		throw new ComponentNotReadyException("Required function(s) " + missingFunctions.toString() + " are missing!");
    	}
    }

    @Override
    public void initGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
    	inputRecords[0] = inputRecord;

    	executor.executeFunction(initGroupFunction, initGroupArguments(null, groupAccumulator),
    			inputRecords, NO_DATA_RECORDS);
    }

    @Override
    public void initGroupOnError(Exception exception, DataRecord inputRecord, DataRecord groupAccumulator)
    		throws TransformException {
		if (initGroupOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Rollup failed!", exception);
		}

    	inputRecords[0] = inputRecord;

    	executor.executeFunction(initGroupOnErrorFunction, initGroupArguments(exception, groupAccumulator),
    			inputRecords, NO_DATA_RECORDS);
    }

    @Override
    public boolean updateGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        return groupFunctionImpl(updateGroupFunction, null, inputRecord, groupAccumulator);
    }

    @Override
    public boolean updateGroupOnError(Exception exception, DataRecord inputRecord, DataRecord groupAccumulator)
    		throws TransformException {
		if (updateGroupOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Rollup failed!", exception);
		}

        return groupFunctionImpl(updateGroupOnErrorFunction, exception, inputRecord, groupAccumulator);
    }

    @Override
    public boolean finishGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        return groupFunctionImpl(finishGroupFunction, null, inputRecord, groupAccumulator);
    }

    @Override
    public boolean finishGroupOnError(Exception exception, DataRecord inputRecord, DataRecord groupAccumulator)
    		throws TransformException {
		if (finishGroupOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Rollup failed!", exception);
		}

        return groupFunctionImpl(finishGroupOnErrorFunction, exception, inputRecord, groupAccumulator);
    }

    private boolean groupFunctionImpl(CLVFFunctionDeclaration function, Exception exception, DataRecord inputRecord,
    		DataRecord groupAccumulator) throws TransformException {
    	inputRecords[0] = inputRecord;

        Object result = executor.executeFunction(function, initGroupArguments(exception, groupAccumulator),
        		inputRecords, NO_DATA_RECORDS);

        if (!(result instanceof Boolean)) {
            throw new TransformLangExecutorRuntimeException(function.getName() + "() function must return a boolean!");
        }

        return (Boolean) result;
    }

    private Object[] initGroupArguments(Exception exception, DataRecord groupAccumulator) {
    	if (exception != null) {
    		// provide exception message and stack trace
	    	groupOnErrorArguments[0] = ExceptionUtils.getMessage(null, exception);
	    	groupOnErrorArguments[1] = ExceptionUtils.stackTraceToString(exception);

	    	// if group accumulator is empty we use an empty record for better error reporting in scope of CTL
	    	groupOnErrorArguments[2] = (groupAccumulator != null) ? groupAccumulator : emptyRecord;

	    	return groupOnErrorArguments;
    	}

        // if group accumulator is empty we use an empty record for better error reporting in scope of CTL
    	groupArguments[0] = (groupAccumulator != null) ? groupAccumulator : emptyRecord;

    	return groupArguments;
    }

    @Override
    public int updateTransform(int counter, DataRecord inputRecord, DataRecord groupAccumulator,
    		DataRecord[] outputRecords) throws TransformException {
    	return transformFunctionImpl(updateTransformFunction, null, counter, inputRecord,
    			groupAccumulator, outputRecords);
    }

    @Override
    public int updateTransformOnError(Exception exception, int counter, DataRecord inputRecord,
    		DataRecord groupAccumulator, DataRecord[] outputRecords) throws TransformException {
		if (updateTransformOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Rollup failed!", exception);
		}

    	return transformFunctionImpl(updateTransformOnErrorFunction, exception, counter, inputRecord,
    			groupAccumulator, outputRecords);
    }

    @Override
    public int transform(int counter, DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
    		throws TransformException {
    	return transformFunctionImpl(transformFunction, null, counter, inputRecord,
    			groupAccumulator, outputRecords);
    }

    @Override
    public int transformOnError(Exception exception, int counter, DataRecord inputRecord, DataRecord groupAccumulator,
    		DataRecord[] outputRecords) throws TransformException {
		if (transformOnErrorFunction == null) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Rollup failed!", exception);
		}

    	return transformFunctionImpl(transformOnErrorFunction, exception, counter, inputRecord,
    			groupAccumulator, outputRecords);
    }

    private int transformFunctionImpl(CLVFFunctionDeclaration function, Exception exception, int counter,
    		DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords) throws TransformException {
    	inputRecords[0] = inputRecord;

		Object result = executor.executeFunction(function, initTransformArguments(exception, counter, groupAccumulator),
				inputRecords, outputRecords);

        if (!(result instanceof Integer)) {
            throw new TransformLangExecutorRuntimeException(function.getName() + "() function must return an int!");
        }

        return (Integer) result;
    }

    private Object[] initTransformArguments(Exception exception, int counter, DataRecord groupAccumulator) {
    	if (exception != null) {
    		// provide exception message, stack trace and call counter
	    	transformOnErrorArguments[0] = ExceptionUtils.getMessage(null, exception);
	    	transformOnErrorArguments[1] = ExceptionUtils.stackTraceToString(exception);
	    	transformOnErrorArguments[2] = counter;

	    	// if group accumulator is empty we use an empty record for better error reporting in scope of CTL
	    	transformOnErrorArguments[3] = (groupAccumulator != null) ? groupAccumulator : emptyRecord;

	    	return transformOnErrorArguments;
    	}

        // if group accumulator is empty we use an empty record for better error reporting in scope of CTL
        transformArguments[0] = counter;
    	transformArguments[1] = (groupAccumulator != null) ? groupAccumulator : emptyRecord;

    	return transformArguments;
    }

}
