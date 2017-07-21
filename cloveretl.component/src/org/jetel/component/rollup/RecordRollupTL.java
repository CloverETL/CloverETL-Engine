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
import org.jetel.component.AbstractTransformTL;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLRecordValue;
import org.jetel.interpreter.data.TLStringValue;
import org.jetel.interpreter.data.TLValue;
import org.jetel.interpreter.data.TLValueType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;

/**
 * An implementation of the {@link RecordRollup} interface for the transformation language. Serves as a wrapper
 * around the CTL code written by the user.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 23rd June 2010
 * @since 28th April 2009
 */
public class RecordRollupTL extends AbstractTransformTL implements RecordRollup {

    /** the name of the initGroup() function in CTL */
    public static final String INIT_GROUP_FUNCTION_NAME = "initGroup";
    /** the name of the initGroupOnError() function in CTL */
    public static final String INIT_GROUP_ON_ERROR_FUNCTION_NAME = "initGroupOnError";
    /** the name of the updateGroup() function in CTL */
    public static final String UPDATE_GROUP_FUNCTION_NAME = "updateGroup";
    /** the name of the updateGroupOnError() function in CTL */
    public static final String UPDATE_GROUP_ON_ERROR_FUNCTION_NAME = "updateGroupOnError";
    /** the name of the finishGroup() function in CTL */
    public static final String FINISH_GROUP_FUNCTION_NAME = "finishGroup";
    /** the name of the finishGroupOnError() function in CTL */
    public static final String FINISH_GROUP_ON_ERROR_FUNCTION_NAME = "finishGroupOnError";
    /** the name of the updateTransform() function in CTL */
    public static final String UPDATE_TRANSFORM_FUNCTION_NAME = "updateTransform";
    /** the name of the updateTransformOnError() function in CTL */
    public static final String UPDATE_TRANSFORM_ON_ERROR_FUNCTION_NAME = "updateTransformOnError";
    /** the name of the transform() function in CTL */
    public static final String TRANSFORM_FUNCTION_NAME = "transform";
    /** the name of the transformOnError() function in CTL */
    public static final String TRANSFORM_ON_ERROR_FUNCTION_NAME = "transformOnError";

    /** the name of the counter param used in CTL */
    public static final String COUNTER_PARAM_NAME = "counter";
    /** the name of the groupAccumulator param used in CTL */
    public static final String GROUP_ACCUMULATOR_PARAM_NAME = "groupAccumulator";

    /** input records used when an array of input records is required */
    private final DataRecord[] inputRecords = new DataRecord[1];
    /** empty data record used instead of null group accumulator for better error reporting in scope of CTL */
    private final DataRecord emptyRecord = DataRecordFactory.newRecord(new DataRecordMetadata("emptyGroupAccumulator"));

    /** group arguments */
    private final TLValue[] groupArguments = new TLValue[] { new TLRecordValue((DataRecord) null) };
    /** group onError arguments */
    private final TLValue[] groupOnErrorArguments = new TLValue[] {
    		new TLStringValue(), new TLStringValue(), groupArguments[0] };
    /** update and updateTransform arguments */
    private final TLValue[] transformArguments = new TLValue[] {
    		TLValue.create(TLValueType.INTEGER), groupArguments[0] }; 
    /** update and updateTransform onError arguments */
    private final TLValue[] transformOnErrorArguments = new TLValue[] {
    		groupOnErrorArguments[0], groupOnErrorArguments[1], transformArguments[0], groupArguments[0] }; 

    /** the ID of the prepared initGroup() function */
    private int initGroupFunction;
    /** the ID of the prepared initGroupOnError() function */
    private int initGroupOnErrorFunction;
    /** the ID of the prepared updateGroup() function */
    private int updateGroupFunction;
    /** the ID of the prepared updateGroupOnError() function */
    private int updateGroupOnErrorFunction;
    /** the ID of the prepared finishGroup() function */
    private int finishGroupFunction;
    /** the ID of the prepared finishGroupOnError() function */
    private int finishGroupOnErrorFunction;
    /** the ID of the prepared updateTransform() function */
    private int updateTransformFunction;
    /** the ID of the prepared updateTransformOnError() function */
    private int updateTransformOnErrorFunction;
    /** the ID of the prepared transform() function */
    private int transformFunction;
    /** the ID of the prepared transformOnError() function */
    private int transformOnErrorFunction;

    /**
     * Creates an instance of the <code>RecordRollupTL</code> class.
     *
     * @param sourceCode the source code of the transformation
     * @param logger the logger to be used by this TL wrapper
     */
    public RecordRollupTL(String sourceCode, Logger logger) {
    	super(sourceCode, logger);
    }

    @Override
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

        initGroupFunction = wrapper.prepareFunctionExecution(INIT_GROUP_FUNCTION_NAME);
        initGroupOnErrorFunction = wrapper.prepareOptionalFunctionExecution(INIT_GROUP_ON_ERROR_FUNCTION_NAME);
        updateGroupFunction = wrapper.prepareFunctionExecution(UPDATE_GROUP_FUNCTION_NAME);
        updateGroupOnErrorFunction = wrapper.prepareOptionalFunctionExecution(UPDATE_GROUP_ON_ERROR_FUNCTION_NAME);
        finishGroupFunction = wrapper.prepareFunctionExecution(FINISH_GROUP_FUNCTION_NAME);
        finishGroupOnErrorFunction = wrapper.prepareOptionalFunctionExecution(FINISH_GROUP_ON_ERROR_FUNCTION_NAME);
        updateTransformFunction = wrapper.prepareFunctionExecution(UPDATE_TRANSFORM_FUNCTION_NAME);
        updateTransformOnErrorFunction = wrapper.prepareOptionalFunctionExecution(UPDATE_TRANSFORM_ON_ERROR_FUNCTION_NAME);
        transformFunction = wrapper.prepareFunctionExecution(TRANSFORM_FUNCTION_NAME);
        transformOnErrorFunction = wrapper.prepareOptionalFunctionExecution(TRANSFORM_ON_ERROR_FUNCTION_NAME);

		// initialize an empty data record to be used instead of a null group accumulator
        emptyRecord.init();
        emptyRecord.reset();
    }

    @Override
    public void initGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        wrapper.executePreparedFunction(initGroupFunction, inputRecord, initGroupArguments(null, groupAccumulator));
    }

    @Override
    public void initGroupOnError(Exception exception, DataRecord inputRecord, DataRecord groupAccumulator)
    		throws TransformException {
		if (initGroupOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Rollup failed!", exception);
		}

        wrapper.executePreparedFunction(initGroupOnErrorFunction, inputRecord,
        		initGroupArguments(exception, groupAccumulator));
    }

    @Override
    public boolean updateGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        return groupFunctionImpl(updateGroupFunction, null, inputRecord, groupAccumulator, false);
    }

    @Override
    public boolean updateGroupOnError(Exception exception, DataRecord inputRecord, DataRecord groupAccumulator)
    		throws TransformException {
		if (updateGroupOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Rollup failed!", exception);
		}

        return groupFunctionImpl(updateGroupOnErrorFunction, exception, inputRecord, groupAccumulator, false);
    }

    @Override
    public boolean finishGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        return groupFunctionImpl(finishGroupFunction, null, inputRecord, groupAccumulator, true);
    }

    @Override
    public boolean finishGroupOnError(Exception exception, DataRecord inputRecord, DataRecord groupAccumulator)
    		throws TransformException {
		if (finishGroupOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Rollup failed!", exception);
		}

        return groupFunctionImpl(finishGroupOnErrorFunction, exception, inputRecord, groupAccumulator, true);
    }

    private boolean groupFunctionImpl(int functionId, Exception exception, DataRecord inputRecord,
    		DataRecord groupAccumulator, boolean defaultReturnValue) {
		// set the error message to null so that the inherited getMessage() method works correctly if no error occurs
		errorMessage = null;

        TLValue result = wrapper.executePreparedFunction(functionId, inputRecord,
        		initGroupArguments(exception, groupAccumulator));

        if (result != null) {
            if (result.getType() == TLValueType.BOOLEAN) {
                return (result == TLBooleanValue.TRUE);
            }

            errorMessage = "Unexpected return result: " + result + " (" + result.getType() + ")";
        }

        return defaultReturnValue;
    }

    private TLValue[] initGroupArguments(Exception exception, DataRecord groupAccumulator) {
    	if (exception != null) {
    		// provide exception message and stack trace
	    	groupOnErrorArguments[0].setValue(ExceptionUtils.getMessage(null, exception));
	    	groupOnErrorArguments[1].setValue(ExceptionUtils.stackTraceToString(exception));

	    	// if group accumulator is empty we use an empty record for better error reporting in scope of CTL
	    	groupOnErrorArguments[2].setValue((groupAccumulator != null) ? groupAccumulator : emptyRecord);

	    	return groupOnErrorArguments;
    	}

        // if group accumulator is empty we use an empty record for better error reporting in scope of CTL
    	groupArguments[0].setValue((groupAccumulator != null) ? groupAccumulator : emptyRecord);

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
		if (updateTransformOnErrorFunction < 0) {
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
		if (transformOnErrorFunction < 0) {
			// no custom error handling implemented, throw an exception so the transformation fails
			throw new TransformException("Rollup failed!", exception);
		}

        return transformFunctionImpl(transformOnErrorFunction, exception, counter, inputRecord,
        		groupAccumulator, outputRecords);
    }

    private int transformFunctionImpl(int functionId, Exception exception, int counter, DataRecord inputRecord,
    		DataRecord groupAccumulator, DataRecord[] outputRecords) {
		// set the error message to null so that the inherited getMessage() method works correctly if no error occurs
		errorMessage = null;

        inputRecords[0] = inputRecord;

        TLValue result = wrapper.executePreparedFunction(functionId, inputRecords, outputRecords,
        		initTransformArguments(exception, counter, groupAccumulator));

        if (result != null) {
            if (result.getType().isNumeric()) {
                return result.getNumeric().getInt();
            }

            errorMessage = "Unexpected return result: " + result + " (" + result.getType() + ")";
        }

        return SKIP;
    }

    private TLValue[] initTransformArguments(Exception exception, int counter, DataRecord groupAccumulator) {
    	if (exception != null) {
    		// provide exception message, stack trace and call counter
	    	transformOnErrorArguments[0].setValue(ExceptionUtils.getMessage(null, exception));
	    	transformOnErrorArguments[1].setValue(ExceptionUtils.stackTraceToString(exception));
	    	transformOnErrorArguments[2].getNumeric().setValue(counter);

	    	// if group accumulator is empty we use an empty record for better error reporting in scope of CTL
	    	transformOnErrorArguments[3].setValue((groupAccumulator != null) ? groupAccumulator : emptyRecord);

	    	return transformOnErrorArguments;
    	}

        // if group accumulator is empty we use an empty record for better error reporting in scope of CTL
        transformArguments[0].getNumeric().setValue(counter);
    	transformArguments[1].setValue((groupAccumulator != null) ? groupAccumulator : emptyRecord);

    	return transformArguments;
    }

}
