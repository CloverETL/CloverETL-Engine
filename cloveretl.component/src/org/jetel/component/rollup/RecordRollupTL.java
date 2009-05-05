/*
 * jETeL/Clover.ETL - Java based ETL application framework.
 * Copyright (C) 2002-2009  David Pavlis <david.pavlis@javlin.eu>
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
package org.jetel.component.rollup;

import java.util.Properties;

import org.apache.commons.logging.LogFactory;
import org.jetel.component.WrapperTL;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
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
 * @version 5th May 2009
 * @since 28th April 2009
 */
public class RecordRollupTL implements RecordRollup {

    /** the name of the init() function in CTL */
    public static final String FUNCTION_INIT_NAME = "init";
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
    /** the name of the reset() function in CTL */
    public static final String FUNCTION_RESET_NAME = "reset";
    /** the name of the free() function in CTL */
    public static final String FUNCTION_FREE_NAME = "free";

    /** the TL wrapper used to execute all the functions */
    private final WrapperTL wrapper;

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

    /**
     * Creates an instance of the <code>RecordRollupTL</code> class.
     *
     * @param sourceCode the source code of the transformation
     * @param graph the graph this transformation belongs to
     */
    public RecordRollupTL(String sourceCode, TransformationGraph graph) {
        wrapper = new WrapperTL(sourceCode, LogFactory.getLog(RecordRollupTL.class));
        wrapper.setGraph(graph);
    }

    public void init(Properties parameters, DataRecordMetadata inputMetadata, DataRecordMetadata accumulatorMetadata,
            DataRecordMetadata[] outputMetadata) throws ComponentNotReadyException {
        wrapper.setParameters(parameters);
        wrapper.setMetadata(new DataRecordMetadata[] { inputMetadata }, outputMetadata);
        wrapper.init();

        try {
            wrapper.execute(FUNCTION_INIT_NAME, null);
        } catch (JetelException exception) {
            // OK, don't do anything, function init() is not necessary
        }

        functionInitGroupId = wrapper.prepareFunctionExecution(FUNCTION_INIT_GROUP_NAME);
        functionUpdateGroupId = wrapper.prepareFunctionExecution(FUNCTION_UPDATE_GROUP_NAME);
        functionFinishGroupId = wrapper.prepareFunctionExecution(FUNCTION_FINISH_GROUP_NAME);
        functionUpdateTransformId = wrapper.prepareFunctionExecution(FUNCTION_UPDATE_TRANSFORM_NAME);
        functionTransformId = wrapper.prepareFunctionExecution(FUNCTION_TRANSFORM_NAME);
    }

    public void initGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        wrapper.executePreparedFunction(functionInitGroupId, inputRecord,
                new TLValue[] { new TLRecordValue(groupAccumulator) });
    }

    public boolean updateGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        TLValue result = wrapper.executePreparedFunction(functionUpdateGroupId, inputRecord,
                new TLValue[] { new TLRecordValue(groupAccumulator) });

        if (result != null) {
            return (result == TLBooleanValue.TRUE);
        }

        return false;
    }

    public boolean finishGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
        TLValue result = wrapper.executePreparedFunction(functionFinishGroupId, inputRecord,
                new TLValue[] { new TLRecordValue(groupAccumulator) });

        if (result != null) {
            return (result == TLBooleanValue.TRUE);
        }

        return true;
    }

    public int updateTransform(int counter, DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
            throws TransformException {
        TLValue counterTL = TLValue.create(TLValueType.INTEGER);
        counterTL.setValue(counter);

        TLValue result = wrapper.executePreparedFunction(functionUpdateTransformId, new DataRecord[] { inputRecord },
                outputRecords, new TLValue[] { counterTL, new TLRecordValue(groupAccumulator) });

        if (result != null) {
            if (result == TLBooleanValue.TRUE) {
                return ALL;
            }

            if (result.getType().isNumeric()) {
                return result.getNumeric().getInt();
            }
        }

        return SKIP;
    }
    
    public int transform(int counter, DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
            throws TransformException {
        TLValue counterTL = TLValue.create(TLValueType.INTEGER);
        counterTL.setValue(counter);

        TLValue result = wrapper.executePreparedFunction(functionTransformId, new DataRecord[] { inputRecord },
                outputRecords, new TLValue[] { counterTL, new TLRecordValue(groupAccumulator) });

        if (result != null) {
            if (result == TLBooleanValue.TRUE) {
                return ALL;
            }

            if (result.getType().isNumeric()) {
                return result.getNumeric().getInt();
            }
        }

        return SKIP;
    }

    public void reset() throws ComponentNotReadyException {
        try {
            wrapper.execute(FUNCTION_RESET_NAME, null);
        } catch (JetelException exception) {
            // OK, don't do anything, function reset() is not necessary
        }

        wrapper.reset();
    }

    public void free() {
        try {
            wrapper.execute(FUNCTION_FREE_NAME, null);
        } catch (JetelException exception) {
            // OK, don't do anything, function free() is not necessary
        }
    }

}
