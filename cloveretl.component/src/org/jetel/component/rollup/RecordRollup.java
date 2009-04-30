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

import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.NotInitializedException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * <p>Represents an interface of a rollup transform which processes groups of data records. Each group of data records
 * may share a data record referred to as a group "accumulator". This group "accumulator" is created an initialized
 * when the first data record of the group is encountered and updated for each data record in the group (including the
 * first and the last data record). When the last data record of the group is encountered, the processing of the group
 * is finished and the group "accumulator" is disposed.</p>
 * <p>The lifecycle of a rollup transform is as follows:</p>
 * <ul>
 *   <li>The {@link #init(Properties, DataRecordMetadata, DataRecordMetadata, DataRecordMetadata[])} method is called
 *   to initialize the transform.</li>
 *   <li>For each input data record as a current data record:
 *     <ul>
 *       <li>If the current data record belongs to a new group:
 *         <ul>
 *           <li>If requested, a group "accumulator" is created.</li>
 *           <li>The {@link #initGroup(DataRecord, DataRecord)} method is called to initialize processing of the group
 *           and to initialize the "accumulator" (if it exists).</li>
 *         </ul>
 *       </li>
 *       <li>The {@link #updateGroup(DataRecord, DataRecord)} method is called for the current data record and
 *       the corresponding group "accumulator" (if it was requested).</li>
 *       <li>If the method returned <code>true</code>, the {@link #transform(DataRecord, DataRecord, DataRecord[])}
 *       method is called repeatedly to generate output data record(s) until it returns RecordRollup.SKIP.</li>
 *       <li>If the current data record is the last one in its group:
 *         <ul>
 *           <li>The {@link #finishGroup(DataRecord, DataRecord)} method is called to finish the group processing.</li>
 *           <li>If the method returned <code>true</code>, the {@link #transform(DataRecord, DataRecord, DataRecord[])}
 *           method is called repeatedly to generate output data record(s) until it returns RecordRollup.SKIP.</li>
 *           <li>If the group "accumulator" was requested, its contents is disposed.</li>
 *         </ul>
 *       </li>
 *     </ul>
 *   </li>
 *   <li>The {@link #free()} method is called to free any allocated resources.</li>
 * </ul>
 *
 * @author Martin Zatopek, Javlin a.s. &lt;martin.zatopek@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 30th April 2009
 * @since 24th February 2009
 */
public interface RecordRollup {

    /** the return value of the transform() method specifying that the record will be sent to all the output ports */
    public static final int ALL = Integer.MAX_VALUE;
    /** the return value of the transform() method specifying that the record will be skipped */
    public static final int SKIP = -1;

    /**
     * Initializes the rollup transformation. This method is called once at the beginning of the life-cycle of the
     * rollup transformation. Any internal allocation/initialization code should be placed here.
     *
     * @param parameters global graph parameters and parameters defined for the component which calls this transformation
     * @param inputMetadata meta data of input data records
     * @param accumulatorMetadata meta data of a group "accumulator" used to store intermediate results
     * or <code>null</code> if no meta data were specified
     * @param outputMetadata array of meta data of output data records
     *
     * @throws ComponentNotReadyException if an error occurred during the initialization
     */
    public void init(Properties parameters, DataRecordMetadata inputMetadata, DataRecordMetadata accumulatorMetadata,
            DataRecordMetadata[] outputMetadata) throws ComponentNotReadyException;

    /**
     * This method is called for the first data record in a group. Any initialization of the group "accumulator" should
     * be placed here.
     *
     * @param inputRecord the first input data record in the group
     * @param groupAccumulator the data record used as an "accumulator" for the group or <code>null</code> if none
     * was requested
     *
     * @throws TransformException if any error occurred during the initialization
     */
    public void initGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException;

    /**
     * This method is called for each data record (including the first one as well as the last one) in a group
     * in order to update the group "accumulator".
     *
     * @param inputRecord the current input data record
     * @param groupAccumulator the data record used as an "accumulator" for the group or <code>null</code> if none
     * was requested
     *
     * @return <code>true</code> if the {@link #transform(DataRecord, DataRecord, DataRecord[])} method should be called
     * to generate an output data record and send it to the output, <code>false</code> otherwise
     *
     * @throws TransformException if any error occurred during the update
     */
    public boolean updateGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException;

    /**
     * This method is called for the last data record in a group in order to finish the group processing.
     *
     * @param inputRecord any input data record from the current group
     * @param groupAccumulator the data record used as an "accumulator" for the group or <code>null</code> if none
     * was requested
     *
     * @return <code>true</code> if the {@link #transform(DataRecord, DataRecord, DataRecord[])} method should be called
     * to generate an output data record and send it to the output, <code>false</code> otherwise
     *
     * @throws TransformException if any error occurred during the final processing
     */
    public boolean finishGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException;

    /**
     * This method is used to generate an output data record based on the input data record and the contents of the
     * group "accumulator" (if it was requested). The output data record will be sent the output when this method finishes.
     *
     * @param inputRecord any input data record from the current group
     * @param groupAccumulator the data record used as an "accumulator" for the group or <code>null</code> if none
     * was requested
     * @param outputRecords output data records to be filled with data
     *
     * @return RecordRollup.ALL -- send the data record(s) to all the output ports<br>
     *         >= 0 -- send the data record(s) to a specified output port<br>
     *         RecordRollup.SKIP -- end processing of the current group<br>
     *         < RecordRollup.SKIP -- fatal error, stop the execution
     *
     * @throws TransformException if any error occurred during the transformation
     */
    public int transform(DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
            throws TransformException;

    /**
     * Resets the rollup transformation to the initial state (for another execution). This method can be called only
     * if the {@link #init(Properties, DataRecordMetadata, DataRecordMetadata)} method has already been called.
     *
     * @throws NotInitializedException if the transformation is not initialized
     * @throws ComponentNotReadyException if an error occurred during the reset
     */
    public void reset() throws ComponentNotReadyException;

    /**
     * Frees any resources allocated in the {@link #init(Properties, DataRecordMetadata, DataRecordMetadata)} method.
     * If the rollup transformation is not initialized, a call to this method has no effect.
     */
    public void free();

}
