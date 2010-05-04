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
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * <p>Represents an interface of a rollup transform which processes groups of data records. Each group of data records
 * may share a data record referred to as a group "accumulator". It may be used to store intermediate results. The group
 * "accumulator" is created an initialized when the first data record of a group is encountered and updated for each
 * data record in this group (including the first and the last data record). When the last data record of a group is
 * encountered, processing of this group is finished and the group "accumulator" is disposed. If the input data records
 * are not sorted, each group is finished as soon as all input data records are read and processed.</p>
 * <p>The life cycle of a rollup transform is as follows:</p>
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
 *       <li>If the method returned <code>true</code>, the {@link #updateTransform(int, DataRecord, DataRecord, DataRecord[])}
 *       method is called repeatedly to generate output data record(s) until it returns RecordRollup.SKIP. If it returns
 *       value < RecordRollup.SKIP, the {@link #getMessage()} method is called.</li>
 *       <li>If the current data record is the last one in its group:
 *         <ul>
 *           <li>The {@link #finishGroup(DataRecord, DataRecord)} method is called to finish the group processing.</li>
 *           <li>If the method returned <code>true</code>, the {@link #transform(int, DataRecord, DataRecord, DataRecord[])}
 *           method is called repeatedly to generate output data record(s) until it returns RecordRollup.SKIP. If it
 *           returns value < RecordRollup.SKIP, the {@link #getMessage()} method is called.</li>
 *           <li>If the group "accumulator" was requested, its contents is disposed.</li>
 *         </ul>
 *       </li>
 *     </ul>
 *   </li>
 *   <li>The {@link #finished()} method is called to notify that the rollup transform finished.</li>
 *   <li>The {@link #reset()} method may optionally be called to reset the transformation and so that the previous
 *   two steps may be executed again.</li>
 * </ul>
 *
 * @author Martin Zatopek, Javlin a.s. &lt;martin.zatopek@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 4th May 2010
 * @since 24th February 2009
 */
public interface RecordRollup {

    /** the return value of the transform() method specifying that the record will be sent to all the output ports */
    public static final int ALL = Integer.MAX_VALUE;
    /** the return value of the transform() method specifying that the record will be skipped */
    public static final int SKIP = -1;

    /**
     * Associates a graph with this rollup transform.
     *
     * @param graph a <code>TransformationGraph</code> graph to be set
     */
    public void setGraph(TransformationGraph graph);

    /**
	 * @return a <code>TransformationGraph</code> associated with this rollup transform, or <code>null</code>
	 * if no graph is associated
	 */
    public TransformationGraph getGraph();

    /**
     * Initializes the rollup transform. This method is called once at the beginning of the life-cycle of the rollup
     * transform. Any internal allocation/initialization code should be placed here.
     *
     * @param parameters custom component attributes defined for the component which calls this transformation
     * @param inputMetadata metadata of input data records
     * @param accumulatorMetadata metadata of a group "accumulator" used to store intermediate results
     * or <code>null</code> if no metadata were specified
     * @param outputMetadata array of metadata of output data records
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
     * @throws TransformException if any error occurred during initialization of the group
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
     * @return <code>true</code> if the {@link #updateTransform(int, DataRecord, DataRecord, DataRecord[])} method should
     * be called to generate output data record(s) and send them to the output, <code>false</code> otherwise
     *
     * @throws TransformException if any error occurred during update of the group
     */
    public boolean updateGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException;

    /**
     * This method is called for the last data record in a group in order to finish the group processing.
     *
     * @param inputRecord any input data record from the current group
     * @param groupAccumulator the data record used as an "accumulator" for the group or <code>null</code> if none
     * was requested
     *
     * @return <code>true</code> if the {@link #transform(int, DataRecord, DataRecord, DataRecord[])} method should
     * be called to generate output data record(s) and send them to the output, <code>false</code> otherwise
     *
     * @throws TransformException if any error occurred during final processing of the group
     */
    public boolean finishGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException;

    /**
     * This method is used to generate output data records based on the input data record and the contents of the group
     * "accumulator" (if it was requested). The output data record will be sent to the output when this method finishes.
     * This method is called whenever the {@link #updateGroup(DataRecord, DataRecord)} method returns true.
     *
     * @param counter the number of previous calls to this method for the current group update
     * @param inputRecord the current input data record
     * @param groupAccumulator the data record used as an "accumulator" for the group or <code>null</code> if none
     * was requested
     * @param outputRecords output data records to be filled with data
     *
     * @return RecordRollup.ALL -- send the data record(s) to all the output ports<br>
     *         >= 0 -- send one data record to a specified output port<br>
     *         RecordRollup.SKIP -- end processing of the current group<br>
     *         < RecordRollup.SKIP -- fatal error, stop the execution
     *
     * @throws TransformException if any error occurred during the transformation
     */
    public int updateTransform(int counter, DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
            throws TransformException;
    
    /**
     * This method is used to generate output data records based on the input data record and the contents of the group
     * "accumulator" (if it was requested). The output data record will be sent to the output when this method finishes.
     * This method is called whenever the {@link #finishGroup(DataRecord, DataRecord)} method returns true.
     *
     * @param counter the number of previous calls to this method for the current group
     * @param inputRecord any input data record from the current group
     * @param groupAccumulator the data record used as an "accumulator" for the group or <code>null</code> if none
     * was requested
     * @param outputRecords output data records to be filled with data
     *
     * @return RecordRollup.ALL -- send the data record(s) to all the output ports<br>
     *         >= 0 -- send one data record to a specified output port<br>
     *         RecordRollup.SKIP -- end processing of the current group<br>
     *         < RecordRollup.SKIP -- fatal error, stop the execution
     *
     * @throws TransformException if any error occurred during the transformation
     */
    public int transform(int counter, DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
            throws TransformException;

    /**
     * This method is called if the {@link #updateTransform(int, DataRecord, DataRecord, DataRecord[])} or
     * {@link #transform(int, DataRecord, DataRecord, DataRecord[])} method returns value < RecordRollup.SKIP.
     *
     * @return a user-defined error message when an error occurs
     */
    public String getMessage();

    /**
     * Called at the end of the rollup transform after all input data records were processed.
     */
    public void finished();

    /**
     * Resets the rollup transform to the initial state (for another execution). This method can be called only
     * if the {@link #init(Properties, DataRecordMetadata, DataRecordMetadata)} method has already been called.
     *
     * @throws ComponentNotReadyException if an error occurred during the reset
     */
    public void reset() throws ComponentNotReadyException;

}
