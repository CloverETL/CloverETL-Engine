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
 * Represents an interface of a rollup transform which processes groups of data records. Each group of data records
 * shares an output data record, referred to as a group "accumulator". This group "accumulator" is initialized when
 * the first data record of the group is encountered and updated for each data record in the group (including the first
 * and the last data record). When the last data record of the group is encountered, the processing of the group is
 * finished and the group "accumulator" can be sent to the output. Intermediate states of the group "accumulator" can
 * be publishes as well.
 *
 * @author Martin Zatopek, Javlin a.s. &lt;martin.zatopek@javlin.eu&gt;
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 24th February 2009
 * @since 24th February 2009
 */
public interface RecordRollup {

    /**
     * Initializes the rollup transformation. This method is called once at the beginning of the life-cycle of the
     * rollup transformation. Any allocation/initialization code should be placed here.
     *
     * @param parameters global graph parameters and parameters defined for the component which calls this transformation
     * @param inputMetadata meta data of input data records
     * @param outputMetadata meta data of output data records
     *
     * @throws ComponentNotReadyException if an error occurred during the initialization
     */
    public void init(Properties parameters, DataRecordMetadata inputMetadata, DataRecordMetadata outputMetadata)
            throws ComponentNotReadyException;

    /**
     * This method is called for the first data record in a group. Any internal group initialization code and/or
     * initialization of the output data record should be placed here.
     *
     * @param inputRecord the first input data record in the group
     * @param outputRecord the output data record that will be used as an "accumulator" for the group
     *
     * @throws TransformException if any error occurred during the initialization
     */
    public void initGroup(DataRecord inputRecord, DataRecord outputRecord) throws TransformException;

    /**
     * This method is called for each data record (including the first one as well as the last one) in a group
     * in order to update the group "accumulator".
     *
     * @param inputRecord the current input data record
     * @param outputRecord the output data record that serves as a group "accumulator"
     *
     * @return <code>true</code> if the intermediate output data record should be sent to the output,
     * <code>false</code> otherwise
     *
     * @throws TransformException if any error occurred during the update
     */
    public boolean updateGroup(DataRecord inputRecord, DataRecord outputRecord) throws TransformException;

    /**
     * This method is called for the last data record in a group in order to finish the group processing.
     *
     * @param inputRecord the last input data record
     * @param outputRecord the output data record that serves as a group "accumulator"
     *
     * @return <code>true</code> if the complete output data record should be sent to the output,
     * <code>false</code> otherwise
     *
     * @throws TransformException if any error occurred during the final processing
     */
    public boolean finishGroup(DataRecord inputRecord, DataRecord outputRecord) throws TransformException;

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
