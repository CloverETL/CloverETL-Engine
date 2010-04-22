/*
 * jETeL/Clover - Java based ETL application framework.
 * Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
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
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 */
package org.jetel.component.rollup;

import java.util.Properties;

import org.jetel.ctl.CTLCompilable;
import org.jetel.ctl.CTLEntryPoint;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Ancestor for any Java code generated from CTL code in the Rollup component.
 *
 * @author Martin Janik, Javlin a.s. &lt;martin.janik@javlin.eu&gt;
 *
 * @version 22nd April 2010
 * @created 22nd April 2010
 *
 * @see RecordRollup
 * @see Rollup
 */
public abstract class CTLRecordRollup implements RecordRollup, CTLCompilable {

	/** an empty array of data records used for calls to functions that do not access to any data records */
    private static final DataRecord[] NO_DATA_RECORDS = new DataRecord[0];

    /** input data record passed to CTL functions */
    private DataRecord inputRecord;
    /** an array for output data records passed to CTL functions */
	private DataRecord[] outputRecords;

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

	/**
	 * Initializes global scope via call to the {@link #global()} method, then calls user initialization code
	 * via the {@link #init()} method.
	 * <p>
	 * All parameters of the call are ignored.
	 */
	public void init(Properties parameters, DataRecordMetadata inputMetadata, DataRecordMetadata accumulatorMetadata,
			DataRecordMetadata[] outputMetadata) throws ComponentNotReadyException {
		global();
		init();
	}

	@CTLEntryPoint(name = "init", required = false)
	public void init() {
		// do nothing by default
	}

	public void initGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
		this.inputRecord = inputRecord;
		this.outputRecords = NO_DATA_RECORDS;

		initGroup(groupAccumulator);
	}

	@CTLEntryPoint(name = "initGroup", parameterNames = { "groupAccumulator" }, required = true)
	public abstract void initGroup(DataRecord groupAccumulator) throws TransformException;

	public boolean updateGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
		this.inputRecord = inputRecord;
		this.outputRecords = NO_DATA_RECORDS;

		return updateGroup(groupAccumulator);
	}

	@CTLEntryPoint(name = "updateGroup", parameterNames = { "groupAccumulator" }, required = true)
	public abstract boolean updateGroup(DataRecord groupAccumulator) throws TransformException;

	public boolean finishGroup(DataRecord inputRecord, DataRecord groupAccumulator) throws TransformException {
		this.inputRecord = inputRecord;
		this.outputRecords = NO_DATA_RECORDS;

		return finishGroup(groupAccumulator);
	}

	@CTLEntryPoint(name = "finishGroup", parameterNames = { "groupAccumulator" }, required = true)
	public abstract boolean finishGroup(DataRecord groupAccumulator) throws TransformException;

	public int updateTransform(int counter, DataRecord inputRecord, DataRecord groupAccumulator,
			DataRecord[] outputRecords) throws TransformException {
		this.inputRecord = inputRecord;
		this.outputRecords = NO_DATA_RECORDS;

		return updateTransform(counter, groupAccumulator);
	}

	@CTLEntryPoint(name = "updateTransform", parameterNames = { "counter", "groupAccumulator" }, required = true)
	public abstract int updateTransform(int counter, DataRecord groupAccumulator) throws TransformException;

	public int transform(int counter, DataRecord inputRecord, DataRecord groupAccumulator, DataRecord[] outputRecords)
			throws TransformException {
		this.inputRecord = inputRecord;
		this.outputRecords = NO_DATA_RECORDS;

		return transform(counter, groupAccumulator);
	}

	@CTLEntryPoint(name = "transform", parameterNames = { "counter", "groupAccumulator" }, required = true)
	public abstract int transform(int counter, DataRecord groupAccumulator) throws TransformException;

	@CTLEntryPoint(name = "getMessage", required = false)
	public String getMessage() {
		// null by default
		return null;
	}

	@CTLEntryPoint(name = "finished", required = false)
	public void finished() {
		// do nothing by default
	}

	@CTLEntryPoint(name = "reset", required = false)
	public void reset() throws ComponentNotReadyException {
		// do nothing by default
	}

	@CTLEntryPoint(name = "global", required = false)
	public void global() throws ComponentNotReadyException {
		// do nothing by default
	}

	public DataRecord getInputRecord(int index) {
		return (index == 0) ? inputRecord : null;
	}

	public DataRecord getOutputRecord(int index) {
		return (index >= 0 && index < outputRecords.length) ? outputRecords[index] : null;
	}

}
