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
package org.jetel.component;

import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPortDirect;
import org.jetel.graph.Result;
import org.jetel.graph.runtime.tracker.BasicComponentTokenTracker;
import org.jetel.graph.runtime.tracker.ComponentTokenTracker;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Created to fix wrong inheritance of ExtFilter and Condition
 * 
 * @author salamonp (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 18. 5. 2015
 */
public abstract class ExtFilterBase extends Node {

	protected static final String XML_FILTEREXPRESSION_ATTRIBUTE = "filterExpression";

	protected String filterExpression;

	protected RecordFilter filter = null;

	protected final static int READ_FROM_PORT = 0;
	protected final static int WRITE_TO_PORT = 0;
	protected final static int REJECTED_PORT = 1;

	public ExtFilterBase(String id) {
		super(id);
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}
		super.init();
		if (filterExpression != null) {
			initFilterExpression();
		}
	}

	protected void initFilterExpression() throws ComponentNotReadyException {
		filter = RecordFilterFactory.createFilter(filterExpression, getInMetadata().get(READ_FROM_PORT), getGraph(), getId(), LogFactory.getLog(ExtFilterBase.class));
	}

	@Override
	public Result execute() throws Exception {
		InputPortDirect inPort = getInputPortDirect(READ_FROM_PORT);
		OutputPortDirect outPort = getOutputPortDirect(WRITE_TO_PORT);
		OutputPortDirect rejectedPort = getOutputPortDirect(REJECTED_PORT);
		boolean isData = true;
		DataRecord record = DataRecordFactory.newRecord(getInputPort(READ_FROM_PORT).getMetadata());
		CloverBuffer recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);

		while (isData && runIt) {
			try {
				if (!inPort.readRecordDirect(recordBuffer)) {
					isData = false;
					break;
				}
				record.deserialize(recordBuffer);
				if (filter.isValid(record)) {
					recordBuffer.rewind();
					outPort.writeRecordDirect(recordBuffer);
				} else if (rejectedPort != null) {
					recordBuffer.rewind();
					rejectedPort.writeRecordDirect(recordBuffer);
				}

			} catch (ClassCastException ex) {
				throw new JetelException("Invalid filter expression - does not evaluate to TRUE/FALSE !", ex);
			}
			SynchronizeUtils.cloverYield();
		}
		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 1, 1) || !checkOutputPorts(status, 1, 2)) {
			return status;
		}
		checkMetadata(status, getInMetadata(), getOutMetadata());
		return status;
	}

	public void setFilterExpression(String filterExpression) {
		this.filterExpression = filterExpression;
	}

	public RecordFilter getRecordFilter() {
		return filter;
	}

	@Override
	protected ComponentTokenTracker createComponentTokenTracker() {
		return new BasicComponentTokenTracker(this);
	}

}
