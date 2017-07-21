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
package org.jetel.component.tree.writer.model.runtime;

import java.io.IOException;
import java.util.Map;

import org.jetel.component.RecordsFilter;
import org.jetel.component.tree.writer.TreeFormatter;
import org.jetel.component.tree.writer.model.runtime.WritableMapping.MappingWriteState;
import org.jetel.component.tree.writer.portdata.DataIterator;
import org.jetel.component.tree.writer.portdata.PortData;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 9.1.2012
 */
public class PortBinding {

	private WritableContainer container;

	private final int portIndex;
	private final PortData portData;
	private final DataRecord record;
	private final int parentPort;
	private final PortBinding parentBinding;

	private final RecordsFilter recordFilter;
	private int[] keys;
	private int[] parentKeys;

	private DataIterator iterator;

	public PortBinding(PortBinding parentBinding, PortData portData, int[] keys, int[] parentKeys,
			RecordsFilter recordFilter) throws ComponentNotReadyException {
		this.parentBinding = parentBinding;
		this.parentPort = parentBinding != null ? parentBinding.getPortIndex() : -1;
		this.portData = portData;
		this.keys = keys;
		this.parentKeys = parentKeys;
		this.recordFilter = recordFilter;

		DataRecordMetadata metadata = portData.getInPort().getMetadata();
		record = DataRecordFactory.newRecord(metadata);
		record.init();

		portIndex = portData.getInPort().getInputPortNumber();
	}

	protected void setContainer(WritableContainer container) {
		this.container = container;
	}

	public void write(TreeFormatter formatter, DataRecord[] availableData) throws JetelException, IOException {
		WritableMapping mapping = formatter.getMapping();
		MappingWriteState state = mapping.getState();

		if (state == MappingWriteState.HEADER && container == mapping.getPartitionElement()) {
			mapping.setState(MappingWriteState.NOTHING);
			container.writeContainerStart(formatter, availableData);
		} else if (state == MappingWriteState.FOOTER && container == mapping.getPartitionElement()) {
			mapping.setState(MappingWriteState.ALL);
			container.writeContainerEnd(formatter, availableData);
		} else if (state == MappingWriteState.ALL || state == MappingWriteState.HEADER) {
			DataRecord[] currentAvailableData = new DataRecord[availableData.length];
			System.arraycopy(availableData, 0, currentAvailableData, 0, availableData.length);

			DataRecord keyDataRecord = null;
			DataRecord nextKeyDataRecord = null;
			if (parentBinding != null) {
				keyDataRecord = availableData[parentPort];
				nextKeyDataRecord = parentBinding.getNextRecord();
			}
			
			iterator = portData.iterator(keys, parentKeys, keyDataRecord, nextKeyDataRecord);

			container.writeContainerStart(formatter, availableData);
			while (iterator.hasNext()) {
				writeRecord(formatter, currentAvailableData, iterator.next());
			}
			container.writeContainerEnd(formatter, availableData);
		}
	}

	private void writeRecord(TreeFormatter formatter, DataRecord[] currentAvailableData, DataRecord writeRecord)
			throws JetelException, IOException {
		try {
			if (recordFilter != null) {
				currentAvailableData[portIndex] = writeRecord;
				if (!recordFilter.isValid(currentAvailableData)) {
					return;
				}
			}
		} catch (TransformException e) {
			throw new JetelException(e);
		}
		record.copyFrom(writeRecord);
		currentAvailableData[portIndex] = record;

		container.writeContent(formatter, currentAvailableData);
	}

	public boolean isEmpty(Map<Integer, DataRecord> availableData) {
		return false;
	}

	public int getPortIndex() {
		return portIndex;
	}

	public DataRecord getRecord() {
		return record;
	}

	public DataRecord getNextRecord() {
		return iterator.peek();
	}

	public PortData getPortData() {
		return portData;
	}

	public void setIterator(DataIterator iterator) {
		this.iterator = iterator;
	}

}
