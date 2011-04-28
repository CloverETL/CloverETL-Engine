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
package org.jetel.component.xml.writer.model;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.logging.Log;
import org.jetel.component.RecordFilter;
import org.jetel.component.RecordFilterFactory;
import org.jetel.component.xml.writer.DataIterator;
import org.jetel.component.xml.writer.PortData;
import org.jetel.component.xml.writer.XmlFormatter;
import org.jetel.component.xml.writer.model.WritableMapping.MappingWriteState;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.graph.TransformationGraph;

/**
 * @author LKREJCI (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20 Dec 2010
 */
public class WritableLoopElement extends WritableElement {
	
	public static final String FILTER_PREFIX = "//#CTL2\n";
	
	private final boolean hidden;
	
	private final int portIndex;
	private final PortData portData;
	private final DataRecord record;
	private final int parentPort;
	private final WritableLoopElement parentLoopElement;
	
	
	private final RecordFilter recordFilter;
	private int[] keys;
	private int[] parentKeys;
	
	private DataIterator iterator;

	public WritableLoopElement(WritableLoopElement parentLoopElement, String name, String namespaceURI, boolean hidden,
			PortData portData, int parentPort, int[] keys, int[] parentKeys, String filterExpression, TransformationGraph graph,
			String componentId, Log logger) throws ComponentNotReadyException {
		super(name, namespaceURI, false);
		this.parentLoopElement = parentLoopElement;
		this.hidden = hidden;
		this.parentPort = parentPort;
		this.portData = portData;
		this.keys = keys;
		this.parentKeys = parentKeys;
		if (filterExpression != null) {
			this.recordFilter = RecordFilterFactory.createFilter(FILTER_PREFIX + filterExpression, portData.getRecord().getMetadata(), graph, componentId, logger);
		} else {
			this.recordFilter = null;
		}
		
		record = portData.getRecord();
		record.init();
		
		portIndex = portData.getInPort().getInputPortNumber();
	}

	@Override
	public void write(XmlFormatter formatter, Map<Integer, DataRecord> availableData) throws XMLStreamException, IOException {
		WritableMapping mapping = formatter.getMapping();
		MappingWriteState state = mapping.getState();
		
		if (state == MappingWriteState.HEADER && this == mapping.getPartitionElement()) {
			mapping.setState(MappingWriteState.NOTHING);
		} else if (state == MappingWriteState.FOOTER && this == mapping.getPartitionElement()) {
			mapping.setState(MappingWriteState.ALL);
		} else if (state == MappingWriteState.ALL || state == MappingWriteState.HEADER) {
			DataRecord writeRecord;
			Map<Integer, DataRecord> currentAvailableData = new HashMap<Integer, DataRecord>(availableData);
			currentAvailableData.put(portIndex, record);

			iterator = portData.iterator(keys, parentKeys, availableData.get(parentPort),
					parentLoopElement != null ? parentLoopElement.getNextRecord() : null);
			
			while (iterator.hasNext()) {
				writeRecord = iterator.next();
				writeRecord(formatter, currentAvailableData, writeRecord);
			}
		}
	}

	@Override
	public boolean isEmpty(Map<Integer, DataRecord> availableData) {
		return false;
	}
	
	public void writeRecord(XmlFormatter formatter, Map<Integer, DataRecord> availableData, DataRecord writeRecord) throws XMLStreamException, IOException {
		try {
			if (recordFilter != null && !recordFilter.isValid(writeRecord)) {
				return;
			}
		} catch (TransformException e) {
			throw new XMLStreamException(e);
		}
		record.copyFrom(writeRecord);
		if (!hidden) {
			if (children.length == 0) {
				formatter.getWriter().writeEmptyElement(name);
			} else {
				formatter.getWriter().writeStartElement(name);
			}
			for (Writable namespace : namespaces) {
				namespace.write(formatter, availableData);
			}
			for (Writable attribute : attributes) {
				attribute.write(formatter, availableData);
			}
		}
		for (Writable child : children) {
			child.write(formatter, availableData);
		}
		if (!hidden && children.length != 0) {
			formatter.getWriter().writeEndElement();
		}
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
