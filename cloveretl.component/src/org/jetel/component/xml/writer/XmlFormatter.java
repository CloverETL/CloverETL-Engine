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
package org.jetel.component.xml.writer;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.Map;

import javax.xml.stream.XMLStreamException;

import org.jetel.component.xml.writer.model.WritableMapping;
import org.jetel.component.xml.writer.model.WritableMapping.MappingWriteState;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.Formatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * XmlFormatter which methods are called from MultiFileWriter.
 * 
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18 Jan 2011
 */
public class XmlFormatter implements Formatter {
	
	private WritableMapping mapping;
	private boolean omitNewLines;
	private String charset;
	
	private XmlStreamWriterImpl writer;
	
	private Map<Integer, DataRecord> availableData = new HashMap<Integer, DataRecord>();
	
	public XmlFormatter(WritableMapping mapping, boolean omitNewLines, String charset) {
		this.mapping = mapping;
		this.omitNewLines = omitNewLines;
		this.charset = charset;
	}

	@Override
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
	}

	@Override
	public void setDataTarget(Object outputDataTarget) throws IOException {
		close();

		if (outputDataTarget instanceof WritableByteChannel) {
			writer = new XmlStreamWriterImpl(Channels.newOutputStream((WritableByteChannel) outputDataTarget), charset, omitNewLines);
		} else {
			throw new IllegalArgumentException("parameter " + outputDataTarget + " is not instance of WritableByteChannel");
		}
	}

	@Override
	public void close() throws IOException {
		if (writer == null) {
			return;
		}
		flush();
		writer.close();
		writer = null;
	}

	@Override
	public int write(DataRecord record) throws IOException {
		try {
			mapping.setState(MappingWriteState.ALL);
			if (mapping.getPartitionElement() != null) {
				availableData.clear();
				availableData.put(mapping.getPartitionElement().getPortIndex(), record);
				mapping.getPartitionElement().writeRecord(this, availableData, record);
			} else {
				mapping.getRootElement().write(this, new HashMap<Integer, DataRecord>());
			}
		} catch (XMLStreamException e) {
			throw new IOException(e);
		}
		return 0;

	}

	@Override
	public int writeHeader() throws IOException {
		try {
			writer.writeStartDocument(mapping.getVersion());
			if (mapping.getPartitionElement() != null) {
				mapping.setState(MappingWriteState.HEADER);
				mapping.getRootElement().write(this, new HashMap<Integer, DataRecord>());
			}
		} catch (XMLStreamException e) {
			throw new IOException(e);
		}
		return 0;
	}

	@Override
	public int writeFooter() throws IOException {
		try {
			if (mapping.getPartitionElement() != null) {
				mapping.setState(MappingWriteState.FOOTER);
				mapping.getRootElement().write(this, new HashMap<Integer, DataRecord>());
			}
			writer.writeEndDocument();
		} catch (XMLStreamException e) {
			throw new IOException(e);
		}
		return 0;
	}

	@Override
	public void flush() throws IOException {
		writer.flush();
	}

	@Override
	public void finish() throws IOException {
		writeFooter();
	}

	@Override
	public void reset() {
	}

	public XmlStreamWriterImpl getWriter() {
		return writer;
	}

	public WritableMapping getMapping() {
		return mapping;
	}

}
