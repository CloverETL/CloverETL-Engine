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
package org.jetel.component.tree.writer;

import java.io.IOException;
import java.util.Arrays;

import org.jetel.component.tree.writer.model.runtime.WritableMapping;
import org.jetel.component.tree.writer.model.runtime.WritableMapping.MappingWriteState;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.AbstractFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * TreeFormatter which methods are called from MultiFileWriter.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 18 Jan 2011
 */
public abstract class TreeFormatter extends AbstractFormatter {

	private WritableMapping mapping;
	private final int availableDataArrayLength;

	private DataRecord[] availableData;

	public TreeFormatter(WritableMapping mapping, int maxPortIndex) {
		if (mapping == null) {
			throw new IllegalArgumentException("Mapping cannot be null");
		}
		if (maxPortIndex < 0) {
			throw new IllegalArgumentException("Port index cannot be negative");
		}
		this.mapping = mapping;
		this.availableDataArrayLength = maxPortIndex + 1;
	}

	@Override
	public void init(DataRecordMetadata _metadata) throws ComponentNotReadyException {
		availableData = new DataRecord[availableDataArrayLength];
	}

	@Override
	public int write(DataRecord record) throws IOException {
		try {
			mapping.setState(MappingWriteState.ALL);
			if (mapping.getPartitionElement() != null) {
				Arrays.fill(availableData, null);
				availableData[mapping.getPartitionElement().getPortBinding().getPortIndex()] =  record;
				mapping.getPartitionElement().writeContent(this, availableData);
			}
		} catch (JetelException e) {
			throw new IOException(e);
		}
		return 0;

	}

	@Override
	public int writeHeader() throws IOException {
		try {
			getTreeWriter().writeStartTree();
			mapping.setState(MappingWriteState.HEADER);
			mapping.getRootElement().write(this, new DataRecord[availableDataArrayLength]);
		} catch (JetelException e) {
			throw new IOException(e);
		}
		return 0;
	}

	@Override
	public int writeFooter() throws IOException {
		try {
			mapping.setState(MappingWriteState.FOOTER);
			mapping.getRootElement().write(this, new DataRecord[availableDataArrayLength]);
			getTreeWriter().writeEndTree();
		} catch (JetelException e) {
			throw new IOException(e);
		}
		return 0;
	}

	@Override
	public void reset() {
	}

	@Override
	public void finish() throws IOException {
		writeFooter();
		flush();
	}

	public abstract TreeWriter getTreeWriter();
	
	/**
	 * Whether receiver is capable directly assign list data fields.
	 * @return
	 */
	public abstract boolean isListSupported();

	public CollectionWriter getCollectionWriter() {
		throw new UnsupportedOperationException("This format does not support collections");
	}

	public NamespaceWriter getNamespaceWriter() {
		throw new UnsupportedOperationException("This format does not support namespaces");
	}

	public AttributeWriter getAttributeWriter() {
		throw new UnsupportedOperationException("This format does not support attributes");
	}

	public CommentWriter getCommentWriter() {
		throw new UnsupportedOperationException("This format does not support comments");
	}

	public CDataWriter getCDataWriter() {
		throw new UnsupportedOperationException("This format does not support CDATA sections.");
	}
	
	public WritableMapping getMapping() {
		return mapping;
	}

}
