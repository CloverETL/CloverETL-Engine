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
package org.jetel.component.tree.reader;

import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.spi.XmlReader;

import org.jetel.component.AbstractMultiMetadataProvider;
import org.jetel.component.ComponentMetadataProvider;
import org.jetel.component.TreeReader;
import org.jetel.graph.Node;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MetadataPropagationResolver;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Metadata provider for tree based components - descendants of {@link TreeReader}.
 * 
 * @see TreeReader
 * @see XmlReader
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 10. 10. 2013
 */
public class TreeReaderErrMetadataProvider extends AbstractMultiMetadataProvider implements ComponentMetadataProvider {

	private List<DataRecordMetadata> allMetadata = new ArrayList<DataRecordMetadata>();
	
	private Node component;
	
	public TreeReaderErrMetadataProvider() {
		DataRecordMetadata m1 = new DataRecordMetadata("TreeReader_ErrPortWithoutFile");
		m1.setRecordDelimiter("\n");
		m1.setFieldDelimiter(";");
		m1.addField(new DataFieldMetadata("port", DataFieldType.INTEGER, null));
		m1.addField(new DataFieldMetadata("recordNumber", DataFieldType.INTEGER, null));
		m1.addField(new DataFieldMetadata("fieldNumber", DataFieldType.INTEGER, null));
		m1.addField(new DataFieldMetadata("fieldName", DataFieldType.STRING, null));
		m1.addField(new DataFieldMetadata("value", DataFieldType.STRING, null));
		m1.addField(new DataFieldMetadata("message", DataFieldType.STRING, null));

		DataRecordMetadata m2 = new DataRecordMetadata("TreeReader_ErrPortWithFile");
		m2.setRecordDelimiter("\n");
		m2.setFieldDelimiter(";");
		m2.addField(new DataFieldMetadata("port", DataFieldType.INTEGER, null));
		m2.addField(new DataFieldMetadata("recordNumber", DataFieldType.INTEGER, null));
		m2.addField(new DataFieldMetadata("fieldNumber", DataFieldType.INTEGER, null));
		m2.addField(new DataFieldMetadata("fieldName", DataFieldType.STRING, null));
		m2.addField(new DataFieldMetadata("value", DataFieldType.STRING, null));
		m2.addField(new DataFieldMetadata("message", DataFieldType.STRING, null));
		m2.addField(new DataFieldMetadata("file", DataFieldType.STRING, null));
		
		allMetadata.add(m1);
		allMetadata.add(m2);
	}
	
	@Override
	public List<MVMetadata> getAllInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		return null;
	}
	
	@Override
	public List<MVMetadata> getAllOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		if (portIndex == component.getOutPorts().size() - 1) {
			List<MVMetadata> result = new ArrayList<>();
			for (DataRecordMetadata metadata : allMetadata) {
				result.add(metadataPropagationResolver.createMVMetadata(metadata));
			}
			return result;
		} else {
			return null;
		}
	}

	@Override
	public void setComponent(Node component) {
		this.component = component;
	}
	
}
