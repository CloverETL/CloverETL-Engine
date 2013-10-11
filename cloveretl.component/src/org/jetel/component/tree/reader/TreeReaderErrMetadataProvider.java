/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.component.tree.reader;

import java.util.ArrayList;
import java.util.List;

import javax.sql.rowset.spi.XmlReader;

import org.jetel.component.AbstractMultiMetadataProvider;
import org.jetel.component.ComponentMetadataProvider;
import org.jetel.component.TreeReader;
import org.jetel.graph.MetadataPropagationResolver;
import org.jetel.graph.Node;
import org.jetel.graph.modelview.MVMetadata;
import org.jetel.graph.modelview.impl.MVEngineMetadata;
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

	private List<MVMetadata> metadata = new ArrayList<MVMetadata>();
	
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
		
		metadata.add(new MVEngineMetadata(m1));
		metadata.add(new MVEngineMetadata(m2));
	}
	
	@Override
	public List<MVMetadata> getAllInputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		return null;
	}
	
	@Override
	public List<MVMetadata> getAllOutputMetadata(int portIndex, MetadataPropagationResolver metadataPropagationResolver) {
		if (portIndex == component.getOutPorts().size() - 1) {
			return metadata;
		} else {
			return null;
		}
	}

	@Override
	public void setComponent(Node component) {
		this.component = component;
	}
	
}
