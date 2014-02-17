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

import java.util.Map;

import org.jetel.component.tree.writer.BaseTreeFormatterProvider;
import org.jetel.component.tree.writer.model.runtime.WritableMapping;
import org.jetel.component.tree.writer.util.AbstractMappingValidator;
import org.jetel.component.tree.writer.xml.XmlFormatterProvider;
import org.jetel.component.tree.writer.xml.XmlMappingValidator;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 * @author lkrejci
 * @created Dec 03, 2010
 */
public class ExtXmlWriter extends TreeWriter {

	public final static String COMPONENT_TYPE = "EXT_XML_WRITER";

	public static final String XML_MK_DIRS_ATTRIBUTE = "makeDirs";
	public static final String XML_OMIT_NEW_LINES_ATTRIBUTE = "omitNewLines";
	public static final String XML_SCHEMA_URL_ATTRIBUTE = "xmlSchemaURL";

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ExtXmlWriter writer = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		writer = new ExtXmlWriter(xattribs.getString(XML_ID_ATTRIBUTE));
		readCommonAttributes(writer, xattribs);

		if (xattribs.exists(XML_MK_DIRS_ATTRIBUTE)) {
			writer.setMkDir(xattribs.getBoolean(XML_MK_DIRS_ATTRIBUTE));
		}
		writer.setOmitNewLines(xattribs.getBoolean(XML_OMIT_NEW_LINES_ATTRIBUTE, false));

		return writer;
	}

	private boolean mkDir;
	private boolean omitNewLines;

	public ExtXmlWriter(String id) {
		super(id);
	}
	
	@Override
	protected void configureWriter() throws ComponentNotReadyException {
		super.configureWriter();
		writer.setMkDir(mkDir);
	}

	@Override
	protected AbstractMappingValidator createValidator(Map<Integer, DataRecordMetadata> connectedPorts) {
		return new XmlMappingValidator(connectedPorts, recordsPerFile == 1 || recordsCount == 1);
	}

	public void setMkDir(boolean mkDir) {
		this.mkDir = mkDir;
	}

	public void setOmitNewLines(boolean omitNewLines) {
		this.omitNewLines = omitNewLines;
	}

	@Override
	protected BaseTreeFormatterProvider createFormatterProvider(WritableMapping engineMapping, int maxPortIndex) {
		return new XmlFormatterProvider(engineMapping, maxPortIndex, omitNewLines, charset, designMapping.getVersion());
	}
}
