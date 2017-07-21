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

import org.jetel.data.tree.bean.formatter.MapTreeFormatterProvider;
import org.jetel.data.tree.formatter.BaseTreeFormatterProvider;
import org.jetel.data.tree.formatter.runtimemodel.WritableMapping;
import org.jetel.data.tree.xml.formatter.util.AbstractMappingValidator;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 * @author krejcil (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 1.2.2012
 */
public class MapWriter extends TreeWriter {
	
	public final static String COMPONENT_TYPE = "MAP_WRITER";
	
	public static MapWriter fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		MapWriter writer;
		try {
			writer = new MapWriter(xattribs.getString(XML_ID_ATTRIBUTE));
			readCommonAttributes(writer, xattribs);
		} catch (AttributeNotFoundException ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}

		return writer;
	}

	public MapWriter(String id) {
		super(id);
	}
	
	@Override
	protected void configureWriter() throws ComponentNotReadyException {
		super.configureWriter();
		writer.setStoreRawData(false);
	}

	@Override
	protected AbstractMappingValidator createValidator(Map<Integer, DataRecordMetadata> connectedPorts) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected BaseTreeFormatterProvider createFormatterProvider(WritableMapping engineMapping, int maxPortIndex)
			throws ComponentNotReadyException {
		return new MapTreeFormatterProvider(engineMapping, maxPortIndex);
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

}
