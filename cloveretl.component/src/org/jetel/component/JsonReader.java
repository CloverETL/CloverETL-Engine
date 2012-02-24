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

import org.jetel.component.tree.JsonReaderParserProvider;
import org.jetel.component.tree.TreeReaderParserProvider;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 19 Jan 2012
 */
public class JsonReader extends TreeReader {
	
	public final static String COMPONENT_TYPE = "JSON_READER";

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		try {
			JsonReader reader = new JsonReader(xattribs.getString(XML_ID_ATTRIBUTE));
			readCommonAttributes(reader, xattribs);
			
			return reader;
		} catch (AttributeNotFoundException ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}
	}
	
	public JsonReader(String id) {
		super(id);
	}

	@Override
	protected TreeReaderParserProvider getTreeReaderParserProvider() {
		return new JsonReaderParserProvider();
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

}
