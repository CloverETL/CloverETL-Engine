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

import org.jetel.data.tree.bean.formatter.BeanTreeFormatterProvider;
import org.jetel.data.tree.bean.schema.generator.BeanParser;
import org.jetel.data.tree.bean.schema.model.SchemaObject;
import org.jetel.data.tree.bean.util.SchemaObjectReader;
import org.jetel.data.tree.bean.util.TypedObjectClearingVisitor;
import org.jetel.data.tree.formatter.BaseTreeFormatterProvider;
import org.jetel.data.tree.formatter.runtimemodel.WritableMapping;
import org.jetel.data.tree.xml.formatter.util.AbstractMappingValidator;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.compile.ClassLoaderUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

/**
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9.11.2011
 */
public class BeanWriter extends TreeWriter {
	
	public final static String COMPONENT_TYPE = "BEAN_WRITER";

	public static final String XML_CLASSPATH_ATTRIBUTE = "classpath";
	public static final String XML_SCHEMA_ATTRIBUTE = "schema";
	
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		BeanWriter writer = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		try {
			writer = new BeanWriter(xattribs.getString(XML_ID_ATTRIBUTE));
			readCommonAttributes(writer, xattribs);
			
			writer.setSchema(xattribs.getString(XML_SCHEMA_ATTRIBUTE));
			if (xattribs.exists(XML_CLASSPATH_ATTRIBUTE)) {
				writer.setClasspath(xattribs.getStringEx(XML_CLASSPATH_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));
			}
		} catch (AttributeNotFoundException ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}

		return writer;
	}
	
	private String schema;
	private String classpath;
	
	public BeanWriter(String id) {
		super(id);
	}
	
	@Override
	protected void configureWriter() throws ComponentNotReadyException {
		super.configureWriter();
		writer.setStoreRawData(false);
	}

	@Override
	protected AbstractMappingValidator createValidator(Map<Integer, DataRecordMetadata> connectedPorts) {
		return null;
	}

	@Override
	protected BaseTreeFormatterProvider createFormatterProvider(WritableMapping engineMapping, int maxPortIndex) throws ComponentNotReadyException {
		try {
			ClassLoader classloader = ClassLoaderUtils.createURLClassLoader(getGraph().getRuntimeContext().getContextURL(), classpath);
			
			SchemaObject structure = SchemaObjectReader.readFromString(schema);
			structure.acceptVisitor(new TypedObjectClearingVisitor());
			BeanParser.addReferencedTypes(structure, classloader);
			
			return new BeanTreeFormatterProvider(engineMapping, maxPortIndex, structure, classloader);
		} catch (SAXException e) {
			throw new ComponentNotReadyException("Failed to load schema", e);
		}
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	public void setSchema(String schema) {
		this.schema = schema;
	}

	public void setClasspath(String classpath) {
		this.classpath = classpath;
	}

}
