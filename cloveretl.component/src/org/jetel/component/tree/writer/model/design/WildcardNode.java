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
package org.jetel.component.tree.writer.model.design;

import javax.xml.stream.XMLStreamException;

import org.jetel.component.tree.writer.util.MappingVisitor;

/**
 * Class representing xml multiple elements or attributes.
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 14 Dec 2010
 */
public class WildcardNode extends AbstractNode {

	public static final String XML_WILDCARD_NODE_DEFINITION = "elements";
	
	public static final String DESCRIPTION = "A construct used for mapping multiple clover fields to ";
	public static final String INVALID_AGGREGATE_ELEMENT = "Wildcard element must be a child of standard element!";
	
	private static final MappingProperty[] AVAILABLE_PROPERTIES_ELEMENT = {
		MappingProperty.INCLUDE, 
		MappingProperty.EXCLUDE, 
		MappingProperty.WRITE_NULL_ELEMENT, 
		MappingProperty.OMIT_NULL_ELEMENT, 
		MappingProperty.RAW_VALUE
	};
	
	public WildcardNode(ContainerNode parent) {
		super(parent, true);
	}
	
	public void setProperty(String localName, String attributeValue) throws XMLStreamException {
		MappingProperty keyword = MappingProperty.fromString(localName);
		if (!setProperty(keyword, attributeValue)) {
			throw new XMLStreamException(TreeWriterMapping.UNKNOWN_ATTRIBUTE + localName);
		}
	}

	@Override
	public MappingProperty[] getAvailableProperties() {
		return AVAILABLE_PROPERTIES_ELEMENT;
	}
	
	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}

	@Override
	public String getSimpleContent() {
		StringBuilder sb = new StringBuilder();
		String property = getProperty(MappingProperty.INCLUDE);
		if (property != null) {
			sb.append("Include: '").append(property).append("'");
		}
		
		property = getProperty(MappingProperty.EXCLUDE);
		if (property != null) {
			sb.append(" Exclude: '").append(property).append("'");
		}
		
		return sb.toString();
	}

	@Override
	public String getDisplayName() {
		return "Wildcard element";
	}

	@Override
	public short getType() {
		return AbstractNode.AGGREGATE_ELEMENT;
	}

	@Override
	public String getDescription() {
		return DESCRIPTION + "elements";
	}
}
