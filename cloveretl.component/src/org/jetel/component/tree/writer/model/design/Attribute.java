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
import org.jetel.component.tree.writer.xml.XmlMappingValidator;
import org.jetel.util.string.StringUtils;


/**
 * Class representing xml attribute
 * 
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 14 Dec 2010
 */
public class Attribute extends AbstractNode {
	
	public static final String XML_ATTRIBUTE_DEFINITION = "attribute";
	
	public static final String INVALID_TEMPLATE_ELEMENT = "Attribute element must be a child of standard element!";
	
	private static final MappingProperty[] AVAILABLE_PROPERTIES = {MappingProperty.NAME, 
		MappingProperty.VALUE, MappingProperty.INDEX};
	
	public static final boolean WRITE_NULL_DEFAULT = false;

	public Attribute(ContainerNode parent) {
		super(parent, false);
	}

	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}
	
	public void setProperty(String localName, String attributeValue) throws XMLStreamException {
		MappingProperty keyword = MappingProperty.fromString(localName);
		if (!setProperty(keyword, attributeValue)) {
			throw new XMLStreamException(TreeWriterMapping.UNKNOWN_ATTRIBUTE + localName);
		}
	}

	@Override
	public String getProperty(MappingProperty property) {
		if (property == MappingProperty.INDEX) {
			return String.valueOf(parent.getAttributes().indexOf(this));
		}
		return super.getProperty(property);
	}
	
	@Override
	public MappingProperty[] getAvailableProperties() {
		return AVAILABLE_PROPERTIES;
	}

	@Override
	public String getSimpleContent() {
		return properties.get(MappingProperty.VALUE);
	}

	@Override
	public String getDisplayName() {
		return properties.get(MappingProperty.NAME);
	}

	@Override
	public String getPath() {
		return parent.getPath() + "[@" + getDisplayName() + "]";
	}

	@Override
	public short getType() {
		return AbstractNode.ATTRIBUTE;
	}

	@Override
	public String getDescription() {
		return "An XML attribute. Example: <element0 attribute=\"value\">";
	}
	
	public boolean isChild() {
		String name = getProperty(MappingProperty.NAME);
		return !StringUtils.isEmpty(name) && name.matches(XmlMappingValidator.QUALIFIED_FIELD_REFERENCE_PATTERN);
	}
	
}
