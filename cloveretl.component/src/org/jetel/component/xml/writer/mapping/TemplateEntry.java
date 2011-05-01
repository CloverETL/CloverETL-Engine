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
package org.jetel.component.xml.writer.mapping;

import javax.xml.stream.XMLStreamException;

import org.jetel.component.xml.writer.MappingVisitor;

/**
 * Class representing xml point where referenced template should be expanded.
 * 
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11 Jan 2011
 */
public class TemplateEntry extends AbstractElement {

	public static final String INVALID_TEMPLATE_ELEMENT = "Template entry element must be a child of standard element!";
	
	public static final MappingProperty[] AVAILABLE_PROPERTIES = {MappingProperty.TEMPLATE_NAME};
	
	public TemplateEntry(Element parent, boolean registerAsChild) {
		super(parent, registerAsChild);
	}

	public void setAttribute(String localName, String value) throws XMLStreamException {
		MappingProperty property = MappingProperty.fromString(localName);
		if (!setProperty(property, value)) {
			throw new XMLStreamException(XmlMapping.UNKNOWN_ATTRIBUTE + localName);
		}
	}
	
	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}

	@Override
	public String getSimpleContent() {
		return properties.get(MappingProperty.TEMPLATE_NAME);
	}

	@Override
	public String getDisplayName() {
		return "Insert template";
	}

	@Override
	public String getPath() {
		return parent.getPath() + AbstractElement.LEVEL_DELIMITER
			+ getDisplayName() + ": " + properties.get(MappingProperty.TEMPLATE_NAME);
	}

	@Override
	public MappingProperty[] getAvailableProperties() {
		return AVAILABLE_PROPERTIES;
	}

	@Override
	public short getType() {
		return AbstractElement.TEMPLATE_ENTRY;
	}
}
