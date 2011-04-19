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

import org.jetel.component.xml.writer.Mapping;
import org.jetel.component.xml.writer.MappingVisitor;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 14 Dec 2010
 */
public class ObjectAggregate extends ObjectRepresentation {
	
	public static final String INVALID_AGGREGATE_ELEMENT = "Wildcard element must be a child of standard element!";
	
	public static final MappingProperty[] AVAILABLE_PROPERTIES_ATTRIBUTE = {MappingProperty.INCLUDE, 
		MappingProperty.EXCLUDE};
	
	public static final MappingProperty[] AVAILABLE_PROPERTIES_ELEMENT = {MappingProperty.INCLUDE, 
		MappingProperty.EXCLUDE, MappingProperty.WRITE_NULL_ELEMENT, MappingProperty.OMIT_NULL_ELEMENT};
	
	private final boolean isElement;

	public ObjectAggregate(ObjectElement parent, boolean isElement) {
		super(parent, isElement);
		this.isElement = isElement;
	}
	
	public void setProperty(String localName, String attributeValue) throws XMLStreamException {
		MappingProperty keyword = MappingProperty.fromString(localName);
		if (!setProperty(keyword, attributeValue)) {
			throw new XMLStreamException(Mapping.UNKNOWN_ATTRIBUTE + localName);
		}
	}

	@Override
	public MappingProperty[] getAvailableProperties() {
		return isElement ? AVAILABLE_PROPERTIES_ELEMENT : AVAILABLE_PROPERTIES_ATTRIBUTE;
	}
	
	@Override
	public void accept(MappingVisitor visitor) throws Exception {
		visitor.visit(this);
	}

	@Override
	public String getSimpleContent() {
		return null;
	}

	@Override
	public String getDisplayName() {
		return isElement ? "Wildcard element" : "Wildcard attribute";
	}
	
	public boolean isElement() {
		return isElement;
	}

	@Override
	public short getType() {
		return isElement ? ObjectRepresentation.AGGREGATE_ELEMENT : ObjectRepresentation.AGGREGATE_ATTRIBUTE;
	}
}
