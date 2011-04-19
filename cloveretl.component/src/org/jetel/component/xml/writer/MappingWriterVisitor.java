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
package org.jetel.component.xml.writer;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.jetel.component.xml.writer.mapping.MappingProperty;
import org.jetel.component.xml.writer.mapping.ObjectAggregate;
import org.jetel.component.xml.writer.mapping.ObjectAttribute;
import org.jetel.component.xml.writer.mapping.ObjectComment;
import org.jetel.component.xml.writer.mapping.ObjectElement;
import org.jetel.component.xml.writer.mapping.ObjectNamespace;
import org.jetel.component.xml.writer.mapping.ObjectRepresentation;
import org.jetel.component.xml.writer.mapping.ObjectTemplateEntry;
import org.jetel.component.xml.writer.mapping.ObjectValue;
import org.jetel.component.xml.writer.mapping.RecurringElementInfo;
import org.jetel.util.string.StringUtils;

/**
 * @author LKREJCI (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15 Dec 2010
 */
public class MappingWriterVisitor implements MappingVisitor {
	
	private XMLStreamWriter writer;
	
	public MappingWriterVisitor(XMLStreamWriter writer) {
		this.writer = writer;
	}

	@Override
	public void visit(ObjectAggregate element) throws Exception {
		checkCloverPrefix();
		
		if (element.isElement()) {
			writer.writeEmptyElement(Mapping.MAPPING_KEYWORDS_NAMESPACEURI, MappingProperty.ELEMENTS.getName());
		}
		writePropertyAsCloverAttribute(element, MappingProperty.INCLUDE);
		writePropertyAsCloverAttribute(element, MappingProperty.EXCLUDE);
		if (element.isElement()) {
			writePropertyAsCloverAttribute(element, MappingProperty.WRITE_NULL_ELEMENT);
			writePropertyAsCloverAttribute(element, MappingProperty.OMIT_NULL_ELEMENT);
		}
	}

	@Override
	public void visit(ObjectElement element) throws Exception {
		// don't write the dummy root element it self 
		if (element.getParent() == null && !element.getChildren().isEmpty()) {
			for (ObjectRepresentation child : element.getChildren()) {
				child.accept(this);
			}
			return;
		}

		// regular element
		
		if (element.isTemplate()) {
			checkCloverPrefix();
			writer.writeStartElement(Mapping.MAPPING_KEYWORDS_NAMESPACEURI, MappingProperty.TEMPLATE_DECLARATION.getName());
			writePropertyAsCloverAttribute(element, MappingProperty.TEMPLATE_NAME);
		} else {
			if (element.getChildren().isEmpty()) {
				writer.writeEmptyElement(element.getProperty(MappingProperty.NAME));
			} else {
				writer.writeStartElement(element.getProperty(MappingProperty.NAME));
			}
		}
		
		//write namespaces
		for (ObjectNamespace namespace : element.getNamespaces()) {
			namespace.accept(this);
		}
		
		//write attributes
		for (ObjectAttribute attribute : element.getAttributes()) {
			attribute.accept(this);
		}
		
		//write recurring element attributes
		if (element.getRecurringInfo() != null) {
			element.getRecurringInfo().accept(this);
		}
		
		//write aggregate attributes
		checkCloverPrefix();
		writePropertyAsCloverAttribute(element, MappingProperty.WRITE_NULL_ELEMENT);
		writePropertyAsCloverAttribute(element, MappingProperty.WRITE_NULL_ATTRIBUTE);
		writePropertyAsCloverAttribute(element, MappingProperty.OMIT_NULL_ATTRIBUTE);
		writePropertyAsCloverAttribute(element, MappingProperty.HIDE);
		writePropertyAsCloverAttribute(element, MappingProperty.PARTITION);
		if (element.getAttributeInfo() != null) {
			element.getAttributeInfo().accept(this);
		}

		//write children
		if (!element.getChildren().isEmpty()) {
			for (ObjectRepresentation child : element.getChildren()) {
				child.accept(this);
			}
			writer.writeEndElement();
		}

	}

	@Override
	public void visit(ObjectValue element) throws XMLStreamException {
		String toWrite = element.getProperty(MappingProperty.VALUE);
		if (toWrite != null) {
			writer.writeCharacters(toWrite);
		}
	}

	@Override
	public void visit(ObjectAttribute element) throws XMLStreamException {
		String name = element.getProperty(MappingProperty.NAME);
		if (!StringUtils.isEmpty(name)) {
			String value = element.getProperty(MappingProperty.VALUE);
			writer.writeAttribute(name, value == null ? "" : value);
		}
	}

	@Override
	public void visit(ObjectNamespace element) throws XMLStreamException {
		String name = element.getProperty(MappingProperty.NAME);
		if (!StringUtils.isEmpty(name)) {
			String value = element.getProperty(MappingProperty.VALUE);
			writer.writeNamespace(name, value == null ? "" : value);
		}
	}

	@Override
	public void visit(RecurringElementInfo element) throws XMLStreamException {
		checkCloverPrefix();
		for (MappingProperty property : element.getAvailableProperties()) {
			writePropertyAsCloverAttribute(element, property);
		}
	}

	@Override
	public void visit(ObjectTemplateEntry element) throws XMLStreamException {
		checkCloverPrefix();
		writer.writeEmptyElement(Mapping.MAPPING_KEYWORDS_NAMESPACEURI, MappingProperty.TEMPLATE_ENTRY.getName());
		writePropertyAsCloverAttribute(element, MappingProperty.TEMPLATE_NAME);
	}
	
	@Override
	public void visit(ObjectComment objectComment) throws Exception {
		StringBuilder comment = new StringBuilder();
		if (Boolean.valueOf(objectComment.getProperty(MappingProperty.INCLUDE))) {
			comment.append(" ");
			comment.append(Mapping.MAPPING_INCLUDE_COMMENT);
		}
		String value = objectComment.getProperty(MappingProperty.VALUE);
		if (value != null) {
			comment.append(" ");
			comment.append(value);
		}
		comment.append(" ");
		writer.writeComment(comment.toString());
	}
	
	private void writePropertyAsCloverAttribute(ObjectRepresentation element, MappingProperty property) throws XMLStreamException {
		String attribute = element.getProperty(property);
		if (attribute != null) {
			writer.writeAttribute(Mapping.MAPPING_KEYWORDS_NAMESPACEURI, property.getName(), attribute);
		}
	}
	
	private void checkCloverPrefix() throws XMLStreamException {
		if (writer.getPrefix(Mapping.MAPPING_KEYWORDS_NAMESPACEURI) == null) {
			writer.setPrefix(Mapping.MAPPING_KEYWORDS_PREFIX, Mapping.MAPPING_KEYWORDS_NAMESPACEURI);
		}
	}

}
