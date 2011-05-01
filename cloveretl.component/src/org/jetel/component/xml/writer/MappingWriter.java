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
import org.jetel.component.xml.writer.mapping.WildcardElement;
import org.jetel.component.xml.writer.mapping.Attribute;
import org.jetel.component.xml.writer.mapping.Comment;
import org.jetel.component.xml.writer.mapping.Element;
import org.jetel.component.xml.writer.mapping.Namespace;
import org.jetel.component.xml.writer.mapping.AbstractElement;
import org.jetel.component.xml.writer.mapping.TemplateEntry;
import org.jetel.component.xml.writer.mapping.Value;
import org.jetel.component.xml.writer.mapping.Relation;
import org.jetel.component.xml.writer.mapping.XmlMapping;
import org.jetel.util.string.StringUtils;

/**
 * Visitor which serializes mapping into xml.
 * 
 * @author lkrejci (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 15 Dec 2010
 */
public class MappingWriter implements MappingVisitor {
	
	private XMLStreamWriter writer;
	
	public MappingWriter(XMLStreamWriter writer) {
		this.writer = writer;
	}

	@Override
	public void visit(WildcardElement element) throws Exception {
		checkCloverPrefix();
		
		if (element.isElement()) {
			writer.writeEmptyElement(XmlMapping.MAPPING_KEYWORDS_NAMESPACEURI, MappingProperty.ELEMENTS.getName());
		}
		writePropertyAsCloverAttribute(element, MappingProperty.INCLUDE);
		writePropertyAsCloverAttribute(element, MappingProperty.EXCLUDE);
		if (element.isElement()) {
			writePropertyAsCloverAttribute(element, MappingProperty.WRITE_NULL_ELEMENT);
			writePropertyAsCloverAttribute(element, MappingProperty.OMIT_NULL_ELEMENT);
		}
	}

	@Override
	public void visit(Element element) throws Exception {
		// don't write the dummy root element it self 
		if (element.getParent() == null && !element.getChildren().isEmpty()) {
			for (AbstractElement child : element.getChildren()) {
				child.accept(this);
			}
			return;
		}

		// regular element
		
		if (element.isTemplate()) {
			checkCloverPrefix();
			writer.writeStartElement(XmlMapping.MAPPING_KEYWORDS_NAMESPACEURI, MappingProperty.TEMPLATE.getName());
			writePropertyAsCloverAttribute(element, MappingProperty.TEMPLATE_NAME);
		} else {
			if (element.getChildren().isEmpty()) {
				writer.writeEmptyElement(element.getProperty(MappingProperty.NAME));
			} else {
				writer.writeStartElement(element.getProperty(MappingProperty.NAME));
			}
		}
		
		//write namespaces
		for (Namespace namespace : element.getNamespaces()) {
			namespace.accept(this);
		}
		
		//write attributes
		for (Attribute attribute : element.getAttributes()) {
			attribute.accept(this);
		}
		
		//write recurring element attributes
		if (element.getRelation() != null) {
			element.getRelation().accept(this);
		}
		
		//write aggregate attributes
		checkCloverPrefix();
		writePropertyAsCloverAttribute(element, MappingProperty.WRITE_NULL_ELEMENT);
		writePropertyAsCloverAttribute(element, MappingProperty.WRITE_NULL_ATTRIBUTE);
		writePropertyAsCloverAttribute(element, MappingProperty.OMIT_NULL_ATTRIBUTE);
		writePropertyAsCloverAttribute(element, MappingProperty.HIDE);
		writePropertyAsCloverAttribute(element, MappingProperty.PARTITION);
		if (element.getWildcardAttribute() != null) {
			element.getWildcardAttribute().accept(this);
		}

		//write children
		if (!element.getChildren().isEmpty()) {
			for (AbstractElement child : element.getChildren()) {
				child.accept(this);
			}
			writer.writeEndElement();
		}

	}

	@Override
	public void visit(Value element) throws XMLStreamException {
		String toWrite = element.getProperty(MappingProperty.VALUE);
		if (toWrite != null) {
			writer.writeCharacters(toWrite);
		}
	}

	@Override
	public void visit(Attribute element) throws XMLStreamException {
		String name = element.getProperty(MappingProperty.NAME);
		if (!StringUtils.isEmpty(name)) {
			String value = element.getProperty(MappingProperty.VALUE);
			writer.writeAttribute(name, value == null ? "" : value);
		}
	}

	@Override
	public void visit(Namespace element) throws XMLStreamException {
		String name = element.getProperty(MappingProperty.NAME);
		if (!StringUtils.isEmpty(name)) {
			String value = element.getProperty(MappingProperty.VALUE);
			writer.writeNamespace(name, value == null ? "" : value);
		}
	}

	@Override
	public void visit(Relation element) throws XMLStreamException {
		checkCloverPrefix();
		for (MappingProperty property : element.getAvailableProperties()) {
			writePropertyAsCloverAttribute(element, property);
		}
	}

	@Override
	public void visit(TemplateEntry element) throws XMLStreamException {
		checkCloverPrefix();
		writer.writeEmptyElement(XmlMapping.MAPPING_KEYWORDS_NAMESPACEURI, MappingProperty.TEMPLATE_ENTRY.getName());
		writePropertyAsCloverAttribute(element, MappingProperty.TEMPLATE_NAME);
	}
	
	@Override
	public void visit(Comment objectComment) throws Exception {
		StringBuilder comment = new StringBuilder();
		if (Boolean.valueOf(objectComment.getProperty(MappingProperty.INCLUDE))) {
			comment.append(" ");
			comment.append(XmlMapping.MAPPING_INCLUDE_COMMENT);
		}
		String value = objectComment.getProperty(MappingProperty.VALUE);
		if (value != null) {
			comment.append(" ");
			comment.append(value);
		}
		comment.append(" ");
		writer.writeComment(comment.toString());
	}
	
	private void writePropertyAsCloverAttribute(AbstractElement element, MappingProperty property) throws XMLStreamException {
		String attribute = element.getProperty(property);
		if (attribute != null) {
			writer.writeAttribute(XmlMapping.MAPPING_KEYWORDS_NAMESPACEURI, property.getName(), attribute);
		}
	}
	
	private void checkCloverPrefix() throws XMLStreamException {
		if (writer.getPrefix(XmlMapping.MAPPING_KEYWORDS_NAMESPACEURI) == null) {
			writer.setPrefix(XmlMapping.MAPPING_KEYWORDS_PREFIX, XmlMapping.MAPPING_KEYWORDS_NAMESPACEURI);
		}
	}

}
