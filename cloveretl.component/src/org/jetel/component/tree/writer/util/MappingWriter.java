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
package org.jetel.component.tree.writer.util;

import java.util.LinkedList;
import java.util.List;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

import org.jetel.component.tree.writer.model.design.AbstractNode;
import org.jetel.component.tree.writer.model.design.Attribute;
import org.jetel.component.tree.writer.model.design.CDataSection;
import org.jetel.component.tree.writer.model.design.CollectionNode;
import org.jetel.component.tree.writer.model.design.Comment;
import org.jetel.component.tree.writer.model.design.MappingProperty;
import org.jetel.component.tree.writer.model.design.Namespace;
import org.jetel.component.tree.writer.model.design.ObjectNode;
import org.jetel.component.tree.writer.model.design.Relation;
import org.jetel.component.tree.writer.model.design.TemplateEntry;
import org.jetel.component.tree.writer.model.design.TreeWriterMapping;
import org.jetel.component.tree.writer.model.design.Value;
import org.jetel.component.tree.writer.model.design.WildcardAttribute;
import org.jetel.component.tree.writer.model.design.WildcardNode;
import org.jetel.util.string.StringUtils;
import org.jetel.util.string.TagName;

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
	public void visit(WildcardAttribute element) throws Exception {
		checkCloverPrefix();
		
		writePropertyAsCloverAttribute(element, MappingProperty.INCLUDE);
		writePropertyAsCloverAttribute(element, MappingProperty.EXCLUDE);
	}

	@Override
	public void visit(WildcardNode element) throws Exception {
		checkCloverPrefix();

		writer.writeEmptyElement(TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI, WildcardNode.XML_WILDCARD_NODE_DEFINITION);
		writePropertyAsCloverAttribute(element, MappingProperty.INCLUDE);
		writePropertyAsCloverAttribute(element, MappingProperty.EXCLUDE);
		writePropertyAsCloverAttribute(element, MappingProperty.WRITE_NULL_ELEMENT);
		writePropertyAsCloverAttribute(element, MappingProperty.OMIT_NULL_ELEMENT);
	}

	@Override
	public void visit(ObjectNode element) throws Exception {
		// don't write the dummy root element it self 
		if (element.getParent() == null && !element.getChildren().isEmpty()) {
			for (AbstractNode child : element.getChildren()) {
				child.accept(this);
			}
			return;
		}

		List<Attribute> plainAttributes = new LinkedList<Attribute>();
		List<Attribute> childAttributes = new LinkedList<Attribute>();
		for (Attribute attribute : element.getAttributes()) {
			if (attribute.isChild()) {
				childAttributes.add(attribute);
			} else {
				plainAttributes.add(attribute);
			}
		}
		
		if (element.isTemplate()) {
			checkCloverPrefix();
			writer.writeStartElement(TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI, ObjectNode.XML_TEMPLATE_DEFINITION);
			writePropertyAsCloverAttribute(element, MappingProperty.NAME);
		} else {
			String name = element.getProperty(MappingProperty.NAME);
			boolean writeNameAsAttribute = TagName.hasInvalidCharacters(name);
			
			String elementName = writeNameAsAttribute ? TreeWriterMapping.MAPPING_KEYWORDS_PREFIX + ":" + ObjectNode.XML_ELEMENT_WITH_NAME_ATTRIBUTE : name;
			
			if (element.getChildren().isEmpty() && childAttributes.isEmpty()) {
				writer.writeEmptyElement(elementName);
			} else {
				writer.writeStartElement(elementName);
			}
			
			if (writeNameAsAttribute) {
				writePropertyAsCloverAttribute(element, MappingProperty.NAME);
			}
		}
		
		//write namespaces
		for (Namespace namespace : element.getNamespaces()) {
			namespace.accept(this);
		}
		
		//write attributes
		for (Attribute attribute : plainAttributes) {
			attribute.accept(this);
		}
		
		//write recurring element attributes
		if (element.getRelation() != null) {
			element.getRelation().accept(this);
		}
		
		//write aggregate attributes
		checkCloverPrefix();
		writePropertyAsCloverAttribute(element, MappingProperty.DATA_TYPE);
		writePropertyAsCloverAttribute(element, MappingProperty.WRITE_NULL_ELEMENT);
		writePropertyAsCloverAttribute(element, MappingProperty.WRITE_NULL_ATTRIBUTE);
		writePropertyAsCloverAttribute(element, MappingProperty.OMIT_NULL_ATTRIBUTE);
		writePropertyAsCloverAttribute(element, MappingProperty.HIDE);
		writePropertyAsCloverAttribute(element, MappingProperty.PARTITION);
		if (element.getWildcardAttribute() != null) {
			element.getWildcardAttribute().accept(this);
		}

		//write children
		for (Attribute attribute : childAttributes) {
			attribute.accept(this);
		}
		for (AbstractNode child : element.getChildren()) {
			child.accept(this);
		}
		if (!element.getChildren().isEmpty() || !childAttributes.isEmpty()) {
			writer.writeEndElement();
		}

	}

	@Override
	public void visit(Value element) throws XMLStreamException {
		String toWrite = element.getProperty(MappingProperty.VALUE);
		if (toWrite != null) {
			writeText(toWrite);
		}
	}
	
	private void writeText(String text) throws XMLStreamException {
		// If CRLF is written, LFLF is subsequently read 
		text = text.replaceAll("\r\n", "\n"); // maybe StaxPrettyPrintHandler would be better place do this
		writer.writeCharacters(text);
	}

	@Override
	public void visit(Attribute element) throws XMLStreamException {
		String name = element.getProperty(MappingProperty.NAME);
		if (!StringUtils.isEmpty(name)) {
			if (element.isChild()) {
				writer.writeEmptyElement(TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI, Attribute.XML_ATTRIBUTE_DEFINITION);
				writePropertyAsCloverAttribute(element, MappingProperty.NAME);
				writePropertyAsCloverAttribute(element, MappingProperty.VALUE);
				writePropertyAsCloverAttribute(element, MappingProperty.INDEX);
			} else {
				String value = element.getProperty(MappingProperty.VALUE);
				writer.writeAttribute(name, value == null ? "" : value);
			}
		}
	}

	@Override
	public void visit(Namespace element) throws XMLStreamException {
		String name = element.getProperty(MappingProperty.NAME);
		String value = element.getProperty(MappingProperty.VALUE);
		writer.writeNamespace(name, value == null ? "" : value);
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
		writer.writeEmptyElement(TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI, TemplateEntry.XML_TEMPLATE_ENTRY_DEFINITION);
		writePropertyAsCloverAttribute(element, MappingProperty.NAME);
	}
	
	@Override
	public void visit(Comment objectComment) throws Exception {
		StringBuilder comment = new StringBuilder();
		if (Boolean.valueOf(objectComment.getProperty(MappingProperty.WRITE))) {
			comment.append(" ");
			comment.append(TreeWriterMapping.MAPPING_WRITE_COMMENT);
		}
		String value = objectComment.getProperty(MappingProperty.VALUE);
		if (value != null) {
			comment.append(" ");
			comment.append(value);
		}
		comment.append(" ");
		writer.writeComment(comment.toString());
	}
	
	@Override
	public void visit(CDataSection cdataSection) throws Exception {
		
		String value = cdataSection.getProperty(MappingProperty.VALUE);
		writer.writeCData(value != null ? value : "");
	}
	
	private void writePropertyAsCloverAttribute(AbstractNode element, MappingProperty property) throws XMLStreamException {
		String attribute = element.getProperty(property);
		if (attribute != null) {
			checkCloverPrefix();
			writer.writeAttribute(TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI, property.getName(), attribute);
		}
	}
	
	private void checkCloverPrefix() throws XMLStreamException {
		if (writer.getPrefix(TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI) == null) {
			writer.setPrefix(TreeWriterMapping.MAPPING_KEYWORDS_PREFIX, TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI);
		}
	}

	@Override
	public void visit(CollectionNode element) throws Exception {
		checkCloverPrefix();

		if (element.getChildren().isEmpty()) {
			writer.writeEmptyElement(TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI, CollectionNode.XML_COLLECTION_DEFINITION);
		} else {
			writer.writeStartElement(TreeWriterMapping.MAPPING_KEYWORDS_NAMESPACEURI, CollectionNode.XML_COLLECTION_DEFINITION);
		}
		writePropertyAsCloverAttribute(element, MappingProperty.NAME);
		
		//write recurring element attributes
		if (element.getRelation() != null) {
			element.getRelation().accept(this);
		}
		
		//write aggregate attributes
		writePropertyAsCloverAttribute(element, MappingProperty.WRITE_NULL_ELEMENT);
		writePropertyAsCloverAttribute(element, MappingProperty.PARTITION);
		
		//write namespaces
		for (Namespace namespace : element.getNamespaces()) {
			namespace.accept(this);
		}

		//write children
		if (!element.getChildren().isEmpty()) {
			for (AbstractNode child : element.getChildren()) {
				child.accept(this);
			}
			writer.writeEndElement();
		}
	}
}
