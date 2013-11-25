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

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.jetel.component.tree.writer.util.MappingVisitor;
import org.jetel.component.tree.writer.util.MappingWriter;
import org.jetel.component.tree.writer.util.StaxPrettyPrintHandler;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.util.string.StringUtils;
import org.jetel.util.string.TagName;
import org.xml.sax.SAXException;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 8 Dec 2010
 */
public class TreeWriterMapping {

	private static final String INPORT_REFERENCE_PATTERN = "(" + StringUtils.OBJECT_NAME_PATTERN + "|[0-9]+)";
	private static final String QUALIFIED_FIELD_REFERENCE_PATTERN = "(?<!\\$)\\$" + INPORT_REFERENCE_PATTERN + "\\." + StringUtils.OBJECT_NAME_PATTERN;
	private static final String REFERENCE = QUALIFIED_FIELD_REFERENCE_PATTERN + "|\\{" + QUALIFIED_FIELD_REFERENCE_PATTERN + "\\}";
	public static final Pattern DATA_REFERENCE = Pattern.compile(REFERENCE);

	public static final String ESCAPED_PORT_REGEX = "\\$\\$";
	public static final String PORT_IDENTIFIER = Matcher.quoteReplacement("$");

	public static final String MAPPING_KEYWORDS_NAMESPACEURI = "http://www.cloveretl.com/ns/xmlmapping";
	public static final String MAPPING_KEYWORDS_PREFIX = "clover";
	public static final String MAPPING_WRITE_COMMENT = MAPPING_KEYWORDS_PREFIX + ":write";
	public static final String DELIMITER = ";";
	public static final char WILDCARD = '*';

	public static final String DEFAULT_VERSION = "1.0";
	public static final String DEFAULT_ENCODING = "UTF-8";
	public static final String EMPTY_MODEL = "<root xmlns:" + MAPPING_KEYWORDS_PREFIX + "=\"" + MAPPING_KEYWORDS_NAMESPACEURI + "\">\n" + "  <element0/>\n" + "</root>";

	public static final String UNKNOWN_ATTRIBUTE = "Unknown property ";

	private ContainerNode rootNode = new ObjectNode(null); // dummy root element
	private String version;

	private Map<String, ObjectNode> templates = new HashMap<String, ObjectNode>();

	public void toXml(OutputStream stream) throws XMLStreamException {
		XMLOutputFactory factory = XMLOutputFactory.newInstance();
		try {
			XMLStreamWriter writer = factory.createXMLStreamWriter(stream, DEFAULT_ENCODING);

			StaxPrettyPrintHandler handler = new StaxPrettyPrintHandler(writer);
			writer = (XMLStreamWriter) Proxy.newProxyInstance(XMLStreamWriter.class.getClassLoader(), new Class[] { XMLStreamWriter.class }, handler);

			writer.writeStartDocument(DEFAULT_ENCODING, DEFAULT_VERSION);
			rootNode.accept(new MappingWriter(writer));
			writer.writeEndDocument();
			writer.close();
		} catch (Exception e) {
			throw new XMLStreamException(e);
		}
	}

	public void visit(MappingVisitor visitor) {
		try {
			updateTemplates();
			rootNode.accept(visitor);
		} catch (Exception e) {
			throw new JetelRuntimeException(e);
		}
	}

	public static TreeWriterMapping fromXml(InputStream stream) throws XMLStreamException {
		TreeWriterMapping mapping = new TreeWriterMapping();
		TemplateEntry templateEntryElement = null;
		WildcardNode aggregateElement = null;
		Attribute attributeElement = null;
		ContainerNode currentElement = mapping.getRootElement();

		XMLInputFactory factory = XMLInputFactory.newInstance();
		try {
			factory.setProperty("report-cdata-event", Boolean.TRUE);
		} catch (IllegalArgumentException e) {
			factory.setProperty("http://java.sun.com/xml/stream/properties/report-cdata-event", Boolean.TRUE);
		}
		
		XMLStreamReader parser = factory.createXMLStreamReader(stream);
		String documentVersion = parser.getVersion();
		if (documentVersion == null) {
			documentVersion = DEFAULT_VERSION;
		}
		mapping.setVersion(documentVersion);

		while (parser.hasNext()) {
			parser.next();
			switch (parser.getEventType()) {
			case XMLStreamConstants.START_ELEMENT:
				if (aggregateElement != null) {
					throw new XMLStreamException(WildcardNode.INVALID_AGGREGATE_ELEMENT, parser.getLocation());
				}
				String keyword = parser.getLocalName();
				if (keyword.equals(WildcardNode.XML_WILDCARD_NODE_DEFINITION) && MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(parser.getNamespaceURI())) {
					if (currentElement.getParent() == null) {
						throw new XMLStreamException(WildcardNode.INVALID_AGGREGATE_ELEMENT, parser.getLocation());
					}
					aggregateElement = parseAggregateElement(parser, currentElement);
				} else if (keyword.equals(TemplateEntry.XML_TEMPLATE_ENTRY_DEFINITION) && MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(parser.getNamespaceURI())) {
					if (currentElement.getParent() == null) {
						throw new XMLStreamException(TemplateEntry.INVALID_TEMPLATE_ELEMENT, parser.getLocation());
					}
					templateEntryElement = parseTemplateEntry(parser, currentElement);
				} else if (keyword.equals(Attribute.XML_ATTRIBUTE_DEFINITION) && MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(parser.getNamespaceURI())) {
					if (currentElement.getParent() == null) {
						throw new XMLStreamException(Attribute.INVALID_TEMPLATE_ELEMENT, parser.getLocation());
					}
					attributeElement = parseAttributeEntry(parser, currentElement);
				} else {
					currentElement = parseContainer(parser, currentElement);
				}
				break;
			case XMLStreamConstants.CDATA: {
				parseCDataSection(parser, currentElement);
				break;
			}
			case XMLStreamConstants.CHARACTERS:
				if (!parser.isWhiteSpace()) {
					Value value = new Value(currentElement);
					value.setProperty(MappingProperty.VALUE, parser.getText());
				}
				break;
			case XMLStreamConstants.END_ELEMENT:
				if (aggregateElement != null) {
					aggregateElement = null;
				} else if (templateEntryElement != null) {
					templateEntryElement = null;
				} else if (attributeElement != null) {
					attributeElement = null;
				} else if (currentElement != null) {
					currentElement = currentElement.getParent();
				}
				break;
			case XMLStreamConstants.COMMENT:
				Comment comment = new Comment(currentElement);
				String commentText = parser.getText().trim();
				if (commentText.startsWith(MAPPING_WRITE_COMMENT)) {
					comment.setProperty(MappingProperty.WRITE, "true");
					commentText = commentText.substring(MAPPING_WRITE_COMMENT.length()).trim();
				} else {
					comment.setProperty(MappingProperty.WRITE, "false");
				}
				comment.setProperty(MappingProperty.VALUE, commentText);
				break;
			}
		}

		return mapping;
	}

	/**
	 * @param parser
	 * @throws XMLStreamException
	 */
	private static TemplateEntry parseTemplateEntry(XMLStreamReader parser, ContainerNode currentElement)
			throws XMLStreamException {
		TemplateEntry templateEntryElement = new TemplateEntry(currentElement);
		for (int i = 0; i < parser.getAttributeCount(); i++) {
			templateEntryElement.setAttribute(parser.getAttributeLocalName(i), parser.getAttributeValue(i));
		}
		return templateEntryElement;
	}

	/**
	 * @param attributes
	 * @throws SAXException
	 */
	private static WildcardNode parseAggregateElement(XMLStreamReader reader, ContainerNode currentElement)
			throws XMLStreamException {
		WildcardNode aggregateElement = new WildcardNode(currentElement);
		for (int i = 0; i < reader.getAttributeCount(); i++) {
			if (MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(reader.getAttributeNamespace(i))) {
				aggregateElement.setProperty(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
			}
		}
		return aggregateElement;
	}
	
	private static void parseCDataSection(XMLStreamReader reader, ContainerNode currentElement) throws XMLStreamException {
		
		CDataSection cdataSection = new CDataSection(currentElement);
		cdataSection.setProperty(MappingProperty.VALUE, reader.getText());
	}

	private static Attribute parseAttributeEntry(XMLStreamReader parser, ContainerNode currentElement)
			throws XMLStreamException {
		Attribute attribute = new Attribute(currentElement);
		for (int i = 0; i < parser.getAttributeCount(); i++) {
			if (MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(parser.getAttributeNamespace(i))) {
				attribute.setProperty(parser.getAttributeLocalName(i), parser.getAttributeValue(i));
			}
		}
		
		Integer index = null;
		try {
			index = Integer.valueOf(attribute.getProperty(MappingProperty.INDEX));
		} catch (NumberFormatException e) {
			// ignore
		}
		int attributesCount = currentElement.getAttributes().size();
		currentElement.addAttribute(index != null && index > -1 && index <= attributesCount ? index : attributesCount, attribute);
		  // TODO position and index of <attribute> elements can be inconsistent
		return attribute;
	}
	
	/**
	 * @param uri
	 * @param localName
	 * @param qName
	 * @param attributes
	 * @throws XMLStreamException
	 */
	private static ContainerNode parseContainer(XMLStreamReader reader, ContainerNode previousElement)
			throws XMLStreamException {
		String keyword = reader.getLocalName();
		if (keyword.equals(ObjectNode.XML_TEMPLATE_DEFINITION) && MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(reader.getNamespaceURI())) {
			ObjectNode currentContainer = new ObjectNode(previousElement);
			currentContainer.setTemplate(true);
			for (int i = 0; i < reader.getAttributeCount(); i++) {
				MappingProperty property = MappingProperty.fromString(reader.getAttributeLocalName(i));
				if (property == MappingProperty.NAME) {
					currentContainer.setProperty(MappingProperty.NAME, reader.getAttributeValue(i));
				}
			}
			return currentContainer;
		} else if (keyword.equals(CollectionNode.XML_COLLECTION_DEFINITION) && MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(reader.getNamespaceURI())) {
			CollectionNode currentContainer = new CollectionNode(previousElement);
			
			for (int i = 0; i < reader.getNamespaceCount(); i++) {
				currentContainer.addNamespace(i, reader.getNamespacePrefix(i), reader.getNamespaceURI(i));
			}
			
			for (int i = 0; i < reader.getAttributeCount(); i++) {
				if (MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(reader.getAttributeNamespace(i))) {
					currentContainer.setAttribute(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
				}
			}
			
			return currentContainer;
		} else {
			ObjectNode currentContainer = new ObjectNode(previousElement);
			String name = reader.getPrefix();
			if (StringUtils.isEmpty(name)) {
				name = TagName.decode(reader.getLocalName());
			} else {
				name += ":" + TagName.decode(reader.getLocalName());
			}
			currentContainer.setProperty(MappingProperty.NAME, name);

			for (int i = 0; i < reader.getNamespaceCount(); i++) {
				currentContainer.addNamespace(i, reader.getNamespacePrefix(i), reader.getNamespaceURI(i));
			}

			for (int i = 0; i < reader.getAttributeCount(); i++) {
				if (MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(reader.getAttributeNamespace(i))) {
					currentContainer.setAttribute(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
				} else {
					name = reader.getAttributePrefix(i);
					if (StringUtils.isEmpty(name)) {
						name = reader.getAttributeLocalName(i);
					} else {
						name += ":" + reader.getAttributeLocalName(i);
					}
					currentContainer.addAttribute(currentContainer.getAttributes().size(), name, reader.getAttributeValue(i));
				}
			}
			return currentContainer;
		}
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public ContainerNode getRootElement() {
		return rootNode;
	}

	public void setRootElement(ObjectNode rootElement) {
		this.rootNode = rootElement;
	}

	public Map<String, ObjectNode> getTemplates() {
		return Collections.unmodifiableMap(templates);
	}

	public void updateTemplates() {
		gatherTemplatesInElement(rootNode);
	}

	private void gatherTemplatesInElement(ContainerNode element) {
		for (AbstractNode child : element.getChildren()) {
			short type = child.getType();
			if (type == AbstractNode.TEMPLATE) {
				templates.put(child.getProperty(MappingProperty.NAME), (ObjectNode) child);
			}
			if (type == AbstractNode.TEMPLATE || type == AbstractNode.ELEMENT) {
				gatherTemplatesInElement((ObjectNode) child);
			}
		}
	}
}
