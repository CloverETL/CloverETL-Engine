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

import org.jetel.component.xml.writer.MappingVisitor;
import org.jetel.component.xml.writer.MappingWriter;
import org.jetel.component.xml.writer.StaxPrettyPrintHandler;
import org.jetel.util.string.StringUtils;
import org.xml.sax.SAXException;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 8 Dec 2010
 */
public class XmlMapping {

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
	public static final String EMPTY_MODEL = "<root xmlns:" + MAPPING_KEYWORDS_PREFIX + "=\"" + MAPPING_KEYWORDS_NAMESPACEURI + "\">\n"
	                                       + "  <element0/>\n"
	                                       + "</root>";

	public static final String UNKNOWN_ATTRIBUTE = "Unknown property ";

	private Element rootElement = new Element(null); // dummy root element
	private String version;

	private Map<String, Element> templates = new HashMap<String, Element>();
	
	public void toXml(OutputStream stream) throws XMLStreamException {
		XMLOutputFactory factory = XMLOutputFactory.newInstance();
		try {
			XMLStreamWriter writer = factory.createXMLStreamWriter(stream, DEFAULT_ENCODING);

			StaxPrettyPrintHandler handler = new StaxPrettyPrintHandler(writer);
			writer = (XMLStreamWriter) Proxy.newProxyInstance(XMLStreamWriter.class.getClassLoader(),
					new Class[] { XMLStreamWriter.class }, handler);

			writer.writeStartDocument(DEFAULT_ENCODING, DEFAULT_VERSION);
			rootElement.accept(new MappingWriter(writer));
			writer.writeEndDocument();
			writer.close();
		} catch (Exception e) {
			throw new XMLStreamException(e);
		}
	}

	public void visit(MappingVisitor visitor) {
		try {
			updateTemplates();
			rootElement.accept(visitor);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static XmlMapping fromXml(InputStream stream) throws XMLStreamException {
		XmlMapping mapping = new XmlMapping();
		TemplateEntry templateEntryElement = null;
		WildcardElement aggregateElement = null;
		Element currentElement = mapping.getRootElement();

		XMLInputFactory factory = XMLInputFactory.newInstance();
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
					throw new XMLStreamException(WildcardElement.INVALID_AGGREGATE_ELEMENT, parser.getLocation());
				}
				MappingProperty keyword = MappingProperty.fromString(parser.getLocalName());
				if (keyword == MappingProperty.ELEMENTS && MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(parser.getNamespaceURI())) {
					if (currentElement.getParent() == null) {
						throw new XMLStreamException(WildcardElement.INVALID_AGGREGATE_ELEMENT, parser.getLocation());
					}
					aggregateElement = parseAggregateElement(parser, currentElement);
				} else if (keyword == MappingProperty.TEMPLATE_ENTRY && MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(parser.getNamespaceURI())) {
					if (currentElement.getParent() == null) {
						throw new XMLStreamException(TemplateEntry.INVALID_TEMPLATE_ELEMENT, parser.getLocation());
					}
					templateEntryElement = parseTemplateEntry(parser, currentElement);
				} else {
					currentElement = parseElement(parser, currentElement);
				}
				break;
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
	private static TemplateEntry parseTemplateEntry(XMLStreamReader parser, Element currentElement)
			throws XMLStreamException {
		TemplateEntry templateEntryElement = new TemplateEntry(currentElement, true);
		for (int i = 0; i < parser.getAttributeCount(); i++) {
			templateEntryElement.setAttribute(parser.getAttributeLocalName(i), parser.getAttributeValue(i));
		}
		return templateEntryElement;
	}

	/**
	 * @param attributes
	 * @throws SAXException
	 */
	private static WildcardElement parseAggregateElement(XMLStreamReader reader, Element currentElement)
			throws XMLStreamException {
		WildcardElement aggregateElement = new WildcardElement(currentElement, true);
		for (int i = 0; i < reader.getAttributeCount(); i++) {
			if (MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(reader.getAttributeNamespace(i))) {
				aggregateElement.setProperty(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
			}
		}
		return aggregateElement;
	}

	/**
	 * @param uri
	 * @param localName
	 * @param qName
	 * @param attributes
	 * @throws XMLStreamException
	 */
	private static Element parseElement(XMLStreamReader reader, Element previousElement)
			throws XMLStreamException {
		Element currentElement = new Element(previousElement);
		MappingProperty keyword = MappingProperty.fromString(reader.getLocalName()); 
		if (keyword == MappingProperty.TEMPLATE && MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(reader.getNamespaceURI())) {
			currentElement.setTemplate(true);
			for (int i = 0; i < reader.getAttributeCount(); i++) {
				keyword = MappingProperty.fromString(reader.getAttributeLocalName(i));
				if (keyword == MappingProperty.TEMPLATE_NAME) {
					currentElement.setProperty(MappingProperty.TEMPLATE_NAME, reader.getAttributeValue(i));
				}
			}
		} else {
			String name = reader.getPrefix();
			if (StringUtils.isEmpty(name)) {
				name = reader.getLocalName();
			} else {
				name += ":" +  reader.getLocalName();
			}
			currentElement.setProperty(MappingProperty.NAME, name);

			for (int i = 0; i < reader.getNamespaceCount(); i++) {
				currentElement.addNamespace(currentElement.getAttributes().size(),
						reader.getNamespacePrefix(i), reader.getNamespaceURI(i));

			}

			for (int i = 0; i < reader.getAttributeCount(); i++) {
				if (MAPPING_KEYWORDS_NAMESPACEURI.equalsIgnoreCase(reader.getAttributeNamespace(i))) {
					currentElement.setAttribute(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
				} else {
					name = reader.getAttributePrefix(i);
					if (StringUtils.isEmpty(name)) {
						name = reader.getAttributeLocalName(i);
					} else {
						name += ":" +  reader.getAttributeLocalName(i);
					}
					currentElement.addAttribute(currentElement.getAttributes().size(),
							name, reader.getAttributeValue(i));
				}
			}
		}
		return currentElement;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public Element getRootElement() {
		return rootElement;
	}

	public void setRootElement(Element rootElement) {
		this.rootElement = rootElement;
	}

	public Map<String, Element> getTemplates() {
		return Collections.unmodifiableMap(templates);
	}
	
	public void updateTemplates() {
		gatherTemplatesInElement(rootElement);
	}
	
	private void gatherTemplatesInElement(Element element) {
		for (AbstractElement child : element.getChildren()) {
			short type = child.getType();
			if (type == AbstractElement.TEMPLATE) {
				templates.put(child.getProperty(MappingProperty.TEMPLATE_NAME), (Element)child);
			}
			if (type == AbstractElement.TEMPLATE || type == AbstractElement.ELEMENT) {
				gatherTemplatesInElement((Element) child);
			}
		}
	}
}
