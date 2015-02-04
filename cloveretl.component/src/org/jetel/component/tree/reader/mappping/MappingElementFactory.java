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
package org.jetel.component.tree.reader.mappping;

import java.util.regex.Pattern;

import org.jetel.data.Defaults;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 * Unit to process given XML DOM into tree mapping structure.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 2.12.2011
 */
public class MappingElementFactory {

	private static final String CONTEXT_ELEMENT = "Context";
	private static final String MAPPING_ELEMENT = "Mapping";
	
	private static final String XPATH_ATTR = "xpath";
	private static final String NAMESPACE_BINDINGS_ATTR = "namespacePaths";
	
	private static final String CLOVER_FIELD_ARTTR = "cloverField";
	private static final String NODE_NAME_ATTR = "nodeName";
	private static final String INPUT_FIELD_ATTR = "inputField";
	private static final String TRIM_ATTR = "trim";
	
	private static final String OUTPUT_PORT_ATTR = "outPort";
	private static final String PARENT_KEY_ATTR = "parentKey";
	private static final String GENERATED_KEY_ATTR = "generatedKey";
	private static final String SEQUENCE_FIELD_ATTR ="sequenceField";
	private static final String SEQUENCE_ID_ATTR = "sequenceId";
	
	//  str1 =     "|'   str2    "|'
	private final static Pattern NAMESPACE_BINDING_PATTERN = Pattern.compile("(.+)[=]([\"]|['])(.+)([\"]|['])$");
	//                                                                     "|'   str2    "|'
	private final static Pattern NAMESPACE_DEFAULT_PATTERN = Pattern.compile("^([\"]|['])(.+)([\"]|['])$");
	
	public MappingContext readMapping(Document doc) throws MalformedMappingException {
		
		Element root = doc.getDocumentElement();
		if (CONTEXT_ELEMENT.equals(root.getTagName())) {
			return processContextElement(root);
		} else {
			throw new MalformedMappingException(CONTEXT_ELEMENT + " expected as root");
		} 
	}
	
	private MappingContext processContextElement(Element element)
		throws MalformedMappingException {
		
		MappingContext context = new MappingContext();
		
		if (element.hasAttribute(XPATH_ATTR)) {
			context.setXPath(element.getAttribute(XPATH_ATTR));
		} else {
			throw new MalformedMappingException(XPATH_ATTR + " is required");
		}
		if (element.hasAttribute(NAMESPACE_BINDINGS_ATTR)) {
			parseNamespacePaths(element.getAttribute(NAMESPACE_BINDINGS_ATTR), context);
		}
		if (element.hasAttribute(OUTPUT_PORT_ATTR)) {
			try {
				context.setOutputPort(Integer.valueOf(element.getAttribute(OUTPUT_PORT_ATTR)));
			} catch (NumberFormatException e) {
				throw new MalformedMappingException(OUTPUT_PORT_ATTR + " has invalid value", e);
			}
			if (element.hasAttribute(PARENT_KEY_ATTR)) {
				String keyString = element.getAttribute(PARENT_KEY_ATTR);
				String keys[] = keyString.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				context.setParentKeys(keys);
			}
			if (element.hasAttribute(GENERATED_KEY_ATTR)) {
				String keyString = element.getAttribute(GENERATED_KEY_ATTR);
				String keys[] = keyString.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				context.setGeneratedKeys(keys);
			}
			if ((context.getParentKeys() == null ^ context.getGeneratedKeys() == null) ||
					(context.getParentKeys() != null && context.getGeneratedKeys() != null &&
					context.getParentKeys().length != context.getGeneratedKeys().length)) {
				throw new MalformedMappingException("invalid configuration: "
						+ PARENT_KEY_ATTR + "='" + element.getAttribute(PARENT_KEY_ATTR)
						+ "', " + GENERATED_KEY_ATTR + "='" + element.getAttribute(GENERATED_KEY_ATTR) + "'");
			}
			if (element.hasAttribute(SEQUENCE_FIELD_ATTR)) {
				context.setSequenceField(element.getAttribute(SEQUENCE_FIELD_ATTR));
			}
			if (element.hasAttribute(SEQUENCE_ID_ATTR)) {
				context.setSequenceId(element.getAttribute(SEQUENCE_ID_ATTR));
			}
		}
		
		for (Node child = element.getFirstChild(); child != null; child = child.getNextSibling()) {
			if (child.getNodeType() == Node.ELEMENT_NODE) {
				Element childElement = (Element)child;
				if (CONTEXT_ELEMENT.equals(childElement.getTagName())) {
					context.addChild(processContextElement(childElement));
				} else if (MAPPING_ELEMENT.equals(childElement.getTagName())) {
					context.addChild(processMappingElement(childElement));
				} else {
					throw new MalformedMappingException("unknown element " + childElement.getTagName());
				}
			}
		}
		
		return context;
	}
	
	private FieldMapping processMappingElement(Element element)
		throws MalformedMappingException {
		
		FieldMapping field = new FieldMapping();
		if (!(element.hasAttribute(XPATH_ATTR) ^ element.hasAttribute(NODE_NAME_ATTR) ^ element.hasAttribute(INPUT_FIELD_ATTR))) {
			throw new MalformedMappingException("Exactly one of attributes " + XPATH_ATTR + ", " + NODE_NAME_ATTR + ", " + INPUT_FIELD_ATTR + " is required on element " + element.getNodeName());
		}
		if (element.hasAttribute(XPATH_ATTR)) {
			field.setXPath(element.getAttribute(XPATH_ATTR));
		}
		if (element.hasAttribute(NODE_NAME_ATTR)) {
			field.setNodeName(element.getAttribute(NODE_NAME_ATTR));
		}
		if (element.hasAttribute(INPUT_FIELD_ATTR)) {
			field.setInputField(element.getAttribute(INPUT_FIELD_ATTR));
		}
		if (!element.hasAttribute(CLOVER_FIELD_ARTTR)) {
			throw new MalformedMappingException(CLOVER_FIELD_ARTTR + " is required on element " + element.getNodeName());
		} else {
			field.setCloverField(element.getAttribute(CLOVER_FIELD_ARTTR));
		}
		if (element.hasAttribute(TRIM_ATTR)) {
			 field.setTrim(Boolean.parseBoolean(element.getAttribute(TRIM_ATTR)));
		}
		if (element.hasAttribute(NAMESPACE_BINDINGS_ATTR)) {
			parseNamespacePaths(element.getAttribute(NAMESPACE_BINDINGS_ATTR), field);
		}
		return field;
	}
	
	private void parseNamespacePaths(String attr, MappingElement element)
		throws MalformedMappingException {
		
		if (NAMESPACE_DEFAULT_PATTERN.matcher(attr).matches()) {
			element.addDefaultNamespace(attr.substring(1, attr.length() - 1));
		} else {
			for (String duplet : attr.split(";")) {
				if (NAMESPACE_BINDING_PATTERN.matcher(duplet).matches()) {
					int equalPos = duplet.indexOf('=');
					String prefix = duplet.substring(0, equalPos);
					String uri = duplet.substring(equalPos + 2, duplet.length() - 1);
					element.addNamespaceBinding(prefix, uri);
				} else {
					throw new MalformedMappingException("invalid namespace declaration: " + attr);
				}
			}
		}
	}
}
