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
package org.jetel.data.xml.mapping.parser;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.data.xml.mapping.InputFieldMappingDefinition;
import org.jetel.data.xml.mapping.TypeOverrideDefinition;
import org.jetel.data.xml.mapping.XMLElementMappingDefinition;
import org.jetel.data.xml.mapping.XMLMappingContext;
import org.jetel.data.xml.mapping.XMLMappingDefinition;
import org.jetel.data.xml.mapping.XMLMappingsDefinition;
import org.jetel.util.XmlUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


/** Class responsible for reading an XML mapping definition.
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.6.2012
 */
public class XMLMappingDefinitionParser {
	private static final Log logger = LogFactory.getLog(XMLMappingDefinitionParser.class);
	
	private static final String XML_INPUTFIELDMAPPING = "FieldMapping";
	private static final String XML_INPUTFIELDMAPPING_INPUTFIELD = "inputField";
	private static final String XML_INPUTFIELDMAPPING_OUTPUTFIELD = "outputField";

	private static final String XML_TYPE_OVERRIDE = "TypeOverride";
	private static final String XML_TYPE_OVERRIDE_ELEMENT_PATH = "elementPath";
	private static final String XML_TYPE_OVERRIDE_OVERRIDING_TYPE = "overridingType";

	private static final String XML_ROOT_ELEMENT = "Mappings";
	
	private static final String XML_MAPPING = "Mapping";
	private static final String XML_MAPPING_PARENT_RECORD = "useParentRecord";
	private static final String XML_MAPPING_IMPLICIT = "implicit";
	
	
	private static final String XML_ELEMENT = "element";
	private static final String XML_OUTPORT = "outPort";
	private static final String XML_PARENTKEY = "parentKey";
	private static final String XML_GENERATEDKEY = "generatedKey";
	private static final String XML_XMLFIELDS = "xmlFields";
	private static final String XML_CLOVERFIELDS = "cloverFields";
	private static final String XML_SEQUENCEFIELD = "sequenceField";
	private static final String XML_SEQUENCEID = "sequenceId";
    private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRows";
    private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";

    private static final String XML_TEMPLATE_ID = "templateId";
    private static final String XML_TEMPLATE_REF = "templateRef";
    private static final String XML_TEMPLATE_DEPTH = "nestedDepth";
	
    private static final Map<Class<? extends XMLMappingDefinition>, String> MAPPING_CLASS_TO_NAME = new HashMap<Class<? extends XMLMappingDefinition>, String>();
    static {
    	MAPPING_CLASS_TO_NAME.put(XMLElementMappingDefinition.class, XML_MAPPING);
    	MAPPING_CLASS_TO_NAME.put(InputFieldMappingDefinition.class, XML_INPUTFIELDMAPPING);
    }
    
    
	// === Mapping context ===
	
    /**
     * Context of the mapping.
     */
	private XMLMappingContext context;
	
	
	// === State data ===
	
	/**
	 *  Templates declared and referenced by the mapping elements.
	 */
    protected TreeMap<String, XMLMappingDefinition> declaredTemplates = new TreeMap<String, XMLMappingDefinition>();

    
	public XMLMappingDefinitionParser(XMLMappingContext context) {
		this.context = context;		
	}

	/** 
	 * Initializes the parser.
	 */
	public void init() {
		declaredTemplates.clear();
	}
	
	/** Class representing a result of the parsing.
	 * 
	 * @author Tomas Laurincik (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 22.6.2012
	 */
	public static class XMLMappingParseResult<T extends XMLMappingDefinition> {
		private T mapping;
		private List<String> errors = new LinkedList<String>();

		public T getMapping() {
			return mapping;
		}
		
		public void setMapping(T mapping) {
			this.mapping = mapping;
		}
		
		public List<String> getErrors() {
			return errors;
		}
		
		public void setErrors(List<String> errors) {
			this.errors = errors;
		}
		
		public boolean isSuccessful() {
			return errors.size() == 0 && mapping != null;
		}
	}
	
	/** Parses the given XML node, assuming the node represents an {@link InputFieldMappingDefinition}.
	 * 
	 * @param nodeXML
	 * @return parsed mapping
	 */
	public XMLMappingParseResult<InputFieldMappingDefinition> parseInputFieldMapping(org.w3c.dom.Node nodeXML) throws XMLMappingDefinitionParseException {
		return parseInputFieldMapping(nodeXML, null);
	}
	
	/** Creates an instance of the component XML attributes based on the given node.
	 * 
	 * @param nodeXML
	 * @return an instance of the component XML attributes
	 */
	private ComponentXMLAttributes createComponentAttributes(org.w3c.dom.Node nodeXML) {
		if (context.getGraph() != null) {
			return new ComponentXMLAttributes((Element) nodeXML, context.getGraph());
		}
		
		return new ComponentXMLAttributes((Element) nodeXML);
	}
	
	/** Parses the given XML node, assuming the node represents an {@link InputFieldMappingDefinition}.
	 * 
	 * @param nodeXML
	 * @return parsed mapping
	 */
	public XMLMappingParseResult<InputFieldMappingDefinition> parseInputFieldMapping(org.w3c.dom.Node nodeXML, XMLMappingDefinition parent) throws XMLMappingDefinitionParseException {
		XMLMappingParseResult<InputFieldMappingDefinition> result = new XMLMappingParseResult<InputFieldMappingDefinition>();

		InputFieldMappingDefinition mapping = new InputFieldMappingDefinition();
		
		ComponentXMLAttributes attributes = createComponentAttributes(nodeXML); 
		if (!checkAttributesPresent(attributes, result.getErrors(), XML_INPUTFIELDMAPPING_INPUTFIELD, XML_INPUTFIELDMAPPING_OUTPUTFIELD)) {
			return result;
		}
		
        String inputField = attributes.getString(XML_INPUTFIELDMAPPING_INPUTFIELD, null);
        mapping.setInputField(inputField);
        
        String outputField = attributes.getString(XML_INPUTFIELDMAPPING_OUTPUTFIELD, null);
        mapping.setOutputField(outputField);
       
        result.setMapping(mapping);
        
		return result;
	}

	/** Creates a mapping definition from template.
	 * 
	 * @return a mapping definition created from template.
	 */
	protected <T extends XMLMappingDefinition> T createFromTemplate(String templateID, Class<T> desiredClass) throws XMLMappingDefinitionParseException {
		// check if the template exists
		if (!declaredTemplates.containsKey(templateID)) {
			throw new XMLMappingDefinitionParseException("Template '" + templateID + "' has not been declared.");
		}
		
		// check if the template defines the same mapping class
		if (desiredClass != null && !desiredClass.isInstance(declaredTemplates.get(templateID))) {
			String templateType = MAPPING_CLASS_TO_NAME.get(declaredTemplates.get(templateID).getClass());
			if (templateType == null) {
				templateType = "<unknown>";
			}
			
			String desiredType = MAPPING_CLASS_TO_NAME.get(desiredClass);
			if (desiredType == null) {
				desiredType = "<unknown>";
			}
			
			throw new XMLMappingDefinitionParseException("Template '" + templateID + "' of type '" + templateType + "' is not compatible with type '" + desiredType + "'.");
		}
		
		@SuppressWarnings("unchecked")
		T mapping = (T)declaredTemplates.get(templateID).createCopy();
		
		return mapping;
	}
	
	/** Parses the given XML node, assuming the node represents an {@link XMLElementMappingDefinition}.
	 * 
	 * @param nodeXML
	 * @param parentMapping
	 * @return parsed mapping
	 */
	public XMLMappingParseResult<XMLElementMappingDefinition> parseElementMapping(org.w3c.dom.Node nodeXML, XMLMappingDefinition parent) throws XMLMappingDefinitionParseException {
		XMLMappingParseResult<XMLElementMappingDefinition> result = new XMLMappingParseResult<XMLElementMappingDefinition>();
		
		ComponentXMLAttributes attributes = createComponentAttributes((Element) nodeXML);
		
		
		XMLElementMappingDefinition mapping = null;
		boolean fromTemplate = false;
		
		// check if the mapping should be created from template
		if (attributes.exists(XML_TEMPLATE_REF)) {
			String templateId = attributes.getString(XML_TEMPLATE_REF, null);

			mapping = createFromTemplate(templateId, XMLElementMappingDefinition.class);
			fromTemplate = true;
		} 
		
		// if the mapping has not been created by other means, create a new one
		if (mapping == null) {
			mapping = new XMLElementMappingDefinition();
		}
		
		mapping.setParent(parent);

    	// standard mapping declaration
    	if (attributes.exists(XML_OUTPORT)) {
    		mapping.setOutputPortSource(attributes.getString(XML_OUTPORT, null));
    		int outputPort = attributes.getInteger(XML_OUTPORT, -1);
    		if (outputPort != -1) {
    			mapping.setOutputPort(outputPort);
    		}
    		
    	} 

		if (attributes.exists(XML_ELEMENT)) {
			mapping.setElementNameSource(attributes.getString(XML_ELEMENT, null));
			mapping.setElementName(XmlUtils.createQualifiedName(attributes.getString(XML_ELEMENT, null), context.getNamespaceBindings()));
		} else if (!fromTemplate) {
        	result.getErrors().add("Required attribute 'element' missing. Skipping this mapping and all children.");
    	}

		boolean parentKeyPresent = false;
		boolean generatedKeyPresent = false;
		if (attributes.exists(XML_PARENTKEY)) {
			mapping.setParentKeySource(attributes.getString(XML_PARENTKEY, null));
			final String[] parentKey = attributes.getString(XML_PARENTKEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			mapping.setParentKey(parentKey);
			parentKeyPresent = true;
		}

		if (attributes.exists(XML_GENERATEDKEY)) {
			mapping.setGeneratedKeySource(attributes.getString(XML_GENERATEDKEY, null));
			final String[] generatedKey = attributes.getString(XML_GENERATEDKEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			mapping.setGeneratedKey(generatedKey);
			generatedKeyPresent = true;
		}

		if (parentKeyPresent != generatedKeyPresent) {
			result.getErrors().add("Mapping for element: " + mapping.getElementName() + " must either have both 'parentKey' and 'generatedKey' attributes or neither.");
			mapping.setParentKey(null);
			mapping.setGeneratedKey(null);
		}

		if (parentKeyPresent && mapping.getParent() == null) {
			result.getErrors().add("Mapping for element: " + mapping.getElementName() + " may only have 'parentKey' or 'generatedKey' attributes if it is a nested mapping.");
			mapping.setParentKey(null);
			mapping.setGeneratedKey(null);
		}

		// mapping between XML fields and clover fields initialization
		if (attributes.exists(XML_XMLFIELDS) && attributes.exists(XML_CLOVERFIELDS)) {
			mapping.setXmlElementsSource(attributes.getString(XML_XMLFIELDS, null));
			mapping.setOutputFieldsSource(attributes.getString(XML_CLOVERFIELDS, null));
			
			String[] xmlFields = attributes.getString(XML_XMLFIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER);
			String[] cloverFields = attributes.getString(XML_CLOVERFIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

			// TODO add existence check for Clover fields, if possible
			if (xmlFields.length == cloverFields.length) {
				mapping.setXmlElements(xmlFields);
				mapping.setOutputFields(cloverFields);
			} else {
				result.getErrors().add("Mapping for element: " + mapping.getElementName() + " must have same number of the xml fields and the clover fields attribute.");
			}
		}

		// sequence field
		if (attributes.exists(XML_SEQUENCEFIELD)) {
			mapping.setSequenceField(attributes.getString(XML_SEQUENCEFIELD, null));
			mapping.setSequenceID(attributes.getString(XML_SEQUENCEID, null));
		}

		// skip rows field
		if (attributes.exists(XML_SKIP_ROWS_ATTRIBUTE)) {
			mapping.setSkipRecordsCountSource(attributes.getString(XML_SKIP_ROWS_ATTRIBUTE, null));
			mapping.setSkipRecordsCount(attributes.getInteger(XML_SKIP_ROWS_ATTRIBUTE, 0));
		}

		// number records field
		if (attributes.exists(XML_NUMRECORDS_ATTRIBUTE)) {
			mapping.setRecordCountSource(attributes.getString(XML_NUMRECORDS_ATTRIBUTE, null));
			mapping.setRecordCountLimit(attributes.getInteger(XML_NUMRECORDS_ATTRIBUTE, Integer.MAX_VALUE));
		}

		// template declaration
		if (attributes.exists(XML_TEMPLATE_ID)) {
			final String templateId = attributes.getString(XML_TEMPLATE_ID, null);
			mapping.setTemplateID(templateId);
			if (declaredTemplates.containsKey(templateId)) {
				result.getErrors().add("Template '" + templateId + "' has duplicate declaration");
			}
			declaredTemplates.put(templateId, mapping);
		}

		if (attributes.exists(XML_TEMPLATE_REF)) {
			mapping.setTemplateReference(attributes.getString(XML_TEMPLATE_REF, null));
		}

		if (attributes.exists(XML_TEMPLATE_DEPTH)) {
			mapping.setTemplateNestedDepthSource(attributes.getString(XML_TEMPLATE_DEPTH, null));
			mapping.setTemplateNestedDepth(attributes.getInteger(XML_TEMPLATE_DEPTH, -1));
		}

		if (attributes.exists(XML_MAPPING_PARENT_RECORD)) {
			String nested = attributes.getString(XML_MAPPING_PARENT_RECORD, null);
			mapping.setUsingParentRecord(Boolean.parseBoolean(nested));
			mapping.setNestedSource(nested);
		}

		if (attributes.exists(XML_MAPPING_IMPLICIT)) {
			String implicit = attributes.getString(XML_MAPPING_IMPLICIT, null);
			mapping.setImplicit(Boolean.parseBoolean(implicit));
			mapping.setImplicitSource(implicit);
		}
		
		result.setMapping(mapping);
		
		// parse children
		if (result.isSuccessful()) {
			NodeList nodes = nodeXML.getChildNodes();
			for (int i = 0; i < nodes.getLength(); i++) {
				org.w3c.dom.Node childNode = nodes.item(i);
				
				XMLMappingParseResult<? extends XMLMappingDefinition> childResult = parseMapping(childNode, result.getMapping());			 
				if (!childResult.isSuccessful()) {
					result.getErrors().addAll(childResult.getErrors());
				} else {
					result.getMapping().getChildren().add(childResult.getMapping());
				}
			}		
		}
		
		return result;
	}
	
	/** Parses the given XML node, assuming the node represents an {@link XMLElementMappingDefinition}.
	 * 
	 * @param nodeXML
	 * @return parsed mapping
	 */
	public XMLMappingParseResult<XMLElementMappingDefinition> parseElementMapping(org.w3c.dom.Node nodeXML)  throws XMLMappingDefinitionParseException {
		return parseElementMapping(nodeXML, null);
	}
	
	public XMLMappingParseResult<? extends XMLMappingDefinition> parseMapping(org.w3c.dom.Node nodeXML) throws XMLMappingDefinitionParseException {
		XMLMappingParseResult<? extends XMLMappingDefinition> result = parseMapping(nodeXML, null);
		return result;
	}

	
	public XMLMappingParseResult<? extends TypeOverrideDefinition> parseTypeOverride(org.w3c.dom.Node nodeXML) throws XMLMappingDefinitionParseException {
		XMLMappingParseResult<TypeOverrideDefinition> result = new XMLMappingParseResult<TypeOverrideDefinition>();
		
		TypeOverrideDefinition definition = new TypeOverrideDefinition();
		
		ComponentXMLAttributes attributes = createComponentAttributes((Element) nodeXML);

		
		if (attributes.exists(XML_TYPE_OVERRIDE_ELEMENT_PATH)) {
			definition.setElementPath(attributes.getString(XML_TYPE_OVERRIDE_ELEMENT_PATH, null));
		}
		
		if (attributes.exists(XML_TYPE_OVERRIDE_OVERRIDING_TYPE)) {
			definition.setOverridingType(attributes.getString(XML_TYPE_OVERRIDE_OVERRIDING_TYPE, null));
		}
		
		result.setMapping(definition);
		
		return result;
	}

	public XMLMappingParseResult<? extends XMLMappingsDefinition> parseMappings(org.w3c.dom.Node nodeXML) throws XMLMappingDefinitionParseException {
		XMLMappingParseResult<XMLMappingsDefinition> result = new XMLMappingParseResult<XMLMappingsDefinition>();
		
		XMLMappingsDefinition definition = new XMLMappingsDefinition();
		
		List<org.w3c.dom.Node> nodesToProcess = new LinkedList<org.w3c.dom.Node>();
		
		// parse children
		NodeList nodes = nodeXML.getChildNodes();
		for (int i = 0; i < nodes.getLength(); i++) {
			org.w3c.dom.Node childNode = nodes.item(i);
			
			if (XML_TYPE_OVERRIDE.equals(childNode.getNodeName())) {
				XMLMappingParseResult<? extends TypeOverrideDefinition> childResult = parseTypeOverride(childNode);			 
				if (!childResult.isSuccessful()) {
					result.getErrors().addAll(childResult.getErrors());
					continue;
				} 
				TypeOverrideDefinition typeOverride = childResult.getMapping();
				
				definition.getTypeOverrides().put(typeOverride.getElementPath(), typeOverride);
			} else {
				nodesToProcess.add(childNode);
			}
		}
		
		result.setMapping(definition);
		
		// parse children
		if (result.isSuccessful()) {
			for (org.w3c.dom.Node childNode : nodesToProcess) {
				XMLMappingParseResult<? extends XMLMappingDefinition> childResult = parseMapping(childNode, result.getMapping());			 
				if (!childResult.isSuccessful()) {
					result.getErrors().addAll(childResult.getErrors());
				} else {
					result.getMapping().getChildren().add(childResult.getMapping());
				}
			}		
		}
		
		return result;
	}	
	
	
	public XMLMappingParseResult<? extends XMLMappingDefinition> parseMapping(org.w3c.dom.Node nodeXML, XMLMappingDefinition mapping) throws XMLMappingDefinitionParseException {
		XMLMappingParseResult<? extends XMLMappingDefinition> result = new XMLMappingParseResult<XMLMappingDefinition>();
		
		
		if (XML_ROOT_ELEMENT.equals(nodeXML.getNodeName())) {
			result = parseMappings(nodeXML);
			
		} else if (XML_INPUTFIELDMAPPING.equals(nodeXML.getNodeName())) {
			result = parseInputFieldMapping(nodeXML, mapping);

		} else if (XML_MAPPING.equals(nodeXML.getNodeName())) {
			result = parseElementMapping(nodeXML, mapping);

        } else if (nodeXML.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
        	result.getErrors().add("Unknown element '" + nodeXML.getNodeName() + "' is ignored with all it's child elements.");
        }

		return result;
    }
	
	
	protected boolean checkAttributesPresent(ComponentXMLAttributes attributes, List<String> errors, String... names) {
		boolean found = true;
		for (String name : names) {
			if (!attributes.exists(name)) {
				errors.add("Missing attribute " + name);
				found = false;
			}
		}
		
		return found;
	}
	
}