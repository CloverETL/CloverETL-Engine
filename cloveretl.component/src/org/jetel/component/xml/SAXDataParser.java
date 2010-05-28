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
package org.jetel.component.xml;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 9.11.2009
 *
 * NOTE: almost hard copy from XMLExtract - has to be re-developed!!!
 * NOTE: autofilling, record skipping and max record number is not supported
 */
public class SAXDataParser {

    private static final Log logger = LogFactory.getLog(SAXDataParser.class);

    // mapping attributes
    private static final String XML_MAPPING = "Mapping";
	private final static String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
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
//	private static final String XML_SKIP_SOURCE_ROWS_ATTRIBUTE = "skipSourceRows";
//	private static final String XML_NUM_SOURCE_RECORDS_ATTRIBUTE = "numSourceRecords";
	private static final String XML_TRIM_ATTRIBUTE = "trim";
    private static final String XML_VALIDATE_ATTRIBUTE = "validate";
    private static final String XML_XML_FEATURES_ATTRIBUTE = "xmlFeatures";

    private static final String FEATURES_DELIMETER = ";";
    private static final String FEATURES_ASSIGN = ":=";

    private Node parentNode;
    
	private String rawMapping;

	private DataRecord[] outputDataRecords;
	
    // Map of elementName => output port
    private Map<String, ParseMapping> m_elementPortMap = new HashMap<String, ParseMapping>();

    private SAXParser parser;

    private boolean validate;
    
	private String xmlFeatures;

	// can I use nested nodes for mapping processing?
    private boolean useNestedNodes = true;

	private boolean trim = true;

	public SAXDataParser(Node parentNode, String rawMapping, DataRecord[] outputDataRecords) {
		this.parentNode = parentNode;
		this.rawMapping = rawMapping;
		this.outputDataRecords = outputDataRecords;
		this.validate = false;
	}

	public void init() throws ComponentNotReadyException {
		//create mapping DOM
		Document mappingDOM;
		try {
			mappingDOM = createMappingDOM(rawMapping);
			createMapping(mappingDOM);
		} catch (XMLConfigurationException e) {
			throw new ComponentNotReadyException(e);
		}
		
    	// create new sax factory
        SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setValidating(validate);
		initXmlFeatures(factory);
        
        try {
        	// create new sax parser
            parser = factory.newSAXParser();
        } catch (Exception ex) {
        	throw new ComponentNotReadyException(ex);
        }
	}

    public void parse(InputSource inputSource) throws SAXException, IOException {
    	// parse the input source
    	parser.parse(inputSource, new SAXHandler(parentNode, m_elementPortMap, useNestedNodes, trim));
    }

	public void free() {
		
	}

    private static Document createMappingDOM(String mapping) throws XMLConfigurationException {
        InputSource is = new InputSource(new StringReader(mapping));
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setCoalescing(true);
        try {
            return dbf.newDocumentBuilder().parse(is);
        } catch (Exception e) {
            throw new XMLConfigurationException("Mapping parameter parse error occur.", e);
        }
    }

    private void createMapping(Document mappingDOM) throws XMLConfigurationException {
		Element rootElement = mappingDOM.getDocumentElement();
		NodeList mappingNodes = rootElement.getChildNodes();
		
        //iterate over 'Mapping' elements
        for (int i = 0; i < mappingNodes.getLength(); i++) {
            org.w3c.dom.Node node = mappingNodes.item(i);
            processMappings(null, node);
        }

    }
    
    private void processMappings(ParseMapping parentMapping, org.w3c.dom.Node nodeXML) throws XMLConfigurationException {
        if (XML_MAPPING.equals(nodeXML.getNodeName())) {
            // for a mapping declaration, process all of the attributes
            // element, outPort, parentKeyName, generatedKey, ...
            ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element) nodeXML);
            ParseMapping mapping = null;
            
            try {
            	int outputPort = -1;
            	if (attributes.exists(XML_OUTPORT)) { 
            		outputPort = attributes.getInteger(XML_OUTPORT);
            	}
                mapping = new ParseMapping(attributes.getString(XML_ELEMENT), outputPort);
                mapping.setOutRecord(outputDataRecords[outputPort]);
            } catch (AttributeNotFoundException ex) {
            	throw new XMLConfigurationException("Mapping missing a required attribute - element.", ex);
            }
            
            // Add this mapping to the parent
            if (parentMapping != null) {
                parentMapping.addChildMapping(mapping);
                mapping.setParent(parentMapping);
            } else {
                addMapping(mapping);
            }

            boolean parentKeyPresent = false;
            boolean generatedKeyPresent = false;
            if (attributes.exists(XML_PARENTKEY)) {
                mapping.setParentKey(attributes.getString(XML_PARENTKEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
                parentKeyPresent = true;
            }
            
            if (attributes.exists(XML_GENERATEDKEY)) {
                mapping.setGeneratedKey(attributes.getString(XML_GENERATEDKEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
                generatedKeyPresent = true;
            }
            
            if (parentKeyPresent != generatedKeyPresent) {
                logger.warn("XML Extract Mapping for element: " + mapping.getElement() + " must either have both parentKey and generatedKey attributes or neither.");
                mapping.setParentKey(null);
                mapping.setGeneratedKey(null);
            }

            if (parentKeyPresent && mapping.getParent() == null) {
                logger.warn("XML Extact Mapping for element: " + mapping.getElement() + " may only have parentKey or generatedKey attributes if it is a nested mapping.");
                mapping.setParentKey(null);
                mapping.setGeneratedKey(null);
            }

            //mapping between xml fields and clover fields initialization
            if (attributes.exists(XML_XMLFIELDS) && attributes.exists(XML_CLOVERFIELDS)) {
                String[] xmlFields = attributes.getString(XML_XMLFIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER);
                String[] cloverFields = attributes.getString(XML_CLOVERFIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

                if(xmlFields.length == cloverFields.length){
                    Map<String, String> xmlCloverMap = new HashMap<String, String>();
                    for (int i = 0; i < xmlFields.length; i++) {
                        xmlCloverMap.put(xmlFields[i], cloverFields[i]);
                    }
                    mapping.setXml2CloverFieldsMap(xmlCloverMap);
                } else {
                    logger.warn("XML Extact Mapping for element: " + mapping.getElement() + " must have same number of the xml fields and the clover fields attribute.");
                }
            }
            
            //sequence field
            if (attributes.exists(XML_SEQUENCEFIELD)) {
                mapping.setSequenceField(attributes.getString(XML_SEQUENCEFIELD, null));
                mapping.setSequenceId(attributes.getString(XML_SEQUENCEID, null));
            }
            
            //skip rows field
            if (attributes.exists(XML_SKIP_ROWS_ATTRIBUTE)) {
                mapping.setSkipRecords4Mapping(attributes.getInteger(XML_SKIP_ROWS_ATTRIBUTE, 0));
            }
            
            //number records field
            if (attributes.exists(XML_NUMRECORDS_ATTRIBUTE)) {
                mapping.setNumRecords4Mapping(attributes.getInteger(XML_NUMRECORDS_ATTRIBUTE, Integer.MAX_VALUE));
            }
            
//            //skip source rows field
//            if (attributes.exists(XML_SKIP_SOURCE_ROWS_ATTRIBUTE)) {
//                mapping.setSkipSourceRecords4Mapping(attributes.getInteger(XML_SKIP_SOURCE_ROWS_ATTRIBUTE, 0));
//            }
//            
//            //number source records field
//            if (attributes.exists(XML_NUM_SOURCE_RECORDS_ATTRIBUTE)) {
//                mapping.setNumSourceRecords4Mapping(attributes.getInteger(XML_NUM_SOURCE_RECORDS_ATTRIBUTE, Integer.MAX_VALUE));
//            }
//
            // prepare variables for skip and numRecords for this mapping
        	mapping.prepareProcessSkipOrNumRecords();

            // Process all nested mappings
            NodeList nodes = nodeXML.getChildNodes();
            for (int i = 0; i < nodes.getLength(); i++) {
                org.w3c.dom.Node node = nodes.item(i);
                processMappings(mapping, node);
            }
            
            // prepare variable reset of skip and numRecords' attributes
            mapping.prepareReset4CurrentRecord4Mapping();
            
        } else if (nodeXML.getNodeType() == org.w3c.dom.Node.TEXT_NODE) {
            // Ignore text values inside nodes
        } else {
            logger.warn("Unknown element: " + nodeXML.getLocalName() + " ignoring it and all child elements.");
        }
    }

	private void initXmlFeatures(SAXParserFactory factory) throws ComponentNotReadyException {
		if (xmlFeatures == null) return;
		String[] aXmlFeatures = xmlFeatures.split(FEATURES_DELIMETER);
		String[] aOneFeature;
	    try {
			for (String oneFeature: aXmlFeatures) {
				aOneFeature = oneFeature.split(FEATURES_ASSIGN);
				if (aOneFeature.length != 2) 
					throw new JetelException("The xml feature '" + oneFeature + "' has wrong format");
					factory.setFeature(aOneFeature[0], Boolean.parseBoolean(aOneFeature[1]));
			}
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
	}

    /**
     * Accessor to add a mapping programatically.
     */
    public void addMapping(ParseMapping mapping) {
        m_elementPortMap.put(mapping.getElement(), mapping);
    }

	public boolean isValidate() {
		return validate;
	}

	public void setValidate(boolean validate) {
		this.validate = validate;
	}

	public String getXmlFeatures() {
		return xmlFeatures;
	}

	public void setXmlFeatures(String xmlFeatures) {
		this.xmlFeatures = xmlFeatures;
	}

	public boolean isUseNestedNodes() {
		return useNestedNodes;
	}

	public void setUseNestedNodes(boolean useNestedNodes) {
		this.useNestedNodes = useNestedNodes;
	}

	public boolean isTrim() {
		return trim;
	}

	public void setTrim(boolean trim) {
		this.trim = trim;
	}

}
