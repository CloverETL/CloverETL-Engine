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
package org.jetel.data.parser;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.WeakReference;
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.io.SAXContentHandler;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.StringDataField;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.XmlUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.formatter.DateFormatter;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author mlaska (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Jan 9, 2012
 */
/*
 * Invariant:
 *  Mappings are preprocessed so that all element and attribute references stored in internal structures
 *  use universal names :
 *  	element="mov:movies" -> element="{http://www.javlin.eu}movies"
 *  	xmlFields="mov:att1" -> xmlFields="{http://www.javlin.eu}att1"
 *  
 *  Default namespace in mapping -> NOT POSSIBLE
 *  We are not able to implement default namespace functionality.
 *  Default namespace (declared without prefix e.g. xmlns="http://www.javlin.eu/default-ns") does
 *  NOT apply to attributes!!! (see www.w3.org/TR/REC-xml-names/#defaulting)
 *  Since we currently do not know which xmlFields 
 *  are attributes and which are elements -> so we do not know which should be expanded by default namespace.
 *  User therefore has to declare explicit namespace prefix in the Mapping, even for
 *  attributes and elements falling into the default namespace in the processed XML document.
 *  
 */
public class XmlSaxParser {
	
	private static final String FEATURES_DELIMETER = ";";
	private static final String FEATURES_ASSIGN = ":=";
	
	private static final String XML_MAPPING = "Mapping";
    public static final String XML_ELEMENT = "element";
    public static final String XML_OUTPORT = "outPort";
    public static final String XML_PARENTKEY = "parentKey";
    public static final String XML_GENERATEDKEY = "generatedKey";
    public static final String XML_XMLFIELDS = "xmlFields";
    public static final String XML_CLOVERFIELDS = "cloverFields";
    public static final String XML_SEQUENCEFIELD = "sequenceField";
    public static final String XML_SEQUENCEID = "sequenceId";
    private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRows";
    private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";

    public static final String XML_TEMPLATE_ID = "templateId";
    public static final String XML_TEMPLATE_REF = "templateRef";
    public static final String XML_TEMPLATE_DEPTH = "nestedDepth";

	private static final String PARENT_MAPPING_REFERENCE_PREFIX = "..";
	private static final String PARENT_MAPPING_REFERENCE_SEPARATOR = "/";
	private static final String PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR = PARENT_MAPPING_REFERENCE_PREFIX + PARENT_MAPPING_REFERENCE_SEPARATOR;
	private static final String ELEMENT_VALUE_REFERENCE = "{}.";

	private static final Log logger = LogFactory.getLog(XmlSaxParser.class);

    protected TransformationGraph graph;
    protected Node parentComponent;
	
    protected String mapping;
	protected String mappingURL = null;
    
	private boolean useNestedNodes = true;
	private boolean trim = true;
	private boolean validate;
	private String xmlFeatures;

	// Map of elementName => output port
    protected Map<String, Mapping> m_elementPortMap = new HashMap<String, Mapping>();
    protected TreeMap<String, Mapping> declaredTemplates = new TreeMap<String, Mapping>();
    /**
     * Namespace bindings relate namespace prefix used in Mapping specification
     * and the namespace URI used by the namespace declaration in processed XML document
     */
    private HashMap<String,String> namespaceBindings = new HashMap<String,String>();

    // global skip and numRecords
    private int skipRows=0; // do not skip rows by default
    private int numRecords = -1;

    private AutoFilling autoFilling = new AutoFilling();
  
	protected SAXParser parser;
	protected SAXHandler saxHandler;

	public XmlSaxParser(TransformationGraph graph, Node parentComponent) {
		this(graph, parentComponent, null);
	}
	
	public XmlSaxParser(TransformationGraph graph, Node parentComponent, String mapping) {
		this.graph = graph;
		this.parentComponent = parentComponent;
		this.mapping = mapping;
	}
	
	public void init() throws ComponentNotReadyException {
		augmentNamespaceURIs();

		URL projectURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;

		// prepare mapping
		NodeList mappingNodes = null;
		if (mappingURL != null) {
			try {
				ReadableByteChannel ch = FileUtils.getReadableChannel(projectURL, mappingURL);
				Document doc = XmlUtils.createDocumentFromChannel(ch);
				Element rootElement = doc.getDocumentElement();
				mappingNodes = rootElement.getChildNodes();
			} catch (Exception e) {
				throw new ComponentNotReadyException(e);
			}
		} else if (mapping != null) {
			Document doc;
			try {
				doc = XmlUtils.createDocumentFromString(mapping);
			} catch (JetelException e) {
				throw new ComponentNotReadyException(e);
			}
			Element rootElement = doc.getDocumentElement();
			mappingNodes = rootElement.getChildNodes();
		}
		// iterate over 'Mapping' elements
		declaredTemplates.clear();
		String errorPrefix = parentComponent.getId() + ": Mapping error - ";
		for (int i = 0; i < mappingNodes.getLength(); i++) {
			org.w3c.dom.Node node = mappingNodes.item(i);
			List<String> errors = processMappings(graph, null, node);
			for (String error : errors) {
				logger.warn(errorPrefix + error);
			}
		}
		
		if (m_elementPortMap.size() < 1) {
			throw new ComponentNotReadyException(parentComponent.getId() + ": At least one mapping has to be defined.  <Mapping element=\"elementToMatch\" outPort=\"123\" [parentKey=\"key in parent\" generatedKey=\"new foreign key in target\"]/>");
		}
		
		
		// create new sax factory
		SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setNamespaceAware(true);
		factory.setValidating(validate);
		initXmlFeatures(factory, xmlFeatures); // TODO:
		try {
			parser = factory.newSAXParser();
		} catch (Exception ex) {
			throw new ComponentNotReadyException(ex);
		}
		saxHandler = new SAXHandler(getXMLMappingValues());
	}
	
	public void parse(InputSource inputSource) throws JetelException, SAXException {
		try {
			parser.parse(inputSource, saxHandler);
		} catch (IOException e) {
			throw new JetelException("Unexpected exception", e);
		}
	}
	
	protected OutputPort getOutputPort(int outPortIndex) {
		return parentComponent.getOutputPort(outPortIndex);
	}

	protected Sequence createPrimitiveSequence(String id, TransformationGraph graph, String name) {
		// FIXME: PrimitiveSequence is not accessible from engine (PrimitiveSequence.SEQUENCE_TYPE)
		return SequenceFactory.createSequence(graph, "PRIMITIVE_SEQUENCE", new Object[] { id, graph, name}, new Class[] { String.class, TransformationGraph.class, String.class });
	}

    public List<String> processMappings(TransformationGraph graph, Mapping parentMapping, org.w3c.dom.Node nodeXML) {
		List<String> errors = new LinkedList<String>();
		if (XML_MAPPING.equals(nodeXML.getNodeName())) {
			// for a mapping declaration, process all of the attributes
			// element, outPort, parentKeyName, generatedKey
			ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element) nodeXML, graph);
			Mapping mapping = null;

			if (attributes.exists(XML_TEMPLATE_REF)) {
				// template mapping reference
				String templateId = null;
				try {
					templateId = attributes.getString(XML_TEMPLATE_REF);
				} catch (AttributeNotFoundException e) {
					// this cannot happen (see if above)
					errors.add("Attribute 'templateId' is missing");
					return errors;
				}

				if (!declaredTemplates.containsKey(templateId)) {
					errors.add("Template '" + templateId + "' has not been declared");
					return errors;
				}
				
				mapping = new Mapping(declaredTemplates.get(templateId), parentMapping);
			}

        	// standard mapping declaration
            try {
            	int outputPort = -1;
            	if (attributes.exists(XML_OUTPORT)) {
            		outputPort = attributes.getInteger(XML_OUTPORT);
            	}

            	if (mapping == null) {
           		mapping = new Mapping(
           				createQualifiedName(attributes.getString(XML_ELEMENT)), 
           				outputPort, 
           				parentMapping);
            	} else {
            		if (outputPort != -1) {
	            		mapping.setOutPort(outputPort);
	            		if (attributes.exists(XML_ELEMENT)) {
	            			mapping.setElement(
	            					createQualifiedName(attributes.getString(XML_ELEMENT)));
	            		}
            		}
            	}
            } catch(AttributeNotFoundException ex) {
            	errors.add("Required attribute 'element' missing. Skipping this mapping and all children.");
                return errors;
            }
            
            // Add new root mapping
            if (parentMapping == null) {
                addMapping(mapping);
            }

            boolean parentKeyPresent = false;
            boolean generatedKeyPresent = false;
            if (attributes.exists(XML_PARENTKEY)) {
           	final String[] parentKey = attributes.getString(XML_PARENTKEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
               mapping.setParentKey(parentKey);
                parentKeyPresent = true;
            }
            
            if (attributes.exists(XML_GENERATEDKEY)) {
           	final String[] generatedKey = attributes.getString(XML_GENERATEDKEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX); 
               mapping.setGeneratedKey(generatedKey);
                generatedKeyPresent = true;
            }
            
            if (parentKeyPresent != generatedKeyPresent) {
            	errors.add("Mapping for element: " + mapping.getElement() 
                		+ " must either have both 'parentKey' and 'generatedKey' attributes or neither.");
                mapping.setParentKey(null);
                mapping.setGeneratedKey(null);
            }

            if (parentKeyPresent && mapping.getParent() == null) {
            	errors.add("Mapping for element: " + mapping.getElement() 
                		+ " may only have 'parentKey' or 'generatedKey' attributes if it is a nested mapping.");
                mapping.setParentKey(null);
                mapping.setGeneratedKey(null);
            }

            //mapping between xml fields and clover fields initialization
            if (attributes.exists(XML_XMLFIELDS) && attributes.exists(XML_CLOVERFIELDS)) {
                String[] xmlFields = attributes.getString(XML_XMLFIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER);
                String[] cloverFields = attributes.getString(XML_CLOVERFIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
                // TODO add existence check for Clover fields, if possible
                if(xmlFields.length == cloverFields.length){
                    for (int i = 0; i < xmlFields.length; i++) {
                    	if (xmlFields[i].startsWith(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR) || xmlFields[i].equals(PARENT_MAPPING_REFERENCE_PREFIX)) {
                    		mapping.addAncestorFieldMapping(xmlFields[i], cloverFields[i]);
                    	} else {
                    		mapping.putXml2CloverFieldMap(xmlFields[i], cloverFields[i]);
                    	}
                    }
                } else {
                	errors.add("Mapping for element: " + mapping.getElement() 
                    		+ " must have same number of the xml fields and the clover fields attribute.");
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
            
            // template declaration
            if (attributes.exists(XML_TEMPLATE_ID)) {
            	final String templateId = attributes.getString(XML_TEMPLATE_ID, null);
            	if (declaredTemplates.containsKey(templateId)) {
            		errors.add("Template '" + templateId + "' has duplicate declaration");
            	}
            	declaredTemplates.put(templateId, mapping);
            }
            
            // prepare variables for skip and numRecords for this mapping
        	mapping.prepareProcessSkipOrNumRecords();

        	// multiple nested references of a template
        	if (attributes.exists(XML_TEMPLATE_REF) && attributes.exists(XML_TEMPLATE_DEPTH)) {
				int depth = attributes.getInteger(XML_TEMPLATE_DEPTH, 1) - 1;
				Mapping currentMapping = mapping;
				while (depth > 0) {
					currentMapping = new Mapping(currentMapping, currentMapping);
					currentMapping.prepareProcessSkipOrNumRecords();
					depth--;
				}
				while (currentMapping != mapping) {
					currentMapping.prepareReset4CurrentRecord4Mapping();
					currentMapping = currentMapping.getParent();
				}
        	}
        	
            // Process all nested mappings
            NodeList nodes = nodeXML.getChildNodes();
            for (int i = 0; i < nodes.getLength(); i++) {
                org.w3c.dom.Node node = nodes.item(i);
                errors.addAll(processMappings(graph, mapping, node));
            }
            
            // prepare variable reset of skip and numRecords' attributes
            mapping.prepareReset4CurrentRecord4Mapping();
            
        } else if (nodeXML.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {
        	errors.add("Unknown element '" + nodeXML.getNodeName() + "' is ignored with all it's child elements.");
        } // Ignore every other xml element (text values, comments...)
		return errors;
    }
	
	public void setMapping(String mapping) {
		this.mapping = mapping;
	}
	
	public void setMappingURL(String mappingURL) {
		this.mappingURL = mappingURL;
	}
	
	public boolean isTrim() {
		return trim;
	}

	public void setTrim(boolean trim) {
		this.trim = trim;
	}
	
	public void setGraph(TransformationGraph graph) {
		this.graph = graph;
	}

	public boolean isUseNestedNodes() {
		return useNestedNodes;
	}

	public void setUseNestedNodes(boolean useNestedNodes) {
		this.useNestedNodes = useNestedNodes;
	}

    /**
     * Sets skipRows - how many elements to skip.
     * @param skipRows
     */
    public void setSkipRows(int skipRows) {
        this.skipRows = skipRows;
    }
    
    /**
     * Sets numRecords - how many elements to process.
     * @param numRecords
     */
    public void setNumRecords(int numRecords) {
        this.numRecords = Math.max(numRecords, 0);
    }

    /**
     * Accessor to add a mapping programatically.
     */
    public void addMapping(Mapping mapping) {
        m_elementPortMap.put(mapping.getElement(), mapping);
    }
    
    /**
     * Mapping holds a single mapping.
     */
    public class Mapping {
        String m_element;								// name of an element for this mapping
        int m_outPort;									// output port number
        DataRecord m_outRecord;							// output record
        String[] m_parentKey;							// parent keys
        String[] m_generatedKey;						// generated keys
        Map<String, Mapping> m_childMap;				// direct children for this mapping 
        WeakReference<Mapping> m_parent;				// direct parent mapping
        int m_level;									// original xml tree level (a depth of this element) 
        String m_sequenceField;							// sequence field
        String m_sequenceId;							// sequence ID
        Sequence sequence;								// sequence (Simple, Db,..)
        
        /** Mapping - xml name -> clover field name */
        Map<String, String> xml2CloverFieldsMap = new HashMap<String, String>();

        /** List of clover fields (among else) which will be filled from ancestor */
        List<AncestorFieldMapping> fieldsFromAncestor; 

        /** Mapping - xml name -> clover field name; these xml fields are referenced by descendant mappings */
        Map<String, String> descendantReferences = new HashMap<String, String>();
        
        /** Set of Clover fields which are mapped explicitly (using xmlFields & cloverFields attributes).
         *  It is union of xml2CloverFieldsMap.values() and Clover fields from fieldsFromAncestor list. Its purpose: quick lookup
         */
        Set<String> explicitCloverFields = new HashSet<String>(); 
        
        // for skip and number a record attribute for this mapping
		int skipRecords4Mapping;						// skip records
		int numRecords4Mapping = Integer.MAX_VALUE;		// number records
//		int skipSourceRecords4Mapping;					// skip records
//		int numSourceRecords4Mapping = -1;              // number records
		int currentRecord4Mapping;						// record counter for this mapping
		boolean processSkipOrNumRecords;				// what xml element can be skiped
		boolean bDoMap = true;							// should I skip an xml element? depends on processSkipOrNumRecords
		boolean bReset4CurrentRecord4Mapping;			// should I reset submappings?
        
		/**
		 * Copy constructor - created a deep copy of all attributes and children elements
		 */
		public Mapping(Mapping otherMapping, Mapping parent) {
			this.m_element = otherMapping.m_element;
			this.m_outPort = otherMapping.m_outPort;
			this.m_parentKey = otherMapping.m_parentKey == null ? null : Arrays.copyOf(otherMapping.m_parentKey,otherMapping.m_parentKey.length);
			this.m_generatedKey = otherMapping.m_generatedKey == null ? null : Arrays.copyOf(otherMapping.m_generatedKey, otherMapping.m_generatedKey.length);
			this.m_sequenceField = otherMapping.m_sequenceField;
			this.m_sequenceId = otherMapping.m_sequenceId;
			this.skipRecords4Mapping = otherMapping.skipRecords4Mapping;
			this.numRecords4Mapping = otherMapping.numRecords4Mapping;
			xml2CloverFieldsMap = new HashMap<String, String>(otherMapping.xml2CloverFieldsMap);
			
			// Create deep copy of children elements 
			if (otherMapping.m_childMap != null) {
				this.m_childMap = new HashMap<String,Mapping>();
				for (String key : otherMapping.m_childMap.keySet()) {
					final Mapping child = new Mapping(otherMapping.m_childMap.get(key), this);
					this.m_childMap.put(key, child);
				}
			}

			if (parent != null) {
				setParent(parent);
				parent.addChildMapping(this);
			}
			
			if (otherMapping.hasFieldsFromAncestor()) {
				for (AncestorFieldMapping m : otherMapping.getFieldsFromAncestor()) {
					addAncestorFieldMapping(m.originalFieldReference, m.currentField);
				}
			}

		}
		
        /**
         * Minimally required information.
         */
        public Mapping(String element, int outPort, Mapping parent) {
            m_element = element;
            m_outPort = outPort;
            m_parent = new WeakReference<Mapping>(parent);
            if (parent != null) {
            	parent.addChildMapping(this);
            }
        }
        
		/**
         * Gives the optional attributes parentKey and generatedKey.
         */
        public Mapping(String element, int outPort, String parentKey[],
                String[] generatedKey, Mapping parent) {
            this(element, outPort, parent);
            
            m_parentKey = parentKey;
            m_generatedKey = generatedKey;
        }
        
        /**
         * Gets original xml tree level (a deep of this element)
         * @return
         */
        public int getLevel() {
            return m_level;
        }
        
        /**
         * Sets original xml tree level (a deep of this element)
         * @param level
         */
        public void setLevel(int level) {
            m_level = level;
        }
        
        /**
         * Sets direct children for this mapping. 
         * @return
         */
        public Map<String, Mapping> getChildMap() {
            return m_childMap;
        }
        
        /**
         * Gets direct children for this mapping. 
         * @param element
         * @return
         */
        public Mapping getChildMapping(String element) {
            if (m_childMap == null) {
                return null;
            }
            return m_childMap.get(element);
        }
        
        /**
         * Adds a direct child for this mapping.
         * @param mapping
         */
        public void addChildMapping(Mapping mapping) {
            if (m_childMap == null) {
                m_childMap = new HashMap<String, Mapping>();
            }
            m_childMap.put(mapping.getElement(), mapping);
        }
        
        /**
         * Removes a direct child for this mapping.
         * @param mapping
         */
        public void removeChildMapping(Mapping mapping) {
            if (m_childMap == null) {
                return;
            }
            m_childMap.remove(mapping.getElement());
        }
        
        /**
         * Gets an element name for this mapping.
         * @return
         */
        public String getElement() {
            return m_element;
        }
        
        /**
         * Sets an element name for this mapping.
         * @param element
         */
        public void setElement(String element) {
            m_element = element;
        }
        
        /**
         * Gets generated keys of for this mapping.
         * @return
         */
        public String[] getGeneratedKey() {
            return m_generatedKey;
        }
        
        /**
         * Sets generated keys of for this mapping.
         * @param generatedKey
         */
        public void setGeneratedKey(String[] generatedKey) {
            m_generatedKey = generatedKey;
        }
        
        /**
         * Gets an output port.
         * @return
         */
        public int getOutPort() {
            return m_outPort;
        }
        
        /**
         * Sets an output port.
         * @param outPort
         */
        public void setOutPort(int outPort) {
            m_outPort = outPort;
        }
        
        /**
         * Gets mapping - xml name -> clover field name
         * WARNING: values of this map must be kept in synch with explicitCloverFields; prefer {@link #putXml2CloverFieldMap()} 
         */
        public Map<String, String> getXml2CloverFieldsMap() {
            return xml2CloverFieldsMap;
        }
        
        public void putXml2CloverFieldMap(String xmlField, String cloverField) {
        	xml2CloverFieldsMap.put(createQualifiedName(xmlField), cloverField);
        	explicitCloverFields.add(cloverField);
        }
        
        /**
         * Gets an output record.
         * @return
         */
        public DataRecord getOutRecord() {
            if (m_outRecord == null) {
                OutputPort outPort = getOutputPort(getOutPort());
                if (outPort != null) {
                	DataRecordMetadata dataRecordMetadata = outPort.getMetadata();
                	autoFilling.addAutoFillingFields(dataRecordMetadata);
                    m_outRecord = new DataRecord(dataRecordMetadata);
                    m_outRecord.init();
                    m_outRecord.reset();
                } // Original code is commented, it is valid to have null port now
                /* else {
                    LOG
                            .warn(getId()
                            + ": Port "
                            + getOutPort()
                            + " does not have an edge connected.  Please connect the edge or remove the mapping.");
                }*/ 
            }
            return m_outRecord;
        }
        
        /**
         * Sets an output record.
         * @param outRecord
         */
        public void setOutRecord(DataRecord outRecord) {
            m_outRecord = outRecord;
        }
        
        /**
         * Gets parent key.
         * @return
         */
        public String[] getParentKey() {
            return m_parentKey;
        }
        
        /**
         * Sets parent key.
         * @param parentKey
         */
        public void setParentKey(String[] parentKey) {
            m_parentKey = parentKey;
        }
        
        /**
         * Gets a parent mapping.
         * @return
         */
        public Mapping getParent() {
            if (m_parent != null) {
                return m_parent.get();
            } else {
                return null;
            }
        }
        
        /**
         * Sets a parent mapping.
         * @param parent
         */
        public void setParent(Mapping parent) {
            m_parent = new WeakReference<Mapping>(parent);
        }

        /**
         * Gets a sequence name.
         * @return
         */
        public String getSequenceField() {
            return m_sequenceField;
        }

        /**
         * Sets a sequence name.
         * @param field
         */
        public void setSequenceField(String field) {
            m_sequenceField = field;
        }

        /**
         * Gets a sequence ID.
         * @return
         */
        public String getSequenceId() {
            return m_sequenceId;
        }

        /**
         * Sets a sequence ID.
         * @param id
         */
        public void setSequenceId(String id) {
            m_sequenceId = id;
        }
        
        /**
         * Gets a Sequence (simple sequence, db sequence, ...).
         * @return
         */
        public Sequence getSequence() {
            if(sequence == null) {
                String element = StringUtils.normalizeName(StringUtils.trimXmlNamespace(getElement()));

                if(getSequenceId() == null) {
                    sequence = createPrimitiveSequence(element, graph, element);
                } else {
                    sequence = graph.getSequence(getSequenceId());

                    if(sequence == null) {
                        logger.warn(parentComponent.getId() + ": Sequence " + getSequenceId() + " does not exist in "
                                + "transformation graph. Primitive sequence is used instead.");
                        sequence = createPrimitiveSequence(element, graph, element);
                    }
                }
            }

            return sequence;
        }
        
        /**
         * processSkipOrNumRecords is true - mapping can be skipped
         */
		public boolean getProcessSkipOrNumRecords() {
			if (processSkipOrNumRecords) return true;
			Mapping parent = getParent();
			if (parent == null) {
				return processSkipOrNumRecords;
			}
			return parent.getProcessSkipOrNumRecords();
		}
		
		/**
		 * Sets inner variables for processSkipOrNumRecords.
		 */
		public void prepareProcessSkipOrNumRecords() {
			Mapping parentMapping = getParent();
			processSkipOrNumRecords = parentMapping != null && parentMapping.getProcessSkipOrNumRecords() ||
				(skipRecords4Mapping > 0 || numRecords4Mapping < Integer.MAX_VALUE);
		}
		
		/**
		 * Sets inner variables for bReset4CurrentRecord4Mapping.
		 */
		public void prepareReset4CurrentRecord4Mapping() {
			bReset4CurrentRecord4Mapping = processSkipOrNumRecords;
        	if (m_childMap != null) {
        		Mapping mapping;
        		for (Iterator<Entry<String, Mapping>> it=m_childMap.entrySet().iterator(); it.hasNext();) {
        			mapping = it.next().getValue();
        			if (mapping.processSkipOrNumRecords) {
        				bReset4CurrentRecord4Mapping = true;
        				break;
        			}
        		}
        	}
		}
		
		/**
		 * skipRecords for this mapping.
		 * @param skipRecords4Mapping
		 */
        public void setSkipRecords4Mapping(int skipRecords4Mapping) {
        	this.skipRecords4Mapping = skipRecords4Mapping;
        }
        
        /**
         * numRecords for this mapping.
         * @param numRecords4Mapping
         */
        public void setNumRecords4Mapping(int numRecords4Mapping) {
        	this.numRecords4Mapping = numRecords4Mapping;
        }
        
//		/**
//		 * skipRecords for this mapping.
//		 * @param skipRecords4Mapping
//		 */
//        public void setSkipSourceRecords4Mapping(int skipSourceRecords4Mapping) {
//        	this.skipSourceRecords4Mapping = skipSourceRecords4Mapping;
//        }
//        
//        /**
//         * numRecords for this mapping.
//         * @param numRecords4Mapping
//         */
//        public void setNumSourceRecords4Mapping(int numSourceRecords4Mapping) {
//        	this.numSourceRecords4Mapping = numSourceRecords4Mapping;
//        }
//
        /**
         * Counter for this mapping.
         */
        public void incCurrentRecord4Mapping() {
        	currentRecord4Mapping++;
		}
        
        /**
         * Resets submappings.
         */
        public void resetCurrentRecord4ChildMapping() {
        	if (!bReset4CurrentRecord4Mapping) return;
        	if (m_childMap != null) {
        		Mapping mapping;
        		for (Iterator<Entry<String, Mapping>> it=m_childMap.entrySet().iterator(); it.hasNext();) {
        			mapping = it.next().getValue();
        			mapping.currentRecord4Mapping = 0;
        			mapping.resetCurrentRecord4ChildMapping();
        		}
        	}
		}

        /**
         * Sets if this and child mapping should be skipped.
         */
		public void prepareDoMap() {
			if (!processSkipOrNumRecords) return;
			Mapping parent = getParent();
        	bDoMap = (parent == null || parent.doMap()) && 
        		currentRecord4Mapping >= skipRecords4Mapping && currentRecord4Mapping-skipRecords4Mapping < numRecords4Mapping;
        	if (m_childMap != null) {
        		Mapping mapping;
        		for (Iterator<Entry<String, Mapping>> it=m_childMap.entrySet().iterator(); it.hasNext();) {
        			mapping = it.next().getValue();
        			mapping.prepareDoMap();
        		}
        	}
		}
		
		/**
		 * Can process this mapping? It depends on currentRecord4Mapping, skipRecords4Mapping and numRecords4Mapping
		 * for this and parent mappings.
		 * @return
		 */
        public boolean doMap() {
        	return !processSkipOrNumRecords || (processSkipOrNumRecords && bDoMap);
        }
        
        public void addAncestorField(AncestorFieldMapping ancestorFieldReference) {
        	if (fieldsFromAncestor == null) {
        		fieldsFromAncestor = new LinkedList<AncestorFieldMapping>();
        	}
        	fieldsFromAncestor.add(ancestorFieldReference);
        	if (ancestorFieldReference.ancestor != null) {
        	ancestorFieldReference.ancestor.descendantReferences.put(ancestorFieldReference.ancestorField, null);
        	}
        	explicitCloverFields.add(ancestorFieldReference.currentField);
        }
        
		public List<AncestorFieldMapping> getFieldsFromAncestor() {
			return fieldsFromAncestor;
		}
		
		public boolean hasFieldsFromAncestor() {
			return fieldsFromAncestor != null && !fieldsFromAncestor.isEmpty(); 
		}
		
		private void addAncestorFieldMapping(String ancestorFieldRef, String currentField) {
			String ancestorField = ancestorFieldRef;
			ancestorField = normalizeAncestorValueRef(ancestorField);
			Mapping ancestor = this;
			while (ancestorField.startsWith(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR)) {
				ancestor = ancestor.getParent();
				if (ancestor == null) {
					// User may want this in template declaration
					logger.warn("Invalid ancestor XML field reference " + ancestorFieldRef + " in mapping of element <" + this.getElement() + ">"); 
					break;
				}
				ancestorField = ancestorField.substring(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR.length());
			}
			
			// After the ancestor prefix has been stripped, process the namespace
			ancestorField = createQualifiedName(ancestorField);
			if (ancestor != null) {
				addAncestorField(new AncestorFieldMapping(ancestor, ancestorField, currentField, ancestorFieldRef));
			} else {
				// This AncestorFieldMapping makes sense in templates - invalid ancestor reference may become valid in template reference
				addAncestorField(new AncestorFieldMapping(null, null, currentField, ancestorFieldRef));
			}
		}

		/**
		 * If <code>ancestorField</code> is reference to ancestor element value, returns its normalized
		 * version, otherwise returns unchanged original parameter.
		 * Normalized ancestor field reference always ends with "../.": suffix.
		 * Valid unnormalized ancestor element value references are i.e.: ".." or "../"
		 */
		private String normalizeAncestorValueRef(String ancestorField) {
			if (PARENT_MAPPING_REFERENCE_PREFIX.equals(ancestorField)) {
				return PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR + ELEMENT_VALUE_REFERENCE;
			}
			
			if (ancestorField.startsWith(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR)) {
				if (ancestorField.endsWith(PARENT_MAPPING_REFERENCE_PREFIX)) {
					ancestorField += PARENT_MAPPING_REFERENCE_SEPARATOR + ELEMENT_VALUE_REFERENCE;
				} else if (ancestorField.endsWith(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR)) {
					ancestorField += ELEMENT_VALUE_REFERENCE;
				}
			}
			return ancestorField;
		}
		
    }

	private static class AncestorFieldMapping {
		final Mapping ancestor;
		final String ancestorField;
		final String currentField;
		final String originalFieldReference;

		public AncestorFieldMapping(Mapping ancestor, String ancestorField, String currentField, String originalFieldReference) {
			this.ancestor = ancestor;
			this.ancestorField = ancestorField;
			this.currentField = currentField;
			this.originalFieldReference = originalFieldReference;
		}
	}
	
	
    /**
     * SAX Handler that will dispatch the elements to the different ports.
     */
    protected class SAXHandler extends SAXContentHandler {
        
        // depth of the element, used to determine when we hit the matching
        // close element
        private int m_level = 0;
        
        // flag set if we saw characters, otherwise don't save the column (used
        // to set null values)
        private boolean m_hasCharacters = false;
        //flag to skip text value immediately after end xml tag, for instance
        //<root>
        //	<subtag>text</subtag>
        //	another text
        //</root>
        //"another text" will be ignored
        private boolean m_grabCharacters = true;
        
        // buffer for node value
        private StringBuilder m_characters = new StringBuilder();
        
        // the active mapping
        private Mapping m_activeMapping = null;
        
        private Set<String> cloverAttributes;
        
		/**
		 * @param cloverAttributes
		 */
		public SAXHandler(Set<String> cloverAttributes) {
			super();
			this.cloverAttributes = cloverAttributes;
		}

		/**
         * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String, java.lang.String, org.xml.sax.Attributes)
         */
        @Override
        public void startElement(String namespaceURI, String localName, String qualifiedName, Attributes attributes) throws SAXException {
            m_level++;
            m_grabCharacters = true;
            
    		// store value of parent of currently starting element (if appropriate)
        	if (m_activeMapping != null && m_hasCharacters && m_level == m_activeMapping.getLevel() + 1) {
                if (m_activeMapping.descendantReferences.containsKey(ELEMENT_VALUE_REFERENCE)) {
               		m_activeMapping.descendantReferences.put(ELEMENT_VALUE_REFERENCE, trim ? m_characters.toString().trim() : m_characters.toString());
                }
        		processCharacters(null,null, true);
        	}
        	
            // Regardless of starting element type, reset the length of the buffer and flag
            m_characters.setLength(0);
            m_hasCharacters = false;
            
            final String universalName = augmentURI(namespaceURI) + localName; 
            Mapping mapping = null;
            if (m_activeMapping == null) {
                mapping = (Mapping) m_elementPortMap.get(universalName);
                
                // CL-2053 - backward compatibility (part 1/2)
                if (mapping == null) {
                	mapping = (Mapping) m_elementPortMap.get("{}" + localName);
                }
            } else if (useNestedNodes || m_activeMapping.getLevel() == m_level - 1) {
                mapping = (Mapping) m_activeMapping.getChildMapping(universalName);
                
                // CL-2053 - backward compatibility (part 2/2)
                if (mapping == null) {
                	mapping = (Mapping) m_activeMapping.getChildMapping("{}" + localName);
                }
            }
            if (mapping != null) {
                // We have a match, start converting all child nodes into
                // the DataRecord structure
                m_activeMapping = mapping;
                m_activeMapping.setLevel(m_level);
                // clear cached values of xml fields referenced by descendants (there may be values from previously read element of this m_activemapping)
                for (Entry<String, String> e : m_activeMapping.descendantReferences.entrySet()) {
					e.setValue(null);
				}
                
                if (mapping.getOutRecord() != null) {

	                //sequence fields initialization
	                String sequenceFieldName = m_activeMapping.getSequenceField();
	                if(sequenceFieldName != null && m_activeMapping.getOutRecord().hasField(sequenceFieldName)) {
	                    Sequence sequence = m_activeMapping.getSequence();
	                    DataField sequenceField = m_activeMapping.getOutRecord().getField(sequenceFieldName);
	                    if(sequenceField.getType() == DataFieldMetadata.INTEGER_FIELD) {
	                        sequenceField.setValue(sequence.nextValueInt());
	                    } else if(sequenceField.getType() == DataFieldMetadata.LONG_FIELD
	                            || sequenceField.getType() == DataFieldMetadata.DECIMAL_FIELD
	                            || sequenceField.getType() == DataFieldMetadata.NUMERIC_FIELD) {
	                        sequenceField.setValue(sequence.nextValueLong());
	                    } else {
	                        sequenceField.fromString(sequence.nextValueString());
	                    }
	                }
	               	m_activeMapping.prepareDoMap();
	               	m_activeMapping.incCurrentRecord4Mapping();
	                
	                // This is the closing element of the matched element that
	                // triggered the processing
	                // That should be the end of this record so send it off to the
	                // next Node
	                if (parentComponent.runIt()) {
	                    try {
	                        DataRecord outRecord = m_activeMapping.getOutRecord();
	                        String[] generatedKey = m_activeMapping.getGeneratedKey();
	                        String[] parentKey = m_activeMapping.getParentKey();
	                        if (parentKey != null) {
	                            //if generatedKey is a single array, all parent keys are concatenated into generatedKey field
	                            //I know it is ugly code...
	                            if(generatedKey.length != parentKey.length && generatedKey.length != 1) {
	                                logger.warn(parentComponent.getId() + ": XML Extract Mapping's generatedKey and parentKey attribute has different number of field.");
	                                m_activeMapping.setGeneratedKey(null);
	                                m_activeMapping.setParentKey(null);
	                            } else {
	                                for(int i = 0; i < parentKey.length; i++) {
	                                    boolean existGeneratedKeyField = (outRecord != null) 
	                                    			&& (generatedKey.length == 1 ? outRecord.hasField(generatedKey[0]) : outRecord.hasField(generatedKey[i]));
	                                    boolean existParentKeyField = m_activeMapping.getParent().getOutRecord() != null 
	                                    					&& m_activeMapping.getParent().getOutRecord().hasField(parentKey[i]);
	                                    if (!existGeneratedKeyField) {
	                                        logger.warn(parentComponent.getId() + ": XML Extract Mapping's generatedKey field was not found. generatedKey: "
	                                                + (generatedKey.length == 1 ? generatedKey[0] : generatedKey[i]) + " of element " + m_activeMapping.m_element + ", outPort: " + m_activeMapping.m_outPort);
	                                        m_activeMapping.setGeneratedKey(null);
	                                        m_activeMapping.setParentKey(null);
	                                    } else if (!existParentKeyField) {
	                                        logger.warn(parentComponent.getId() + ": XML Extract Mapping's parentKey field was not found. parentKey: " + parentKey[i] + " of element " + m_activeMapping.m_element + ", outPort: " + m_activeMapping.m_outPort);
	                                        m_activeMapping.setGeneratedKey(null);
	                                        m_activeMapping.setParentKey(null);
	                                    } else {
	                                    	// both outRecord and m_activeMapping.getParrent().getOutRecord are not null
	                                    	// here, because of if-else if-else chain
	                                        DataField generatedKeyField = generatedKey.length == 1 ? outRecord.getField(generatedKey[0]) : outRecord.getField(generatedKey[i]);
	                                        DataField parentKeyField = m_activeMapping.getParent().getOutRecord().getField(parentKey[i]);
	                                        if(generatedKey.length != parentKey.length) {
	                                            if(generatedKeyField.getType() != DataFieldMetadata.STRING_FIELD) {
	                                            	logger.warn(parentComponent.getId() + ": XML Extract Mapping's generatedKey field has to be String type (keys are concatened to this field).");
	                                                m_activeMapping.setGeneratedKey(null);
	                                                m_activeMapping.setParentKey(null);
	                                            } else {
	                                                ((StringDataField) generatedKeyField).append(parentKeyField.toString());
	                                            }
	                                        } else {
	                                            generatedKeyField.setValue(parentKeyField.getValue());
	                                        }
	                                    }
	                                }
	                            }
	                        }
	                    } catch (Exception ex) {
	                        throw new SAXException(" for output port number '" + m_activeMapping.getOutPort() + "'. Check also parent mapping. ", ex);
	                    }
	                    
	                    // Fill fields from parent record (if any mapped)
	                    if (m_activeMapping.hasFieldsFromAncestor()) {
	                    	for (AncestorFieldMapping afm : m_activeMapping.getFieldsFromAncestor()) {
	                    		if (m_activeMapping.getOutRecord().hasField(afm.currentField) && afm.ancestor != null) {
	                    			m_activeMapping.getOutRecord().getField(afm.currentField).fromString(afm.ancestor.descendantReferences.get(afm.ancestorField));
	                    		}
	                    	}
	                    }
	                } else {
	                    throw new SAXException("Stop Signaled");
	                }
                }
            }
            
            if (m_activeMapping != null //used only if we right now recognize new mapping element or if we want to use nested unmapped nodes as a source of data
                    && (useNestedNodes || mapping != null)) {
                // In a matched element (i.e. we are creating a DataRecord)
                // Store all attributes as columns (this hasn't been
                // used/tested)                
                for (int i = 0; i < attributes.getLength(); i++) {
                	/*
                	 * Note: XML namespaces handling
                	 * Default namespace declared via "xmlns" attribute without prefix specification (e.g. xmlns="http://www.javlin.eu/movies)
                	 * does NOT apply on attributes (see http://www.w3.org/TR/REC-xml-names/#defaulting)
                	 */
                	final String attributeLocalName = attributes.getLocalName(i);
                    String attrName = augmentURI(attributes.getURI(i)) + attributeLocalName;
                    
                    if (m_activeMapping.descendantReferences.containsKey(attrName)) {
                    	String val = attributes.getValue(i);
                    	m_activeMapping.descendantReferences.put(attrName, trim ? val.trim() : val);
                    }
                    
                    //use fields mapping
                    final Map<String, String> xmlCloverMap = m_activeMapping.getXml2CloverFieldsMap();
                    String fieldName = null;
                    if (xmlCloverMap != null) {
                    	if (xmlCloverMap.containsKey(attrName)) {
                    		fieldName = xmlCloverMap.get(attrName);
                    	} else if (m_activeMapping.explicitCloverFields.contains(attrName)) {
                    		continue; // don't do implicit mapping if clover field is used in an explicit mapping 
                    	}
                    }
                    
                    if (fieldName == null) {
                    	// we could not find mapping using the universal name -> try implicit mapping using local name
                    	fieldName = attributeLocalName;
                    }

                    // TODO Labels replace:
                    if (m_activeMapping.getOutRecord() != null && m_activeMapping.getOutRecord().hasField(fieldName)) {
                    	String val = attributes.getValue(i);
                        m_activeMapping.getOutRecord().getField(fieldName).fromString(trim ? val.trim() : val);
                    }
                }
            }
            
        }
        
        /**
         * @see org.xml.sax.ContentHandler#characters(char[], int, int)
         */
        @Override
		public void characters(char[] data, int offset, int length) throws SAXException {
            // Save the characters into the buffer, endElement will store it into the field
            if (m_activeMapping != null && m_grabCharacters) {
                m_characters.append(data, offset, length);
                m_hasCharacters = true;
            }
        }
        
        /**
         * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
         */
        @Override
        public void endElement(String namespaceURI, String localName, String qualifiedName) throws SAXException {
            if (m_activeMapping != null) {
            	String fullName = "{" + namespaceURI + "}" + localName;
            	
            	// cache characters value if the xml field is referenced by descendant
                if (m_level - 1 <= m_activeMapping.getLevel() && m_activeMapping.descendantReferences.containsKey(fullName)) {
               		m_activeMapping.descendantReferences.put(fullName, trim ? m_characters.toString().trim() : m_characters.toString());
                }
               	processCharacters(namespaceURI, localName, m_level == m_activeMapping.getLevel());
                
                // Regardless of whether this was saved, reset the length of the
                // buffer and flag
                m_characters.setLength(0);
                m_hasCharacters = false;
            }
            
            if (m_activeMapping != null && m_level == m_activeMapping.getLevel()) {
                // This is the closing element of the matched element that
                // triggered the processing
                // That should be the end of this record so send it off to the
                // next Node
                if (parentComponent.runIt()) {
                    try {
                        OutputPort outPort = getOutputPort(m_activeMapping.getOutPort());
                        
                        if (outPort != null) {
                            // we just ignore creating output, if port is empty (without metadata) or not specified
	                        DataRecord outRecord = m_activeMapping.getOutRecord();
	                        
	                        // skip or process row
	                    	if (skipRows > 0) {
	                    		if (m_activeMapping.getParent() == null) skipRows--;
	                    	} else {
	                            //check for index of last returned record
	                            if(!(numRecords >= 0 && numRecords == autoFilling.getGlobalCounter())) {
	                            	// set autofilling
	                                autoFilling.setAutoFillingFields(outRecord);
	                                
	                                // can I do the map? it depends on skip and numRecords.
	                                if (m_activeMapping.doMap()) {
		                                //send off record
	                                	outPort.writeRecord(outRecord);
	                                }
//	                                if (m_activeMapping.getParent() == null) autoFilling.incGlobalCounter();
	                            }
	                    	}
	                    	
	                    	// resets all child's mappings for skip and numRecords 
                           	m_activeMapping.resetCurrentRecord4ChildMapping();

                        	// reset record
	                        outRecord.reset();
                        }
                       
                        m_activeMapping = m_activeMapping.getParent();
                    } catch (Exception ex) {
                        throw new SAXException(ex);
                    }
                } else {
                    throw new SAXException("Stop Signaled");
                }
            }
            
            //text value immediately after end tag element should not be stored
            m_grabCharacters = false;
            
            //ended an element so decrease our depth
            m_level--; 
        }

		/**
		 * Store the characters processed by the characters() call back only if we have corresponding 
		 * output field and we are on the right level or we want to use data from nested unmapped nodes
		 */
		private void processCharacters(String namespaceURI, String localName, boolean elementValue) {
        	// Create universal name
			String universalName = null;
			if (localName != null) {
				universalName = augmentURI(namespaceURI) + localName;
			}
			
			String fieldName = null;
        	//use fields mapping
            Map<String, String> xml2clover = m_activeMapping.getXml2CloverFieldsMap();
            if (xml2clover != null) {
           		if (elementValue && xml2clover.containsKey(ELEMENT_VALUE_REFERENCE)) {
            		fieldName = xml2clover.get(ELEMENT_VALUE_REFERENCE);
        		} else if (xml2clover.containsKey(universalName)) {
            		fieldName = xml2clover.get(universalName);
        		} else if (m_activeMapping.explicitCloverFields.contains(localName)) {
        			// XXX: this is nonsense code ... the names stored here are field names and the code used XML element names
        			return; // don't do implicit mapping if clover field is used in an explicit mapping
        		}

           		if (fieldName == null) {
           			/*
           			 * As we could not find match using qualified name
           			 * try mapping the xml element/attribute without the namespace prefix
           			 */
           			fieldName = localName;
           		}
            }
            
			// TODO Labels replace:
			if (m_activeMapping.getOutRecord() != null && m_activeMapping.getOutRecord().hasField(fieldName) 
			        && (useNestedNodes || m_level - 1 <= m_activeMapping.getLevel())) {
			    DataField field = m_activeMapping.getOutRecord().getField(fieldName);
			    // If field is nullable and there's no character data set it to null
			    if (m_hasCharacters) {
			        try {
			    	if (field.getValue() != null && cloverAttributes.contains(fieldName)) {
			    		field.fromString(trim ? field.getValue().toString().trim() : field.getValue().toString());
			    	} else {
			    		field.fromString(trim ? m_characters.toString().trim() : m_characters.toString());
			    	}
			        } catch (BadDataFormatException ex) {
			            // This is a bit hacky here SOOO let me explain...
			            if (field.getType() == DataFieldMetadata.DATE_FIELD) {
			                // XML dateTime format is not supported by the
			                // DateFormat oject that clover uses...
			                // so timezones are unparsable
			                // i.e. XML wants -5:00 but DateFormat wants
			                // -500
			                // Attempt to munge and retry... (there has to
			                // be a better way)
			                try {
			                    // Chop off the ":" in the timezone (it HAS
			                    // to be at the end)
			                    String dateTime = m_characters.substring(0,
			                            m_characters.lastIndexOf(":"))
			                            + m_characters
			                            .substring(m_characters
			                            .lastIndexOf(":") + 1);
			                    DateFormatter formatter = field.getMetadata().createDateFormatter();
			                    field.setValue(formatter.parseDate(trim ? dateTime.trim() : dateTime));
			                } catch (Exception ex2) {
			                    // Oh well we tried, throw the originating
			                    // exception
			                    throw ex;
			                }
			            } else {
			                throw ex;
			            }
			        }
			    } else if (field.getType() == DataFieldMetadata.STRING_FIELD 
			    		// and value wasn't already stored (from characters)
			    		&& (field.getValue() == null || field.getValue().equals(field.getMetadata().getDefaultValueStr()))) {
			    	field.setValue("");
			    }
			}
		}
    }

    /**
     * Augments the namespaceURIs with curly brackets to allow easy creation of qualified names
     * E.g. xmlns:mov="http://www.javlin.eu/mov"
     * URI = "http://www.javlin.eu";
     * Augmented URI = "{http://www.javlin.eu}";
     */
    protected void augmentNamespaceURIs() {
    	for (String prefix : namespaceBindings.keySet()) {
    		String uri = namespaceBindings.get(prefix);
    		namespaceBindings.put(prefix, augmentURI(uri));
    	}
    }
    
	/**
	 * Expands a prefixed element or attribute name to a universal name.
	 * I.e. the namespace prefix is replaced by augmented URI.
	 * The URIs are taken from the {@link XMLExtract#namespaceBindings} 
	 * 
	 * @param prefixedName XML element or attribute name e.g. <code>mov:movies</code>
	 * 
	 * @return Universal XML name in the form: <code>{http://www.javlin.eu/movies}title</code>
	 */
	private String createQualifiedName(String prefixedName) {
		if (prefixedName == null || prefixedName.isEmpty()) {
			return prefixedName;
		}
		
		// check if universal XML name exists
		int indexOfOpenBracket = prefixedName.indexOf("{");
		if (-1<indexOfOpenBracket && indexOfOpenBracket<prefixedName.indexOf("}")) {
			return prefixedName;
		}
		
		final String[] parsed = prefixedName.split(":");
		
		if (parsed.length < 2) {
			return "{}" + parsed[0];
		}
		
		/*
		 * Prefixed element:
		 * Get the URI (already in Clark's notation) and use it to create qualified name
		 */
		String namespaceURI = namespaceBindings.get(parsed[0]);
		namespaceURI = namespaceURI == null ? "{}" : namespaceURI;
		return namespaceURI + parsed[1];
	}

    private String augmentURI(String uri) {
    	if (uri == null) {
    		return null;
    	}
    	
    	return "{" + uri + "}";
    }

    public static class MyHandler extends DefaultHandler { 
		//Handler used at checkConfig to parse XML mapping and retrieve attributes names
		private Set<String> attributeNames = new HashSet<String>();
		private Set<String> cloverAttributes = new HashSet<String>();
		
		@Override
		public void startElement(String namespaceURI, String localName, String qName, Attributes atts) { 
			int length = atts.getLength(); 
			for (int i=0; i<length; i++) {
				String xmlField = atts.getQName(i);
				attributeNames.add(xmlField);
				if (xmlField.equals("cloverFields")) {
					cloverAttributes.add(atts.getValue(i));
				}
			}
		}
		
		public Set<String> getAttributeNames() {
			return attributeNames;
		}
		
		public Set<String> getCloverAttributes() {
			return cloverAttributes;
		}
	}
    
	/**
	 * @return the declaredTemplates
	 */
	public TreeMap<String, Mapping> getDeclaredTemplates() {
		return declaredTemplates;
	}
	
	/**
	 * Xml features initialization.
	 * 
	 * @throws JetelException
	 */
	private void initXmlFeatures(SAXParserFactory factory, String xmlFeatures) throws ComponentNotReadyException {
		if (xmlFeatures == null) {
			return;
		}
		
		String[] aXmlFeatures = xmlFeatures.split(FEATURES_DELIMETER);
		String[] aOneFeature;
		try {
			for (String oneFeature : aXmlFeatures) {
				aOneFeature = oneFeature.split(FEATURES_ASSIGN);
				if (aOneFeature.length != 2)
					throw new JetelException("The xml feature '" + oneFeature + "' has wrong format");
				factory.setFeature(aOneFeature[0], Boolean.parseBoolean(aOneFeature[1]));
			}
		} catch (Exception e) {
			throw new ComponentNotReadyException(e);
		}
	}
	
	private Set<String> getXMLMappingValues() {
		try {
			SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
			DefaultHandler handler = new MyHandler();
			InputStream is = null;
			if (this.mappingURL != null) {
				String filePath = FileUtils.getFile(graph.getRuntimeContext().getContextURL(), mappingURL);
				is = new FileInputStream(new File(filePath));
			} else if (this.mapping != null) {
				is = new ByteArrayInputStream(mapping.getBytes("UTF-8"));
			}
			if (is != null) {
				saxParser.parse(is, handler);
				return ((MyHandler) handler).getCloverAttributes();
			}
		} catch (Exception e) {
			return new HashSet<String>();
		}
		return new HashSet<String>();
	}
	
	/**
     * Sets namespace bindings to allow processing that relate namespace prefix used in Mapping
     * and namespace URI used in processed XML document
	 * @param namespaceBindings the namespaceBindings to set
	 */
	public void setNamespaceBindings(HashMap<String, String> namespaceBindings) {
		this.namespaceBindings = namespaceBindings;
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
	
}
