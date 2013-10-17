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
import java.net.URL;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.io.SAXContentHandler;
import org.jetel.component.RecordTransform;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.StringDataField;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.xml.mapping.XMLMappingConstants;
import org.jetel.data.xml.mapping.XMLMappingContext;
import org.jetel.data.xml.mapping.parser.XMLMappingDefinitionParseException;
import org.jetel.data.xml.mapping.parser.XMLMappingDefinitionParser;
import org.jetel.data.xml.mapping.parser.XMLMappingDefinitionParser.XMLMappingParseResult;
import org.jetel.data.xml.mapping.runtime.XMLElementRuntimeMappingModel;
import org.jetel.data.xml.mapping.runtime.XMLElementRuntimeMappingModel.AncestorFieldMapping;
import org.jetel.data.xml.mapping.runtime.factory.RuntimeMappingModelFactory;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.TransformException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.XmlUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.formatter.DateFormatter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author mlaska (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jan 9, 2012
 */
/*
 * Invariant: Mappings are preprocessed so that all element and attribute references stored in internal structures use
 * universal names : element="mov:movies" -> element="{http://www.javlin.eu}movies" xmlFields="mov:att1" ->
 * xmlFields="{http://www.javlin.eu}att1"
 * 
 * Default namespace in mapping -> NOT POSSIBLE We are not able to implement default namespace functionality. Default
 * namespace (declared without prefix e.g. xmlns="http://www.javlin.eu/default-ns") does NOT apply to attributes!!! (see
 * www.w3.org/TR/REC-xml-names/#defaulting) Since we currently do not know which xmlFields are attributes and which are
 * elements -> so we do not know which should be expanded by default namespace. User therefore has to declare explicit
 * namespace prefix in the Mapping, even for attributes and elements falling into the default namespace in the processed
 * XML document.
 */
public class XmlSaxParser {

	private static final String FEATURES_DELIMETER = ";";
	private static final String FEATURES_ASSIGN = ":=";

	public static final String XML_USE_PARENT_RECORD = "useParentRecord";
	public static final String XML_IMPLICIT = "implicit";
	public static final String XML_ELEMENT = "element";
	public static final String XML_OUTPORT = "outPort";
	public static final String XML_PARENTKEY = "parentKey";
	public static final String XML_GENERATEDKEY = "generatedKey";
	public static final String XML_XMLFIELDS = "xmlFields";
	public static final String XML_INPUTFIELDS = "inputFields";
	public static final String XML_INPUTFIELD = "inputField";
	public static final String XML_OUTPUTFIELD = "outputField";
	public static final String XML_CLOVERFIELDS = "cloverFields";
	public static final String XML_SEQUENCEFIELD = "sequenceField";
	public static final String XML_SEQUENCEID = "sequenceId";

	public static final String XML_TEMPLATE_ID = "templateId";
	public static final String XML_TEMPLATE_REF = "templateRef";
	public static final String XML_TEMPLATE_DEPTH = "nestedDepth";

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
	protected Map<String, XMLElementRuntimeMappingModel> m_elementPortMap = new HashMap<String, XMLElementRuntimeMappingModel>();
	// protected TreeMap<String, XmlElementMapping> declaredTemplates = new TreeMap<String, XmlElementMapping>();

	private DataRecord inputRecord;

	/**
	 * Namespace bindings relate namespace prefix used in Mapping specification and the namespace URI used by the
	 * namespace declaration in processed XML document
	 */
	private HashMap<String, String> namespaceBindings = new HashMap<String, String>();

	// global skip and numRecords
	private int skipRows = 0; // do not skip rows by default
	private int numRecords = -1;

	private AutoFilling autoFilling = new AutoFilling();

	protected SAXParser parser;
	protected SAXHandler saxHandler;

	private NodeList mappingNodes;

	public XmlSaxParser(TransformationGraph graph, Node parentComponent) {
		this(graph, parentComponent,(String)null);
	}

	public XmlSaxParser(TransformationGraph graph, Node parentComponent, String mapping) {
		this.graph = graph;
		this.parentComponent = parentComponent;
		this.mapping = mapping;
	}
	
	public XmlSaxParser(TransformationGraph graph, Node parentComponent, SAXParser saxParser){
		this(graph, parentComponent,(String)null);
		this.parser = saxParser;
	}

	public void init() throws ComponentNotReadyException {
		augmentNamespaceURIs();

		URL projectURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;

		// prepare mapping
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
		// declaredTemplates.clear();
		String errorPrefix = parentComponent.getId() + ": Mapping error - ";
		for (int i = 0; i < mappingNodes.getLength(); i++) {
			org.w3c.dom.Node node = mappingNodes.item(i);
			List<String> errors = processMappings(graph, node);
			for (String error : errors) {
				logger.warn(errorPrefix + error);
			}
		}

		if (m_elementPortMap.size() < 1) {
			throw new ComponentNotReadyException(parentComponent.getId() + ": At least one mapping has to be defined. Absence of mapping can be caused by invalid mapping definition. Check for warnings in log above. Mapping example: <Mapping element=\"elementToMatch\" outPort=\"123\" [parentKey=\"key in parent\" generatedKey=\"new foreign key in target\"]/>");
		}

		// create new sax factory & parser (if not already passed in)
		if (parser == null) {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			factory.setNamespaceAware(true);
			factory.setValidating(validate);
			initXmlFeatures(factory, xmlFeatures); // TODO:
			try {
				parser = factory.newSAXParser();
			} catch (Exception ex) {
				throw new ComponentNotReadyException(ex);
			}
		}
		saxHandler = new SAXHandler(getXmlElementMappingValues());
		try {
			// this is required to handle events for CDATA sections, processing instructions and entities
			parser.setProperty("http://xml.org/sax/properties/lexical-handler", saxHandler);
		} catch (SAXNotRecognizedException e) {
			throw new ComponentNotReadyException("Lexical Handler not supported", e);
		} catch (SAXNotSupportedException e) {
			throw new ComponentNotReadyException("Lexical Handler not supported", e);
		}
	}

	public void reset() {

	}
	
	/**
	 * Set external SAX parser to be used for parsing XML data instead of the defaul one.
	 * 
	 * @param parser a SAX parser
	 */
	public void setParser(SAXParser parser){
		this.parser=parser;
	}

	public void parse(InputSource inputSource) throws JetelException, SAXException {
		try {
			parser.parse(inputSource, saxHandler);
		} catch (IOException e) {
			// reasonable error reporting - get record number on which parsing. This should likely be implemented on
			// Node level!
			throw new JetelException(getParseErrorMessage(), e);
		} catch (SAXException e) {
			// reasonable error reporting - get record number on which parsing. This should likely be implemented on
			// Node level!
			logger.error(getParseErrorMessage());
			throw e;
		}
	}

	private String getParseErrorMessage() {
		String message = "Unexpected XML parsing exception";
		InputPort inputPort = parentComponent.getInputPort(0);
		if (inputPort != null) {
			message += " on record #" + inputPort.getInputRecordCounter();
		}
		return message;
	}

	protected OutputPort getOutputPort(int outPortIndex) {
		return parentComponent.getOutputPort(outPortIndex);
	}

	protected XMLMappingContext createMappingContext() {
		XMLMappingContext context = new XMLMappingContext();

		context.setAutoFilling(autoFilling);
		context.setGraph(graph);
		context.setParentComponent(parentComponent);
		context.setNamespaceBindings(namespaceBindings);

		return context;
	}

	/**
	 * Parses a mapping definition and creates a runtime mapping model.
	 * 
	 * @param graph
	 *            - graph to be used as a mapping context
	 * @param nodeXML
	 *            - XML to be parsed
	 * @return list of error messages
	 */
	public List<String> processMappings(TransformationGraph graph, org.w3c.dom.Node nodeXML) {
		if (nodeXML.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE) {

			XMLMappingContext context = createMappingContext();
			context.setGraph(graph);

			// parse the mapping definition
			XMLMappingDefinitionParser mappingParser = new XMLMappingDefinitionParser(context);
			mappingParser.init();

			XMLMappingParseResult<?> mappingResult;
			try {
				mappingResult = mappingParser.parseMapping(nodeXML);
			} catch (XMLMappingDefinitionParseException e) {
				return Collections.singletonList(ExceptionUtils.getMessage(e));
			}

			if (!mappingResult.isSuccessful()) {
				return mappingResult.getErrors();
			}

			RuntimeMappingModelFactory factory = new RuntimeMappingModelFactory(context);

			// build runtime mapping model
			addMapping(factory.createRuntimeMappingModel(mappingResult.getMapping()));
		}

		return Collections.emptyList();
	}

	private static int performMapping(RecordTransform transformation, DataRecord inRecord, DataRecord outRecord,
			String errMessage) {
		try {
			return transformation.transform(new DataRecord[] { inRecord }, new DataRecord[] { outRecord });
		} catch (Exception exception) {
			try {
				return transformation.transformOnError(exception, new DataRecord[] { inRecord }, new DataRecord[] { outRecord });
			} catch (TransformException e) {
				throw new JetelRuntimeException(errMessage, e);
			}
		}
	}

	public void applyInputFieldTransformation(RecordTransform transformation, DataRecord record) {
		// no input available
		if (inputRecord == null) {
			return;
		}

		performMapping(transformation, inputRecord, record, "Input field transformation failed");
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
	 * 
	 * @param skipRows
	 */
	public void setSkipRows(int skipRows) {
		this.skipRows = skipRows;
	}

	/**
	 * Sets numRecords - how many elements to process.
	 * 
	 * @param numRecords
	 */
	public void setNumRecords(int numRecords) {
		this.numRecords = Math.max(numRecords, 0);
	}

	/**
	 * Accessor to add a mapping programatically.
	 */
	public void addMapping(XMLElementRuntimeMappingModel mapping) {
		m_elementPortMap.put(mapping.getElementName(), mapping);
	}

	/**
	 * SAX Handler that will dispatch the elements to the different ports.
	 */
	protected class SAXHandler extends SAXContentHandler {

		private String CHARS_CDATA_START = "<!CDATA[";
		private String CHARS_CDATA_END = "]]>";

		// depth of the element, used to determine when we hit the matching
		// close element
		private int m_level = 0;

		// flag set if we saw characters, otherwise don't save the column (used
		// to set null values)
		private boolean m_hasCharacters = false;
		// flag to skip text value immediately after end xml tag, for instance
		// <root>
		// <subtag>text</subtag>
		// another text
		// </root>
		// "another text" will be ignored
		private boolean m_grabCharacters = true;

		// buffer for node value
		private StringBuilder m_characters = new StringBuilder();

		private List<CharacterBufferMarker> m_elementContentStartIndexStack = new ArrayList<CharacterBufferMarker>();
		// the active mapping
		private XMLElementRuntimeMappingModel m_activeMapping = null;

		private Set<String> cloverAttributes;

		/**
		 * This is needed to know whether to escape entities in output characters or not. Entities in CDATA should not
		 * be escaped while entities in attribute values or characters should be escaped.
		 */
		private boolean in_cdata = false;

		/**
		 * Status flag to know whether the subtree should be concatenated into m_characters. It is also used to identify
		 * when to generate output.
		 */
		private boolean m_element_as_text = false;

		/**
		 * @param cloverAttributes
		 */
		public SAXHandler(Set<String> cloverAttributes) {
			super();
			this.cloverAttributes = cloverAttributes;
		}

		/**
		 * @see org.xml.sax.ContentHandler#startElement(java.lang.String, java.lang.String, java.lang.String,
		 *      org.xml.sax.Attributes)
		 */
		@Override
		public void startElement(String namespaceURI, String localName, String qualifiedName, Attributes attributes)
				throws SAXException {
			m_level++;
			m_grabCharacters = true;

			// store value of parent of currently starting element (if appropriate)
			if (m_activeMapping != null && m_hasCharacters && m_level == m_activeMapping.getLevel() + 1) {
				if (m_activeMapping.getDescendantReferences().containsKey(XMLMappingConstants.ELEMENT_VALUE_REFERENCE)) {
					m_activeMapping.getDescendantReferences().put(XMLMappingConstants.ELEMENT_VALUE_REFERENCE, getCurrentValue());
				}
				if (!m_activeMapping.isCharactersProcessed()) {
					processCharacters(null, null, true);
					m_activeMapping.setCharactersProcessed(true);
					//clear all character markers - all should be processed or we do not need them
					for(int i=this.m_elementContentStartIndexStack.size()-1;i>=0; i--) {
						if(this.m_elementContentStartIndexStack.get(i).type == CharacterBufferMarkerType.CHARACTERS_START ||
								this.m_elementContentStartIndexStack.get(i).type == CharacterBufferMarkerType.CHARACTERS_END) {
							m_elementContentStartIndexStack.remove(i);
						}
					}
					
				}
			}

			final String universalName = augmentURI(namespaceURI) + localName;
			XMLElementRuntimeMappingModel mapping = null;
			if (m_activeMapping == null) {
				mapping = (XMLElementRuntimeMappingModel) m_elementPortMap.get(universalName);

				// CL-2053 - backward compatibility (part 1/2)
				if (mapping == null) {
					mapping = (XMLElementRuntimeMappingModel) m_elementPortMap.get("{}" + localName);
				}
			} else if (useNestedNodes || m_activeMapping.getLevel() == m_level - 1) {
				mapping = (XMLElementRuntimeMappingModel) m_activeMapping.getChildMapping(universalName);

				// CL-2053 - backward compatibility (part 2/2)
				if (mapping == null) {
					mapping = (XMLElementRuntimeMappingModel) m_activeMapping.getChildMapping("{}" + localName);
				}
			}
			if (mapping != null) {
				// We have a match, start converting all child nodes into
				// the DataRecord structure
				m_activeMapping = mapping;
				m_activeMapping.setLevel(m_level);
				m_activeMapping.setCharactersProcessed(false);
				// clear cached values of xml fields referenced by descendants (there may be values from previously read
				// element of this m_activemapping)
				for (Entry<String, String> e : m_activeMapping.getDescendantReferences().entrySet()) {
					e.setValue(null);
				}

				if (mapping.getOutputRecord() != null) {

					if (mapping.getFieldTransformation() != null) {
						applyInputFieldTransformation(mapping.getFieldTransformation(), mapping.getOutputRecord());
					}

					// sequence fields initialization
					String sequenceFieldName = m_activeMapping.getSequenceField();
					if (sequenceFieldName != null && m_activeMapping.getOutputRecord().hasField(sequenceFieldName)) {
						Sequence sequence = m_activeMapping.getSequence();
						DataField sequenceField = m_activeMapping.getOutputRecord().getField(sequenceFieldName);
						if (sequenceField.getType() == DataFieldMetadata.INTEGER_FIELD) {
							sequenceField.setValue(sequence.nextValueInt());
						} else if (sequenceField.getType() == DataFieldMetadata.LONG_FIELD || sequenceField.getType() == DataFieldMetadata.DECIMAL_FIELD || sequenceField.getType() == DataFieldMetadata.NUMERIC_FIELD) {
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
							DataRecord outRecord = m_activeMapping.getOutputRecord();
							String[] generatedKey = m_activeMapping.getGeneratedKeyFields();
							String[] parentKey = m_activeMapping.getParentKeyFields();
							if (parentKey != null) {
								// if generatedKey is a single array, all parent keys are concatenated into generatedKey
								// field
								// I know it is ugly code...
								if (generatedKey.length != parentKey.length && generatedKey.length != 1) {
									logger.warn(parentComponent.getId() + ": XML Extract Mapping's generatedKey and parentKey attribute has different number of field.");
									m_activeMapping.setGeneratedKeyFields(null);
									m_activeMapping.setParentKeyFields(null);
								} else {
									XMLElementRuntimeMappingModel parentKeyFieldsMapping = m_activeMapping.getParent();
									while (parentKeyFieldsMapping != null && parentKeyFieldsMapping.getOutputRecord() == null) {
										parentKeyFieldsMapping = parentKeyFieldsMapping.getParent();
									}
									for (int i = 0; i < parentKey.length; i++) {
										boolean existGeneratedKeyField = (outRecord != null) && (generatedKey.length == 1 ? outRecord.hasField(generatedKey[0]) : outRecord.hasField(generatedKey[i]));
										boolean existParentKeyField = parentKeyFieldsMapping != null && parentKeyFieldsMapping.getOutputRecord().hasField(parentKey[i]);
										if (!existGeneratedKeyField) {
											logger.warn(parentComponent.getId() + ": XML Extract Mapping's generatedKey field was not found. generatedKey: " + (generatedKey.length == 1 ? generatedKey[0] : generatedKey[i]) + " of element " + m_activeMapping.getElementName() + ", outPort: " + m_activeMapping.getOutputPortNumber());
											m_activeMapping.setGeneratedKeyFields(null);
											m_activeMapping.setParentKeyFields(null);
										} else if (!existParentKeyField) {
											logger.warn(parentComponent.getId() + ": XML Extract Mapping's parentKey field was not found. parentKey: " + parentKey[i] + " of element " + m_activeMapping.getElementName() + ", outPort: " + m_activeMapping.getOutputPortNumber());
											m_activeMapping.setGeneratedKeyFields(null);
											m_activeMapping.setParentKeyFields(null);
										} else {
											// both outRecord and m_activeMapping.getParrent().getOutRecord are not null
											// here, because of if-else if-else chain
											DataField generatedKeyField = generatedKey.length == 1 ? outRecord.getField(generatedKey[0]) : outRecord.getField(generatedKey[i]);
											DataField parentKeyField = parentKeyFieldsMapping.getOutputRecord().getField(parentKey[i]);
											if (generatedKey.length != parentKey.length) {
												if (generatedKeyField.getType() != DataFieldMetadata.STRING_FIELD) {
													logger.warn(parentComponent.getId() + ": XML Extract Mapping's generatedKey field has to be String type (keys are concatened to this field).");
													m_activeMapping.setGeneratedKeyFields(null);
													m_activeMapping.setParentKeyFields(null);
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
							String portNumber = "" + m_activeMapping.getOutputPortNumber();
							if(m_activeMapping.isUsingParentRecord()) {
								portNumber = "" + m_activeMapping.getProducingParent().getOutputPortNumber();
							}
							
							throw new SAXException(" for output port number '" + portNumber + "' element '"+m_activeMapping.getElementName()+"'. Check also parent mapping. ", ex);
						}

						// Fill fields from parent record (if any mapped)
						if (m_activeMapping.hasFieldsFromAncestor()) {
							for (AncestorFieldMapping afm : m_activeMapping.getFieldsFromAncestor()) {
								if (m_activeMapping.getOutputRecord().hasField(afm.getCurrentField()) && afm.getAncestor() != null) {
									m_activeMapping.getOutputRecord().getField(afm.getCurrentField()).fromString(afm.getAncestor().getDescendantReferences().get(afm.getAncestorField()));
								}
							}
						}
					} else {
						throw new SAXException("Stop Signaled");
					}
				}
			}


			boolean grabElement = false;
			
			// Extract Element As Text - append element name; only when whole element should be extracted (including
			// tags) or when we are already within the element
			if ((m_element_as_text && (m_activeMapping==null || m_level >= m_activeMapping.getLevel())) || (m_activeMapping!=null &&  m_activeMapping.getFieldsMap().containsKey(XMLMappingConstants.ELEMENT_AS_TEXT) && m_level == m_activeMapping.getLevel())) {
				//logger.trace("startElement(" + qualifiedName + "): storing element name; m_element_as_text");
				// Starting new subtree mapping
				if (m_activeMapping!=null  && m_activeMapping.getFieldsMap().containsKey(XMLMappingConstants.ELEMENT_AS_TEXT) && m_level == m_activeMapping.getLevel()) {
					this.m_elementContentStartIndexStack.add(new CharacterBufferMarker(CharacterBufferMarkerType.SUBTREE_WITH_TAG_START, m_characters.length(), m_level));
				};
				m_characters.append("<").append(localName);
				grabElement = true;
			}

			// Extract Element As Text - start mapping of the subtree as a text
			// may be not in the right place - fix
			if (m_activeMapping != null && (m_activeMapping.getFieldsMap().containsKey(XMLMappingConstants.ELEMENT_CONTENTS_AS_TEXT) || m_activeMapping.getFieldsMap().containsKey(XMLMappingConstants.ELEMENT_AS_TEXT))) {
				m_element_as_text = true;
				m_hasCharacters = true;
			} else if (!m_element_as_text) {
				// Regardless of starting element type, reset the length of the buffer and flag
			//	logger.trace("startElement(" + qualifiedName + "): cleaning m_characters; !m_element_as_text");
				m_characters.setLength(0);
				this.m_elementContentStartIndexStack.clear();

				m_hasCharacters = false;
			}

			if (m_activeMapping != null // used only if we right now recognize new mapping element or if we want to use
										// nested unmapped nodes as a source of data
					&& (useNestedNodes || mapping != null)) {
				// In a matched element (i.e. we are creating a DataRecord)
				// Store all attributes as columns (this hasn't been
				// used/tested)
				for (int i = 0; i < attributes.getLength(); i++) {
					/*
					 * Note: XML namespaces handling Default namespace declared via "xmlns" attribute without prefix
					 * specification (e.g. xmlns="http://www.javlin.eu/movies) does NOT apply on attributes (see
					 * http://www.w3.org/TR/REC-xml-names/#defaulting)
					 */
					final String attributeLocalName = attributes.getLocalName(i);
					String attrName = augmentURI(attributes.getURI(i)) + attributeLocalName;

					if (m_activeMapping.getDescendantReferences().containsKey(attrName)) {
						String val = attributes.getValue(i);
						m_activeMapping.getDescendantReferences().put(attrName, trim ? val.trim() : val);
					}

					// use fields mapping
					final Map<String, String> xmlCloverMap = m_activeMapping.getFieldsMap();
					String fieldName = null;
					if (xmlCloverMap != null) {
						if (xmlCloverMap.containsKey(attrName)) {
							fieldName = xmlCloverMap.get(attrName);
						} else if (m_activeMapping.getExplicitCloverFields().contains(attrName)) {
							continue; // don't do implicit mapping if clover field is used in an explicit mapping
						} else if (m_activeMapping.getSequenceField()!=null && (m_activeMapping.getSequenceField().equals(attributeLocalName)||m_activeMapping.getSequenceField().equals(attrName))) {
							continue; //don't do implicit mapping for fields mapped to sequence
						}
					}

					if (fieldName == null && !m_activeMapping.isImplicit()) {
						continue;
					}

					if (fieldName == null) {
						// we could not find mapping using the universal name -> try implicit mapping using local name
						fieldName = attributeLocalName;
					}

					// TODO Labels replace:
					if (m_activeMapping.getOutputRecord() != null && m_activeMapping.getOutputRecord().hasField(fieldName)) {
						String val = attributes.getValue(i);
						m_activeMapping.getOutputRecord().getField(fieldName).fromString(trim ? val.trim() : val);
					}
				}
			}

			// Extract Element As Text - append all attributes and terminate the start tag; only when whole element
			// should be extracted (including tags) or when we are already within the element
			if (grabElement) {
				for (int i = 0; i < attributes.getLength(); i++) {
					m_characters.append(" ").append(attributes.getLocalName(i)).append("=\"").append(escapeXmlEntity(attributes.getValue(i))).append("\"");
				}
				m_characters.append(">");
			}
			if (m_element_as_text && m_activeMapping.getFieldsMap().containsKey(XMLMappingConstants.ELEMENT_CONTENTS_AS_TEXT) && m_level == m_activeMapping.getLevel()) {
				this.m_elementContentStartIndexStack.add(new CharacterBufferMarker(CharacterBufferMarkerType.SUBTREE_START, m_characters.length(), m_level));
			}

		}
		
		/**
		 * @see org.xml.sax.ContentHandler#characters(char[], int, int)
		 */
		@Override
		public void characters(char[] data, int offset, int length) throws SAXException {
			// Save the characters into the buffer, endElement will store it into the field
			//logger.trace("characters '" + new String(data, offset, length) + "'");
			if (m_activeMapping != null && m_grabCharacters) {
				if(m_element_as_text) {
					if (m_elementContentStartIndexStack.size() > 0 && m_elementContentStartIndexStack.get(m_elementContentStartIndexStack.size() - 1).type == CharacterBufferMarkerType.CHARACTERS_END && m_elementContentStartIndexStack.get(m_elementContentStartIndexStack.size() - 1).index == m_characters.length()) {
						m_elementContentStartIndexStack.remove(m_elementContentStartIndexStack.size() - 1);
					} else {
						m_elementContentStartIndexStack.add(new CharacterBufferMarker(CharacterBufferMarkerType.CHARACTERS_START, m_characters.length(), m_level));
					}
				}

				if (m_element_as_text && !in_cdata) {
					// perform entity escaping only when extracting element as text since the output should still be
					// valid XML.
					m_characters.append(escapeXmlEntity(new String(data, offset, length)));
				} else {
					m_characters.append(data, offset, length);
				}
				if(m_element_as_text) {
					m_elementContentStartIndexStack.add(new CharacterBufferMarker(CharacterBufferMarkerType.CHARACTERS_END, m_characters.length(), m_level));
				}
				m_hasCharacters = true;
			}
		}

		private String escapeXmlEntity(String entity) {
			return entity.replace("&", "&amp;").replace("\"", "&quot").replace("'", "&apos;").replace("<", "&lt;").replace(">", "&gt;");
		}

		/**
		 * @see org.xml.sax.ContentHandler#endElement(java.lang.String, java.lang.String, java.lang.String)
		 */
		@Override
		public void endElement(String namespaceURI, String localName, String qualifiedName) throws SAXException {
			if (m_activeMapping != null) {
				String fullName = "{" + namespaceURI + "}" + localName;

				// cache characters value if the xml field is referenced by descendant
				if (m_level - 1 <= m_activeMapping.getLevel() && m_activeMapping.getDescendantReferences().containsKey(fullName)) {
					m_activeMapping.getDescendantReferences().put(fullName, getCurrentValue());
				}
				if (m_element_as_text && m_activeMapping.getFieldsMap().containsKey(XMLMappingConstants.ELEMENT_CONTENTS_AS_TEXT) && m_level == m_activeMapping.getLevel()) {
					this.m_elementContentStartIndexStack.add(new CharacterBufferMarker(CharacterBufferMarkerType.SUBTREE_END, m_characters.length(), m_level));
				}

				// Extract Element As Text - append element's end tag; only when whole element should be extracted
				// (including tags) or when we are already within the element
				if (m_element_as_text && (m_activeMapping.getFieldsMap().containsKey(XMLMappingConstants.ELEMENT_AS_TEXT) || m_level >= m_activeMapping.getLevel())) {
					//logger.trace("endElement(" + qualifiedName + "): storing element name; m_element_as_text");
					m_characters.append("</").append(localName).append(">");
				}

				// if we are finishing the mapping, check for the mapping on this element through parent
				if (m_activeMapping != null && m_level == m_activeMapping.getLevel()) {
					if (m_activeMapping.hasFieldsFromAncestor()) {
						for (AncestorFieldMapping afm : m_activeMapping.getFieldsFromAncestor()) {
							if (afm.getAncestor() == m_activeMapping.getParent() && m_activeMapping.getOutputRecord() != null && m_activeMapping.getOutputRecord().hasField(afm.getCurrentField()) && afm.getAncestor() != null && afm.getAncestorField().equals(fullName)) {
								m_activeMapping.getOutputRecord().getField(afm.getCurrentField()).fromString(getCurrentValue());
							}
						}
					}
				}
				processCharacters(namespaceURI, localName, m_level == m_activeMapping.getLevel());

				if (m_element_as_text) {
					boolean clearMarkers = true;
					for (CharacterBufferMarker marker : this.m_elementContentStartIndexStack) {
						if (marker.level < m_level) {
							clearMarkers = false;
						}
					}
					if (clearMarkers) {
						this.m_elementContentStartIndexStack.clear();
					}
				}

				// Extract Element As Text - clean-up buffers when value is stored to output
				if (this.m_elementContentStartIndexStack.size() == 0 && m_activeMapping != null && m_activeMapping.getLevel() == m_level && (m_activeMapping.getFieldsMap().containsKey(XMLMappingConstants.ELEMENT_CONTENTS_AS_TEXT) || m_activeMapping.getFieldsMap().containsKey(XMLMappingConstants.ELEMENT_AS_TEXT))) {
					m_element_as_text = false;
					m_hasCharacters = false;
					m_grabCharacters = true;
					m_characters.setLength(0);
					//logger.trace("endElement(" + qualifiedName + "): cleaning m_characters; end of m_element_as_text");
				} else if (!m_element_as_text && this.m_elementContentStartIndexStack.size() == 0) {
					// Regardless of whether this was saved, reset the length of the
					// buffer and flag
					//logger.trace("endElement(" + qualifiedName + "): cleaning m_characters; !m_element_as_text");
					m_characters.setLength(0);
					m_hasCharacters = false;
				}
			}

			if (m_activeMapping != null && m_level == m_activeMapping.getLevel()) {
				// This is the closing element of the matched element that
				// triggered the processing
				// That should be the end of this record so send it off to the
				// next Node
				if (parentComponent.runIt()) {
					try {
						OutputPort outPort = getOutputPort(m_activeMapping.getOutputPortNumber());

						if (outPort != null) {
							// we just ignore creating output, if port is empty (without metadata) or not specified
							DataRecord outRecord = m_activeMapping.getOutputRecord();

							// skip or process row
							if (skipRows > 0) {
								if (m_activeMapping.getParent() == null)
									skipRows--;
							} else {
								// check for index of last returned record
								if (!(numRecords >= 0 && numRecords == autoFilling.getGlobalCounter())) {
									// set autofilling
									autoFilling.setAutoFillingFields(outRecord);

									// can I do the map? it depends on skip and numRecords.
									if (m_activeMapping.doMap() && !m_activeMapping.isUsingParentRecord()) {
										// send off record
										outPort.writeRecord(outRecord);
									}
									// if (m_activeMapping.getParent() == null) autoFilling.incGlobalCounter();
								}
							}

							// resets all child's mappings for skip and numRecords
							m_activeMapping.resetCurrentRecord4ChildMapping();
							m_activeMapping.setCharactersProcessed(false);
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

			// text value immediately after end tag element should not be stored
			if (!m_element_as_text) {
				m_grabCharacters = false;
			}
			// ended an element so decrease our depth
			m_level--;
		}

		@Override
		public void endCDATA() throws SAXException {
			// super.endCDATA();
			in_cdata = false;
			//logger.trace("Leaving CDATA");
			if (m_element_as_text) {
				m_elementContentStartIndexStack.add(new CharacterBufferMarker(CharacterBufferMarkerType.CDATA_END, m_characters.length(), m_level));
				m_characters.append(CHARS_CDATA_END);
				m_hasCharacters = true;
			}
		}

		@Override
		public void startCDATA() throws SAXException {
			super.startCDATA();
			//logger.trace("Entering CDATA");
			in_cdata = true;
			if (m_element_as_text) {
				m_elementContentStartIndexStack.add(new CharacterBufferMarker(CharacterBufferMarkerType.CDATA_START, m_characters.length(), m_level));
				m_characters.append(CHARS_CDATA_START);
				m_hasCharacters = true;
			}
		}

		@Override
		public void processingInstruction(String target, String data) throws SAXException {
			super.processingInstruction(target, data);
			//logger.trace("Processing instruction " + target);
			if (m_element_as_text) {
				m_characters.append("<?").append(target).append(" ").append(data).append("?>");
			}
			m_hasCharacters = true;
		}

		/**
		 * @return
		 */
		private String getCurrentValue() {
			return trim ? m_characters.toString().trim() : m_characters.toString();
		}


		private String getCurrentValue(int startIndex, int endIndex, boolean excludeCDataTag) {
			if (startIndex < 0) {
				return this.getCurrentValue();
			}
			String value = null;

			int excludeStart = -1;
			int excludeEnd = -1;
			int storedEndIndex = endIndex;
			if (excludeCDataTag) {
				for (int i = 0; i < this.m_elementContentStartIndexStack.size(); i++) {
					if (this.m_elementContentStartIndexStack.get(i).index > startIndex && this.m_elementContentStartIndexStack.get(i).index < endIndex) {
						if (this.m_elementContentStartIndexStack.get(i).type == CharacterBufferMarkerType.CDATA_START) {
							excludeStart = this.m_elementContentStartIndexStack.get(i).index;
							excludeEnd = excludeStart + CHARS_CDATA_START.length();
							endIndex = excludeStart;
						} else if (this.m_elementContentStartIndexStack.get(i).type == CharacterBufferMarkerType.CDATA_END) {
							excludeStart = this.m_elementContentStartIndexStack.get(i).index;
							excludeEnd = excludeStart + CHARS_CDATA_END.length();
							endIndex = excludeStart;
						}
					}
				}
			}

			if (endIndex < 0) {
				value = m_characters.substring(startIndex);
			} else {
				value = m_characters.substring(startIndex, endIndex);
			}
			if (excludeStart > 0 && excludeEnd > 0) {
				value += getCurrentValue(excludeEnd, storedEndIndex, excludeCDataTag);
			}

			return (trim && value != null) ? value.trim() : value;
		}

		private int firstIndexWithType(Set<CharacterBufferMarkerType> types, int startFrom) {
			for (int i = startFrom; i < this.m_elementContentStartIndexStack.size(); i++) {
				if (types.contains(this.m_elementContentStartIndexStack.get(i).type)) {
					return i;
				}
			}
			return -1;

		}


		private int lastIndexWithType(CharacterBufferMarkerType type) {
			for (int i = this.m_elementContentStartIndexStack.size() - 1; i >= 0; i--) {
				if (this.m_elementContentStartIndexStack.get(i).type == type) {
					return i;
				}
			}
			return -1;
		}

		/**
		 * Store the characters processed by the characters() call back only if we have corresponding output field and
		 * we are on the right level or we want to use data from nested unmapped nodes
		 */
		private void processCharacters(String namespaceURI, String localName, boolean elementValue) {
			// Create universal name
			String universalName = null;
			if (localName != null) {
				universalName = augmentURIAndLocalName(namespaceURI, localName);
			}

			String fieldName = null;
			// use fields mapping 
			Map<String, String> xml2clover = m_activeMapping.getFieldsMap();
			
			
			if (xml2clover != null /*&& xml2clover.keySet().size() > 0*/) {
				Set<String> keys = xml2clover.keySet();
				
				for (int counter=0; counter<m_activeMapping.getSubtreeKeys().length+1; counter++) { //three times check special values (text content, ...). Fourth time - element mapped by name.
				
					String key = universalName; 
					if(counter<m_activeMapping.getSubtreeKeys().length) {
						key = m_activeMapping.getSubtreeKeys()[counter];
					}
					
					if(key==null) {
						continue;
					}
					
					int startIndex = -1;
					int endIndex = -1;
					boolean excludeCDataTag = false;
					if (elementValue && key.equals(XMLMappingConstants.ELEMENT_VALUE_REFERENCE)) {
						int startIndexPosition = this.lastIndexWithType(CharacterBufferMarkerType.CHARACTERS_START);
						if (startIndexPosition >= 0) {
							int endIndexPosition = this.firstIndexWithType(new HashSet<CharacterBufferMarkerType>(Arrays.asList(CharacterBufferMarkerType.SUBTREE_WITH_TAG_START, CharacterBufferMarkerType.SUBTREE_WITH_TAG_END, CharacterBufferMarkerType.SUBTREE_END, CharacterBufferMarkerType.SUBTREE_START)), startIndexPosition);
							endIndexPosition--; // we need one marker before found
							if (endIndexPosition < 0 && startIndexPosition >= 0) {
								endIndexPosition = this.lastIndexWithType(CharacterBufferMarkerType.CHARACTERS_END);
							}

							if (endIndexPosition > 0) {
								endIndex = this.m_elementContentStartIndexStack.get(endIndexPosition).index;
								// now remove all characters marker between startIndexPosition and endIndexPosition
								for (int i = endIndexPosition - 1; i > startIndexPosition; i--) {
									CharacterBufferMarkerType type = this.m_elementContentStartIndexStack.get(i).type;
									if (type == CharacterBufferMarkerType.CHARACTERS_END || type == CharacterBufferMarkerType.CHARACTERS_START) {
										this.m_elementContentStartIndexStack.remove(i);
									}
								}
								this.m_elementContentStartIndexStack.get(endIndexPosition);
							}
							startIndex = this.m_elementContentStartIndexStack.get(startIndexPosition).index;
							if(startIndexPosition>=0 && endIndexPosition>startIndexPosition) {
								for(int i=endIndexPosition; i>=startIndexPosition; i--) {
									this.m_elementContentStartIndexStack.remove(i);
								}
							}
							
							
						}
						excludeCDataTag = true;
						fieldName = xml2clover.get(XMLMappingConstants.ELEMENT_VALUE_REFERENCE);
					} else if (m_hasCharacters && key.equals(XMLMappingConstants.ELEMENT_CONTENTS_AS_TEXT) && m_activeMapping.getLevel() == m_level) {
						fieldName = xml2clover.get(XMLMappingConstants.ELEMENT_CONTENTS_AS_TEXT);
						int endIndexPosition = this.lastIndexWithType(CharacterBufferMarkerType.SUBTREE_END);
						if (endIndexPosition >= 0) {
							endIndex = this.m_elementContentStartIndexStack.get(endIndexPosition).index;
						}
						int startIndexPosition = this.lastIndexWithType(CharacterBufferMarkerType.SUBTREE_START);
						if (startIndexPosition >= 0) {
							startIndex = this.m_elementContentStartIndexStack.get(startIndexPosition).index;
						}
						if(startIndexPosition>=0 && endIndexPosition>startIndexPosition) {
							for(int i=endIndexPosition; i>=startIndexPosition; i--) {
								this.m_elementContentStartIndexStack.remove(i);
							}
						}
//						logger.trace("processCharacters: getting field name for (" + localName + "); " + fieldName + "; xml2clover=" + xml2clover + ", cloverAttributes=" + cloverAttributes);
					} else if (m_hasCharacters && key.equals(XMLMappingConstants.ELEMENT_AS_TEXT) && m_activeMapping.getLevel() == m_level) {
						fieldName = xml2clover.get(XMLMappingConstants.ELEMENT_AS_TEXT);
						int startIndexPosition = this.lastIndexWithType(CharacterBufferMarkerType.SUBTREE_WITH_TAG_START);
						if (startIndexPosition >= 0) {
							startIndex = this.m_elementContentStartIndexStack.get(startIndexPosition).index;
							for(int i=this.m_elementContentStartIndexStack.size()-1; i>=startIndexPosition; i--) {
								this.m_elementContentStartIndexStack.remove(i);
							}
						}
						
//						logger.trace("processCharacters: getting field name for (" + localName + "); " + fieldName + "; xml2clover=" + xml2clover + ", cloverAttributes=" + cloverAttributes);
					} else 
					if (key.equals(universalName)) {
						fieldName = xml2clover.get(universalName);
						
						if(m_element_as_text) {
							int startIndexPosition = this.lastIndexWithType(CharacterBufferMarkerType.CHARACTERS_START);
							if (startIndexPosition >= 0) {
								int endIndexPosition = this.firstIndexWithType(new HashSet<CharacterBufferMarkerType>(Arrays.asList(CharacterBufferMarkerType.SUBTREE_WITH_TAG_START, CharacterBufferMarkerType.SUBTREE_WITH_TAG_END, CharacterBufferMarkerType.SUBTREE_END, CharacterBufferMarkerType.SUBTREE_START)), startIndexPosition);
								endIndexPosition--; // we need one marker before found
								if (endIndexPosition < 0 && startIndexPosition >= 0) {
									endIndexPosition = this.lastIndexWithType(CharacterBufferMarkerType.CHARACTERS_END);
								}

								if (endIndexPosition > 0) {
									endIndex = this.m_elementContentStartIndexStack.get(endIndexPosition).index;
									// now remove all characters marker between startIndexPosition and endIndexPosition
									for (int i = endIndexPosition - 1; i > startIndexPosition; i--) {
										CharacterBufferMarkerType type = this.m_elementContentStartIndexStack.get(i).type;
										if (type == CharacterBufferMarkerType.CHARACTERS_END || type == CharacterBufferMarkerType.CHARACTERS_START) {
											this.m_elementContentStartIndexStack.remove(i);
										}
									}
									this.m_elementContentStartIndexStack.get(endIndexPosition);
								}
								startIndex = this.m_elementContentStartIndexStack.get(startIndexPosition).index;
								if(startIndexPosition>=0 && endIndexPosition>startIndexPosition) {
									for(int i=endIndexPosition; i>=startIndexPosition; i--) {
										this.m_elementContentStartIndexStack.remove(i);
									}
								}
								
								
							}							
						}
						
					} else if (m_activeMapping.getExplicitCloverFields().contains(localName) || keys.contains(localName) || keys.contains(universalName)) {
						//if local name is mentioned in explicit mapping, we will not let code do implicit mapping for this field
						continue;
					} else if (m_activeMapping.getSequenceField()!=null && (m_activeMapping.getSequenceField().equals(localName) || m_activeMapping.getSequenceField().equals(universalName) ) ) {
						continue; //don't do implicit mapping for fields mapped to sequence
					} 

					if (fieldName == null && m_activeMapping.isImplicit()) {
						/*
						 * As we could not find match using qualified name try mapping the xml element/attribute without
						 * the namespace prefix
						 */
						fieldName = localName;
					}

					// XXX what if fieldName == null ???

					// TODO Labels replace:
					if (m_activeMapping.getOutputRecord() != null && (useNestedNodes || m_level - 1 <= m_activeMapping.getLevel())) {
						int position =  m_activeMapping.getOutputRecord().getMetadata().getFieldPosition(fieldName);
						if(position>=0) {
							DataField field = m_activeMapping.getOutputRecord().getField(position);
						
						// If field is nullable and there's no character data set it to null
						if (m_hasCharacters) {
							
							//logger.trace("processCharacters: storing (" + localName + "); into field " + fieldName + "; xml2clover=" + xml2clover + ", cloverAttributes=" + cloverAttributes);
							try {
								if (field.getValue() != null && cloverAttributes.contains(fieldName)) {
									// XXX WTF?
									field.fromString(trim ? field.getValue().toString().trim() : field.getValue().toString());
								} else {
									//logger.trace("processCharacters: storing a new value (" + localName + "); into field " + fieldName + "; xml2clover=" + xml2clover + ", cloverAttributes=" + cloverAttributes);
									field.fromString(getCurrentValue(startIndex, endIndex, excludeCDataTag));
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
										String dateTime = m_characters.substring(0, m_characters.lastIndexOf(":")) + m_characters.substring(m_characters.lastIndexOf(":") + 1);
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
			} else {
				fieldName = localName;
				// TODO Labels replace:
				if (m_activeMapping.getOutputRecord() != null && m_activeMapping.getOutputRecord().hasField(fieldName) && (useNestedNodes || m_level - 1 <= m_activeMapping.getLevel())) {
					DataField field = m_activeMapping.getOutputRecord().getField(fieldName);
					// If field is nullable and there's no character data set it to null
					if (m_hasCharacters) {
						//logger.trace("processCharacters: storing (" + localName + "); into field " + fieldName + "; xml2clover=" + xml2clover + ", cloverAttributes=" + cloverAttributes);
						try {
							if (field.getValue() != null && cloverAttributes.contains(fieldName)) {
								// XXX WTF?
								field.fromString(trim ? field.getValue().toString().trim() : field.getValue().toString());
							} else {
								//logger.trace("processCharacters: storing a new value (" + localName + "); into field " + fieldName + "; xml2clover=" + xml2clover + ", cloverAttributes=" + cloverAttributes);
								field.fromString(getCurrentValue());
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
									String dateTime = m_characters.substring(0, m_characters.lastIndexOf(":")) + m_characters.substring(m_characters.lastIndexOf(":") + 1);
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
	}

	/**
	 * Augments the namespaceURIs with curly brackets to allow easy creation of qualified names E.g.
	 * xmlns:mov="http://www.javlin.eu/mov" URI = "http://www.javlin.eu"; Augmented URI = "{http://www.javlin.eu}";
	 */
	protected void augmentNamespaceURIs() {
		for (String prefix : namespaceBindings.keySet()) {
			String uri = namespaceBindings.get(prefix);
			namespaceBindings.put(prefix, augmentURI(uri));
		}
	}

	private String augmentURIAndLocalName(String uri, String localName) {
		if (uri == null) {
			return null;
		}

		return "{" + uri + "}" + localName;
	}
	
	private String augmentURI(String uri) {
		if (uri == null) {
			return null;
		}

		return "{" + uri + "}";
	}

	public static class MyHandler extends DefaultHandler {
		// Handler used at checkConfig to parse XML mapping and retrieve attributes names
		private Set<String> attributeNames = new HashSet<String>();
		private Set<String> cloverAttributes = new HashSet<String>();

		@Override
		public void startElement(String namespaceURI, String localName, String qName, Attributes atts) {
			int length = atts.getLength();
			for (int i = 0; i < length; i++) {
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

	//
	// /**
	// * @return the declaredTemplates
	// */
	// public TreeMap<String, XmlElementMapping> getDeclaredTemplates() {
	// return declaredTemplates;
	// }
	//
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

	private Set<String> getXmlElementMappingValues() {
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
	 * Sets namespace bindings to allow processing that relate namespace prefix used in Mapping and namespace URI used
	 * in processed XML document
	 * 
	 * @param namespaceBindings
	 *            the namespaceBindings to set
	 */
	public void setNamespaceBindings(HashMap<String, String> namespaceBindings) {
		this.namespaceBindings = new HashMap<String, String>(namespaceBindings); // Fix of CL-2510 --
																					// augmentNamespaceURIs() was run
																					// twice on the same
																					// namespaceBindings instance
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

	public void setMappingNodes(NodeList mappingNodes) {
		this.mappingNodes = mappingNodes;
	}

	public DataRecord getInputRecord() {
		return inputRecord;
	}

	public void setInputRecord(DataRecord inputRecord) {
		this.inputRecord = inputRecord;
	}

	public AutoFilling getAutoFilling() {
		return autoFilling;
	}

}

enum CharacterBufferMarkerType {
	CHARACTERS_START, SUBTREE_START, SUBTREE_WITH_TAG_START, CDATA_START, CHARACTERS_END, SUBTREE_END, SUBTREE_WITH_TAG_END, CDATA_END
}

class CharacterBufferMarker {
	public CharacterBufferMarkerType type;
	public int index;
	public int level;

	public CharacterBufferMarker(CharacterBufferMarkerType type, int index, int level) {
		this.type = type;
		this.index = index;
		this.level = level;
	}

	@Override
	public String toString() {
		return "("+(type != null ? type.name() : "[NO TYPE]") + ":" + this.index+":"+this.level+")";
	}
}