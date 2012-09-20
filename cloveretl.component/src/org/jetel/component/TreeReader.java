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
package org.jetel.component;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.StringDataField;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.tree.parser.ITreeContentHandler;
import org.jetel.data.tree.parser.TreeStreamParser;
import org.jetel.data.tree.parser.ValueHandler;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.sequence.PrimitiveSequence;
import org.jetel.util.AutoFilling;
import org.jetel.util.SourceIterator;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 13.10.2011
 */
public abstract class TreeReader extends Node {

	private static final Log LOG = LogFactory.getLog(TreeReader.class);

	private static final int INPUT_PORT_INDEX = 0;

	// xml attributes
	public static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
	public static final String XML_USE_NESTED_NODES_ATTRIBUTE = "useNestedNodes";
	public static final String XML_MAPPING_ATTRIBUTE = "mapping";
	public static final String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
	public static final String XML_SKIP_RECORDS_ATTRIBUTE = "skipRecords"; // TODO: some renaming was done here, revert
	public static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";

	// mapping attributes
	private static final String XML_MAPPING = "Mapping"; // TODO: really an uppercase here???
	private static final String XML_ELEMENT = "element";
	private static final String XML_OUTPORT = "outPort";
	private static final String XML_PARENT_KEY = "parentKey";
	private static final String XML_GENERATED_KEY = "generatedKey";
	private static final String XML_XML_FIELDS = "xmlFields";
	private static final String XML_CLOVER_FIELDS = "cloverFields";
	private static final String XML_SEQUENCE_FIELD = "sequenceField";
	private static final String XML_SEQUENCE_ID = "sequenceId";

	/** MiSho Experimental Templates */
	private static final String XML_TEMPLATE_ID = "templateId";
	private static final String XML_TEMPLATE_REF = "templateRef";
	private static final String XML_TEMPLATE_DEPTH = "nestedDepth";

	private static final String PARENT_MAPPING_REFERENCE_PREFIX = "..";
	private static final String PARENT_MAPPING_REFERENCE_SEPARATOR = "/";
	private static final String PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR = PARENT_MAPPING_REFERENCE_PREFIX + PARENT_MAPPING_REFERENCE_SEPARATOR;
	private static final String ELEMENT_VALUE_REFERENCE = ".";

	protected static void readCommonAttributes(TreeReader treeReader, ComponentXMLAttributes xattribs)
			throws XMLConfigurationException {
		try {
			treeReader.setFileURL(xattribs.getStringEx(XML_FILE_URL_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));

			String mappingURL = xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF);
			String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE, null);
			if (mappingURL != null) {
				treeReader.setMappingURL(mappingURL);
			} else if (mapping != null) {
				treeReader.setMappingString(mapping);
			} else {
				// throw configuration exception
				xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF);
			}
			if (xattribs.exists(XML_USE_NESTED_NODES_ATTRIBUTE)) {
				treeReader.setUseNestedNodes(xattribs.getBoolean(XML_USE_NESTED_NODES_ATTRIBUTE));
			}

			if (xattribs.exists(XML_SKIP_RECORDS_ATTRIBUTE)) {
				treeReader.setSkipRecords(xattribs.getInteger(XML_SKIP_RECORDS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)) {
				treeReader.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
			}

		} catch (Exception ex) {
			throw new XMLConfigurationException(treeReader.getType() + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}
	}

	protected String fileURL;
	private SourceIterator sourceIterator;

	private String mappingString;
	private String mappingURL;
	private boolean useNestedNodes;

	private Map<String, Mapping> mappingsMap = new HashMap<String, Mapping>();
	private TreeMap<String, Mapping> declaredTemplates = new TreeMap<String, Mapping>();

	private int numRecords = -1;
	private int skipRecords = 0; // do not skip any records by default

	private AutoFilling autoFilling = new AutoFilling();

	private ITreeContentHandler treeContentHandler;
	private TreeStreamParser treeParser;
	private Object inputData;

	public TreeReader(String id) {
		super(id);
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		for (Mapping mapping : mappingsMap.values()) {
			mapping.checkConfig(status);
		}

		return status;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}
		super.init();
		initMapping();

		treeContentHandler = new TreeContentHandler(createValueHandler());
		treeParser = createTreeParser();
		createSourceIterator();
	}

	private void createSourceIterator() throws ComponentNotReadyException {
		TransformationGraph graph = getGraph();
		URL projectURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;

		sourceIterator = new SourceIterator(getInputPort(INPUT_PORT_INDEX), projectURL, fileURL);
		// TODO: charset!!
		// sourceIterator.setCharset(charset);
		sourceIterator.setPropertyRefResolver(new PropertyRefResolver(graph.getGraphProperties()));
		sourceIterator.setDictionary(graph.getDictionary());
		sourceIterator.init();
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		sourceIterator.preExecute();
	}

	protected abstract ValueHandler createValueHandler();

	protected abstract TreeStreamParser createTreeParser();

	@Override
	public Result execute() throws Exception {
		while (nextSource()) {
			treeParser.parse(inputData, treeContentHandler);
		}
		// TODO: add abortion mechanism again

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	private boolean nextSource() throws JetelException {
		while (sourceIterator.hasNext()) {
			autoFilling.resetSourceCounter();
			autoFilling.resetGlobalSourceCounter();
			inputData = sourceIterator.next();
			if (inputData == null) {
				continue; // if record no record found
			}
			
			autoFilling.setFilename(sourceIterator.getCurrentFileName());
			if (!sourceIterator.isGraphDependentSource()) {
				long fileSize = 0;
				Date fileTimestamp = null;
				if (FileUtils.isLocalFile(autoFilling.getFilename()) && !sourceIterator.isGraphDependentSource()) {
					File tmpFile = new File(autoFilling.getFilename());
					long timestamp = tmpFile.lastModified();
					fileTimestamp = timestamp == 0 ? null : new Date(timestamp);
					fileSize = tmpFile.length();
				}
				autoFilling.setFileSize(fileSize);
				autoFilling.setFileTimestamp(fileTimestamp);
			}
			return true;
		}
		
		sourceIterator.blankRead();
		return false;
	}

	public void setFileURL(String fileURL) {
		this.fileURL = fileURL;
	}

	public void setMappingString(String mappingString) {
		this.mappingString = mappingString;
	}

	public void setMappingURL(String mappingURL) {
		this.mappingURL = mappingURL;
	}

	public void setUseNestedNodes(boolean useNestedNodes) {
		this.useNestedNodes = useNestedNodes;
	}

	public void setSkipRecords(int skipRows) {
		this.skipRecords = skipRows;
	}

	public void setNumRecords(int numRecords) {
		this.numRecords = numRecords;
	}

	@Override
	public abstract String getType();

	private void initMapping() throws ComponentNotReadyException {
		TransformationGraph graph = getGraph();

		InputStream stream;
		if (this.mappingURL != null) {
			try {
				stream = FileUtils.getInputStream(graph.getRuntimeContext().getContextURL(), mappingURL);
			} catch (IOException e) {
				LOG.error("Cannot instantiate node from XML", e);
				throw new ComponentNotReadyException(e.getMessage(), e);
			}
		} else {
			stream = new ByteArrayInputStream(this.mappingString.getBytes());
		}

		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setCoalescing(true);
		Document doc;
		try {
			doc = dbf.newDocumentBuilder().parse(stream);
		} catch (SAXException e) {
			throw new ComponentNotReadyException(e);
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		} catch (ParserConfigurationException e) {
			throw new ComponentNotReadyException(e);
		}
		NodeList mappingNodes = doc.getDocumentElement().getChildNodes();

		String errorPrefix = getId() + ": Mapping error - ";
		for (int i = 0; i < mappingNodes.getLength(); i++) {
			org.w3c.dom.Node node = mappingNodes.item(i);
			List<String> errors = processMappings(graph, null, node);
			for (String error : errors) {
				LOG.warn(errorPrefix + error);
			}
		}
	}

	private List<String> processMappings(TransformationGraph graph, Mapping parentMapping, org.w3c.dom.Node nodeXML) {
		List<String> errors = new LinkedList<String>();

		if (XML_MAPPING.equals(nodeXML.getNodeName())) {
			// for a mapping declaration, process all of the attributes
			// element, outPort, parentKeyName, generatedKey
			ComponentXMLAttributes attributes = new ComponentXMLAttributes((Element) nodeXML, graph);
			Mapping mapping = null;

			if (attributes.exists(XML_TEMPLATE_REF)) {
				// template mapping reference
				String templateId = attributes.getString(XML_TEMPLATE_REF, null);

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
					mapping = new Mapping(attributes.getString(XML_ELEMENT), outputPort, parentMapping);
				} else {
					if (outputPort != -1) {
						mapping.setOutPort(outputPort);
						if (attributes.exists(XML_ELEMENT)) {
							mapping.setElement(attributes.getString(XML_ELEMENT));
						}
					}
				}
			} catch (AttributeNotFoundException ex) {
				errors.add("Required attribute 'element' missing. Skipping this mapping and all children.");
				return errors;
			}

			// Add new root mapping
			if (parentMapping == null) {
				addMapping(mapping);
			}

			boolean parentKeyPresent = false;
			boolean generatedKeyPresent = false;
			if (attributes.exists(XML_PARENT_KEY)) {
				mapping.setParentKey(attributes.getString(XML_PARENT_KEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
				parentKeyPresent = true;
			}

			if (attributes.exists(XML_GENERATED_KEY)) {
				mapping.setGeneratedKey(attributes.getString(XML_GENERATED_KEY, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
				generatedKeyPresent = true;
			}

			if (parentKeyPresent != generatedKeyPresent) {
				errors.add("Mapping for element: " + mapping.getElement() + " must either have both 'parentKey' and 'generatedKey' attributes or neither.");
				mapping.setParentKey(null);
				mapping.setGeneratedKey(null);
			}

			if (parentKeyPresent && mapping.getParent() == null) {
				errors.add("Mapping for element: " + mapping.getElement() + " may only have 'parentKey' or 'generatedKey' attributes if it is a nested mapping.");
				mapping.setParentKey(null);
				mapping.setGeneratedKey(null);
			}

			// mapping between xml fields and clover fields initialization
			if (attributes.exists(XML_XML_FIELDS) && attributes.exists(XML_CLOVER_FIELDS)) {
				String[] xmlFields = attributes.getString(XML_XML_FIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER);
				String[] cloverFields = attributes.getString(XML_CLOVER_FIELDS, null).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
				// TODO add existence check for Clover fields, if possible

				if (xmlFields.length == cloverFields.length) {
					for (int i = 0; i < xmlFields.length; i++) {
						if (xmlFields[i].startsWith(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR) || xmlFields[i].equals(PARENT_MAPPING_REFERENCE_PREFIX)) {
							mapping.addAcestorFieldMapping(xmlFields[i], cloverFields[i]);
						} else {
							mapping.putXml2CloverFieldMap(xmlFields[i], cloverFields[i]);
						}
					}
				} else {
					errors.add("Mapping for element: " + mapping.getElement() + " must have same number of the xml fields and the clover fields attribute.");
				}
			}

			// sequence field
			if (attributes.exists(XML_SEQUENCE_FIELD)) {
				mapping.setSequenceField(attributes.getString(XML_SEQUENCE_FIELD, null));
				mapping.setSequenceId(attributes.getString(XML_SEQUENCE_ID, null));
			}

			// skip rows field
			if (attributes.exists(XML_SKIP_RECORDS_ATTRIBUTE)) {
				mapping.setSkipRecords4Mapping(attributes.getInteger(XML_SKIP_RECORDS_ATTRIBUTE, 0));
			}

			// number records field
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

	public void addMapping(Mapping mapping) {
		mappingsMap.put(mapping.getElement(), mapping);
	}

	private class TreeContentHandler implements ITreeContentHandler {

		private int level;
		private Mapping currentMapping;

		private boolean processLeaves = false;
		private boolean valueInNode = false;

		private ValueHandler valueHandler;

		public TreeContentHandler(ValueHandler valueHandler) {
			this.valueHandler = valueHandler;
		}

		@Override
		public void startTree() {
			// Do nothing
		}

		@Override
		public void startNode(String name) {
			level++;
			processLeaves = true;
			storeValue(null, true, false);

			DataRecord recordToFill = prepareMappingAndRecord(name);
			if (recordToFill != null) {
				fillSequenceFields(recordToFill);
				fillFieldsFromAncestor(recordToFill);
				fillKeyFields(recordToFill);
			}
		}

		private DataRecord prepareMappingAndRecord(String localName) {
			Mapping mapping = getNextMapping(localName);
			if (mapping == null) {
				return null;
			}

			currentMapping = mapping;
			currentMapping.setLevel(level);
			clearDescendantValues();

			DataRecord recordToFill = currentMapping.getOutRecord();
			if (recordToFill == null) {
				return null;
			}

			// TODO: what is this???=========================
			currentMapping.prepareDoMap();
			currentMapping.incCurrentRecord4Mapping();
			// ===============================================

			return recordToFill;
		}

		private void fillKeyFields(DataRecord recordToFill) {
			// if generatedKey is a single array, all parent keys are concatenated into generatedKey
			// field
			// I know it is ugly code...

			String[] generatedKey = currentMapping.getGeneratedKey();
			String[] parentKey = currentMapping.getParentKey();
			if (parentKey == null) {
				return;
			}

			for (int i = 0; i < parentKey.length; i++) {
				DataField generatedKeyField = generatedKey.length == 1 ? recordToFill.getField(generatedKey[0]) : recordToFill.getField(generatedKey[i]);
				DataField parentKeyField = currentMapping.getParent().getOutRecord().getField(parentKey[i]);
				if (generatedKey.length != parentKey.length) {
					((StringDataField) generatedKeyField).append(parentKeyField.toString());
				} else {
					generatedKeyField.setValue(parentKeyField);
				}
			}
		}

		private void fillFieldsFromAncestor(DataRecord recordToFill) {
			if (currentMapping.hasFieldsFromAncestor()) {
				DataRecordMetadata metadata = recordToFill.getMetadata();

				for (AncestorFieldMapping afm : currentMapping.getFieldsFromAncestor()) {
					if (afm.ancestor == null) {
						continue;
					}

					int fieldToFillIndex = metadata.getFieldPosition(afm.currentField);
					if (fieldToFillIndex >= 0) {
						Object value = afm.ancestor.descendantReferrences.get(afm.ancestorField);
						valueHandler.storeValueToField(value, recordToFill.getField(fieldToFillIndex));
					}
				}
			}
		}

		private void fillSequenceFields(DataRecord recordToFill) {
			String sequenceFieldName = currentMapping.getSequenceField();
			if (sequenceFieldName == null) {
				return;
			}
			int fieldToFillIndex = recordToFill.getMetadata().getFieldPosition(sequenceFieldName);
			if (fieldToFillIndex < 0) {
				return;
			}

			Sequence sequence = currentMapping.getSequence();
			DataField sequenceField = recordToFill.getField(fieldToFillIndex);
			switch (sequenceField.getType()) {
			case DataFieldMetadata.INTEGER_FIELD:
				sequenceField.setValue(sequence.nextValueInt());
				break;
			case DataFieldMetadata.LONG_FIELD:
			case DataFieldMetadata.DECIMAL_FIELD:
			case DataFieldMetadata.NUMERIC_FIELD:
				sequenceField.setValue(sequence.nextValueLong());
				break;
			default:
				sequenceField.fromString(sequence.nextValueString());
				break;
			}
		}

		private void clearDescendantValues() {
			for (Entry<String, Object> e : currentMapping.descendantReferrences.entrySet()) {
				e.setValue(null);
			}
		}

		private Mapping getNextMapping(String localName) {
			if (currentMapping == null) {
				return mappingsMap.get(localName);
			} else if (useNestedNodes || currentMapping.getLevel() == level - 1) {
				return currentMapping.getChildMapping(localName);
			}

			return null;
		}

		private void storeValue(String nodeName, boolean isParentValue, boolean endElement) {
			if (currentMapping != null && level == currentMapping.getLevel() + 1 && valueInNode) {
				String descendantKey = endElement ? nodeName : ELEMENT_VALUE_REFERENCE;
				if (currentMapping.storeNodeValueForDescendants(descendantKey)) {
					currentMapping.descendantReferrences.put(descendantKey, valueHandler.getCurrentValue());
				}
				storeValueToField(nodeName, isParentValue);
			}
			valueInNode = false;
			valueHandler.clearCurrentValue();
		}

		private void storeValueToField(String targetFieldName, boolean elementValue) {
			DataRecord recordToFill = currentMapping.getOutRecord();
			if (recordToFill == null || !valueInNode) {
				return; // nowhere to fill
			}
			if (!valueInNode) {
				return; // nothing to fill
			}

			Map<String, String> xml2clover = currentMapping.getXml2CloverFieldsMap();
			if (xml2clover != null) {
				if (elementValue && xml2clover.containsKey(ELEMENT_VALUE_REFERENCE)) {
					targetFieldName = xml2clover.get(ELEMENT_VALUE_REFERENCE);
				} else if (xml2clover.containsKey(targetFieldName)) {
					targetFieldName = xml2clover.get(targetFieldName);
				} else if (currentMapping.explicitCloverFields.contains(targetFieldName)) {
					return; // don't do implicit mapping if clover field is used in an explicit mapping
				}
			}

			if (useNestedNodes || level - 1 <= currentMapping.getLevel()) {
				int fieldPosition = recordToFill.getMetadata().getFieldPosition(targetFieldName);
				if (fieldPosition >= 0) {
					valueHandler.storeValueToField(valueHandler.getCurrentValue(), recordToFill.getField(fieldPosition));
				}
			}
		}

		@Override
		public void leaf(Object value) {
			if (currentMapping != null && processLeaves) {
				valueHandler.appendValue(value);
				valueInNode = true;
			}

		}

		@Override
		public void endNode(String name) {
			if (currentMapping != null) {

				storeValue(name, level == currentMapping.getLevel(), true);

				if (level == currentMapping.getLevel()) {
					// This is the closing element of the matched element that triggered the processing That should be
					// the
					// end of this record so send it off to the next Node
					OutputPort outPort = getOutputPort(currentMapping.getOutPort());
					if (outPort != null) {
						DataRecord outRecord = currentMapping.getOutRecord();

						// skip or process row
						if (skipRecords > 0) {
							if (currentMapping.getParent() == null) {
								skipRecords--;
							}
						} else {
							// if (numRecords < autoFilling.getGlobalCounter()) {
							// autoFilling.setAutoFillingFields(outRecord);

							// can I do the map? it depends on skip and numRecords.
							if (currentMapping.doMap()) {
								try {
									outPort.writeRecord(outRecord);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
							// }
						}

						// resets all child's mappings for skip and numRecords
						currentMapping.resetCurrentRecord4ChildMapping();
						outRecord.reset();
					}

					currentMapping = currentMapping.getParent();
				}
			}

			processLeaves = false;
			level--;
		}

		@Override
		public void endTree() {
			// Do nothing
		}

	}

	/**
	 * Mapping holds a single mapping.
	 */
	public class Mapping {
		private Mapping parent; // direct parent mapping
		private Map<String, Mapping> childMap; // direct children for this mapping

		private String nodeName; // name of an node for this mapping

		private int outPortIndex; // output port number
		private DataRecord outRecord; // output record

		private String sequenceField; // sequence field
		private String sequenceId; // sequence ID
		private Sequence sequence; // sequence (Simple, Db,..)

		private int level; // original xml tree level (a depth of this element)

		/** Mapping - node name -> clover field name */
		private Map<String, String> node2fieldMap = new HashMap<String, String>();
		/**
		 * Set of Clover fields which are mapped explicitly (using xmlFields & cloverFields attributes). It is union of
		 * node2fieldMap.values() and Clover fields from fieldsFromAncestor list. Its purpose: quick lookup
		 */
		private Set<String> explicitCloverFields = new HashSet<String>();

		/** List of clover fields (among else) which will be filled from ancestor */
		private List<AncestorFieldMapping> fieldsFromAncestor;
		/** Mapping - node name -> clover field name. These node fields are referenced by descendant mappings */
		private Map<String, Object> descendantReferrences = new HashMap<String, Object>();

		// TODO: rework this to be int arrays, not String ones!
		private String[] parentKey; // parent keys
		private String[] generatedKey; // generated keys

		// for skip and number a record attribute for this mapping
		int skipRecords4Mapping; // skip records
		int numRecords4Mapping = Integer.MAX_VALUE; // number records
		int currentRecord4Mapping; // record counter for this mapping
		boolean processSkipOrNumRecords; // what xml element can be skiped
		boolean bDoMap = true; // should I skip an xml element? depends on processSkipOrNumRecords
		boolean bReset4CurrentRecord4Mapping; // should I reset submappings?

		/**
		 * Copy constructor - created a deep copy of all attributes and children elements
		 */
		public Mapping(Mapping otherMapping, Mapping parent) {
			this.nodeName = otherMapping.nodeName;
			this.outPortIndex = otherMapping.outPortIndex;
			this.parentKey = otherMapping.parentKey == null ? null : Arrays.copyOf(otherMapping.parentKey, otherMapping.parentKey.length);
			this.generatedKey = otherMapping.generatedKey == null ? null : Arrays.copyOf(otherMapping.generatedKey, otherMapping.generatedKey.length);
			this.sequenceField = otherMapping.sequenceField;
			this.sequenceId = otherMapping.sequenceId;
			this.skipRecords4Mapping = otherMapping.skipRecords4Mapping;
			this.numRecords4Mapping = otherMapping.numRecords4Mapping;
			node2fieldMap = new HashMap<String, String>(otherMapping.node2fieldMap);

			// Create deep copy of children elements
			if (otherMapping.childMap != null) {
				this.childMap = new HashMap<String, Mapping>();
				for (String key : otherMapping.childMap.keySet()) {
					final Mapping child = new Mapping(otherMapping.childMap.get(key), this);
					this.childMap.put(key, child);
				}
			}

			if (parent != null) {
				setParent(parent);
				parent.addChildMapping(this);
			}

			if (otherMapping.hasFieldsFromAncestor()) {
				for (AncestorFieldMapping m : otherMapping.getFieldsFromAncestor()) {
					addAcestorFieldMapping(m.originalFieldReference, m.currentField);
				}
			}

		}

		/**
		 * @param status
		 */
		public void checkConfig(ConfigurationStatus status) {
			// if (generatedKey.length != parentKey.length && generatedKey.length != 1) {
			//
			// }

			//
			// LOG.warn(getId() +
			// ": XML Extract Mapping's generatedKey and parentKey attribute has different number of field.");
			// m_activeMapping.setGeneratedKey(null);
			// m_activeMapping.setParentKey(null);
			// } else {

			// boolean existGeneratedKeyField = (outRecord != null) && (generatedKey.length == 1 ?
			// outRecord.hasField(generatedKey[0]) : outRecord.hasField(generatedKey[i]));
			// boolean existParentKeyField = m_activeMapping.getParent().getOutRecord() != null &&
			// m_activeMapping.getParent().getOutRecord().hasField(parentKey[i]);
			// if (!existGeneratedKeyField) {
			// LOG.warn(getId() + ": XML Extract Mapping's generatedKey field was not found. generatedKey: " +
			// (generatedKey.length == 1 ? generatedKey[0] : generatedKey[i]) + " of element " +
			// m_activeMapping.m_element +
			// ", outPort: " + m_activeMapping.m_outPort);
			// m_activeMapping.setGeneratedKey(null);
			// m_activeMapping.setParentKey(null);
			// } else if (!existParentKeyField) {
			// LOG.warn(getId() + ": XML Extract Mapping's parentKey field was not found. parentKey: " + parentKey[i] +
			// " of element " + m_activeMapping.m_element + ", outPort: " + m_activeMapping.m_outPort);
			// m_activeMapping.setGeneratedKey(null);
			// m_activeMapping.setParentKey(null);
			// } else {

			// if (generatedKey.length != parentKey.length) {
			// if (generatedKeyField.getType() != DataFieldMetadata.STRING_FIELD) {
			// LOG.warn(getId() +
			// ": XML Extract Mapping's generatedKey field has to be String type (keys are concatened to this field).");
			// m_activeMapping.setGeneratedKey(null);
			// m_activeMapping.setParentKey(null);
		}

		/**
		 * Minimally required information.
		 */
		public Mapping(String element, int outPort, Mapping parent) {
			this.nodeName = element;
			this.outPortIndex = outPort;
			this.parent = parent;
			if (parent != null) {
				parent.addChildMapping(this);
			}
		}

		/**
		 * Gives the optional attributes parentKey and generatedKey.
		 */
		public Mapping(String element, int outPort, String parentKey[], String[] generatedKey, Mapping parent) {
			this(element, outPort, parent);

			this.parentKey = parentKey;
			this.generatedKey = generatedKey;
		}

		public boolean storeNodeValueForDescendants(String key) {
			return descendantReferrences.containsKey(key);
		}

		/**
		 * Gets original xml tree level (a deep of this element)
		 * 
		 * @return
		 */
		public int getLevel() {
			return level;
		}

		/**
		 * Sets original xml tree level (a deep of this element)
		 * 
		 * @param level
		 */
		public void setLevel(int level) {
			this.level = level;
		}

		/**
		 * Sets direct children for this mapping.
		 * 
		 * @return
		 */
		public Map<String, Mapping> getChildMap() {
			return childMap;
		}

		/**
		 * Gets direct children for this mapping.
		 * 
		 * @param element
		 * @return
		 */
		public Mapping getChildMapping(String element) {
			if (childMap == null) {
				return null;
			}
			return childMap.get(element);
		}

		/**
		 * Adds a direct child for this mapping.
		 * 
		 * @param mapping
		 */
		public void addChildMapping(Mapping mapping) {
			if (childMap == null) {
				childMap = new HashMap<String, Mapping>();
			}
			childMap.put(mapping.getElement(), mapping);
		}

		/**
		 * Removes a direct child for this mapping.
		 * 
		 * @param mapping
		 */
		public void removeChildMapping(Mapping mapping) {
			if (childMap == null) {
				return;
			}
			childMap.remove(mapping.getElement());
		}

		/**
		 * Gets an element name for this mapping.
		 * 
		 * @return
		 */
		public String getElement() {
			return nodeName;
		}

		/**
		 * Sets an node name for this mapping.
		 * 
		 * @param node
		 */
		public void setElement(String node) {
			this.nodeName = node;
		}

		/**
		 * Gets generated keys of for this mapping.
		 * 
		 * @return
		 */
		public String[] getGeneratedKey() {
			return generatedKey;
		}

		/**
		 * Sets generated keys of for this mapping.
		 * 
		 * @param generatedKey
		 */
		public void setGeneratedKey(String[] generatedKey) {
			this.generatedKey = generatedKey;
		}

		/**
		 * Gets an output port.
		 * 
		 * @return
		 */
		public int getOutPort() {
			return outPortIndex;
		}

		/**
		 * Sets an output port.
		 * 
		 * @param outPortIndex
		 */
		public void setOutPort(int outPortIndex) {
			this.outPortIndex = outPortIndex;
		}

		/**
		 * Gets mapping - xml name -> clover field name WARNING: values of this map must be kept in synch with
		 * explicitCloverFields; prefer {@link #putXml2CloverFieldMap()}
		 */
		public Map<String, String> getXml2CloverFieldsMap() {
			return node2fieldMap;
		}

		public void putXml2CloverFieldMap(String xmlField, String cloverField) {
			node2fieldMap.put(xmlField, cloverField);
			explicitCloverFields.add(cloverField);
		}

		/**
		 * Gets an output record.
		 * 
		 * @return
		 */
		public DataRecord getOutRecord() {
			if (outRecord == null) {
				OutputPort outPort = getOutputPort(getOutPort());
				if (outPort != null) {
					DataRecordMetadata dataRecordMetadata = outPort.getMetadata();
					autoFilling.addAutoFillingFields(dataRecordMetadata);
					outRecord = new DataRecord(dataRecordMetadata);
					outRecord.init();
					outRecord.reset();
				}
			}
			return outRecord;
		}

		/**
		 * Sets an output record.
		 * 
		 * @param outRecord
		 */
		public void setOutRecord(DataRecord outRecord) {
			this.outRecord = outRecord;
		}

		/**
		 * Gets parent key.
		 * 
		 * @return
		 */
		public String[] getParentKey() {
			return parentKey;
		}

		/**
		 * Sets parent key.
		 * 
		 * @param parentKey
		 */
		public void setParentKey(String[] parentKey) {
			this.parentKey = parentKey;
		}

		/**
		 * Gets a parent mapping.
		 * 
		 * @return
		 */
		public Mapping getParent() {
			return parent;
		}

		/**
		 * Sets a parent mapping.
		 * 
		 * @param parent
		 */
		public void setParent(Mapping parent) {
			this.parent = parent;
		}

		/**
		 * Gets a sequence name.
		 * 
		 * @return
		 */
		public String getSequenceField() {
			return sequenceField;
		}

		/**
		 * Sets a sequence name.
		 * 
		 * @param field
		 */
		public void setSequenceField(String field) {
			this.sequenceField = field;
		}

		/**
		 * Gets a sequence ID.
		 * 
		 * @return
		 */
		public String getSequenceId() {
			return sequenceId;
		}

		/**
		 * Sets a sequence ID.
		 * 
		 * @param id
		 */
		public void setSequenceId(String id) {
			this.sequenceId = id;
		}

		/**
		 * Gets a Sequence (simple sequence, db sequence, ...).
		 * 
		 * @return
		 */
		public Sequence getSequence() {
			if (sequence == null) {
				String element = StringUtils.normalizeName(StringUtils.trimXmlNamespace(getElement()));

				if (getSequenceId() == null) {
					sequence = new PrimitiveSequence(element, getGraph(), element);
				} else {
					sequence = getGraph().getSequence(getSequenceId());

					if (sequence == null) {
						LOG.warn(getId() + ": Sequence " + getSequenceId() + " does not exist in " + "transformation graph. Primitive sequence is used instead.");
						sequence = new PrimitiveSequence(element, getGraph(), element);
					}
				}
			}

			return sequence;
		}

		/**
		 * processSkipOrNumRecords is true - mapping can be skipped
		 */
		public boolean getProcessSkipOrNumRecords() {
			if (processSkipOrNumRecords)
				return true;
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
			processSkipOrNumRecords = parentMapping != null && parentMapping.getProcessSkipOrNumRecords() || (skipRecords4Mapping > 0 || numRecords4Mapping < Integer.MAX_VALUE);
		}

		/**
		 * Sets inner variables for bReset4CurrentRecord4Mapping.
		 */
		public void prepareReset4CurrentRecord4Mapping() {
			bReset4CurrentRecord4Mapping = processSkipOrNumRecords;
			if (childMap != null) {
				Mapping mapping;
				for (Iterator<Entry<String, Mapping>> it = childMap.entrySet().iterator(); it.hasNext();) {
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
		 * 
		 * @param skipRecords4Mapping
		 */
		public void setSkipRecords4Mapping(int skipRecords4Mapping) {
			this.skipRecords4Mapping = skipRecords4Mapping;
		}

		/**
		 * numRecords for this mapping.
		 * 
		 * @param numRecords4Mapping
		 */
		public void setNumRecords4Mapping(int numRecords4Mapping) {
			this.numRecords4Mapping = numRecords4Mapping;
		}

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
			if (!bReset4CurrentRecord4Mapping)
				return;
			if (childMap != null) {
				Mapping mapping;
				for (Iterator<Entry<String, Mapping>> it = childMap.entrySet().iterator(); it.hasNext();) {
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
			if (!processSkipOrNumRecords) {
				return;
			}

			Mapping parent = getParent();
			bDoMap = (parent == null || parent.doMap()) && currentRecord4Mapping >= skipRecords4Mapping && currentRecord4Mapping - skipRecords4Mapping < numRecords4Mapping;
			if (childMap != null) {
				Mapping mapping;
				for (Iterator<Entry<String, Mapping>> it = childMap.entrySet().iterator(); it.hasNext();) {
					mapping = it.next().getValue();
					mapping.prepareDoMap();
				}
			}
		}

		/**
		 * Can process this mapping? It depends on currentRecord4Mapping, skipRecords4Mapping and numRecords4Mapping for
		 * this and parent mappings.
		 * 
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
				ancestorFieldReference.ancestor.descendantReferrences.put(ancestorFieldReference.ancestorField, null);
			}
			explicitCloverFields.add(ancestorFieldReference.currentField);
		}

		public List<AncestorFieldMapping> getFieldsFromAncestor() {
			return fieldsFromAncestor;
		}

		public boolean hasFieldsFromAncestor() {
			return fieldsFromAncestor != null && !fieldsFromAncestor.isEmpty();
		}

		private void addAcestorFieldMapping(String ancestorFieldRef, String currentField) {
			String ancestorField = ancestorFieldRef;
			ancestorField = normalizeAncestorValueRef(ancestorField);
			Mapping ancestor = this;
			while (ancestorField.startsWith(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR)) {
				ancestor = ancestor.getParent();
				if (ancestor == null) {
					// User may want this in template declaration
					LOG.debug("Invalid ancestor XML field reference " + ancestorFieldRef + " in mapping of element <" + this.getElement() + ">");
					break;
				}
				ancestorField = ancestorField.substring(PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR.length());
			}
			if (ancestor != null) {
				addAncestorField(new AncestorFieldMapping(ancestor, ancestorField, currentField, ancestorFieldRef));
			} else {
				// This AncestorFieldMapping makes sense in templates - invalid ancestor reference may become valid in
				// template reference
				addAncestorField(new AncestorFieldMapping(null, null, currentField, ancestorFieldRef));
			}
		}

		/**
		 * If <code>ancestorField</code> is reference to ancestor element value, returns its normalized version,
		 * otherwise returns unchanged original parameter. Normalized ancestor field reference always ends with "../.":
		 * suffix. Valid unnormalized ancestor element value references are i.e.: ".." or "../"
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

	public static class AncestorFieldMapping {
		final Mapping ancestor;
		final String ancestorField;
		final String currentField;
		final String originalFieldReference;

		public AncestorFieldMapping(Mapping ancestor, String ancestorField, String currentField,
				String originalFieldReference) {
			this.ancestor = ancestor;
			this.ancestorField = ancestorField;
			this.currentField = currentField;
			this.originalFieldReference = originalFieldReference;
		}
	}
}
