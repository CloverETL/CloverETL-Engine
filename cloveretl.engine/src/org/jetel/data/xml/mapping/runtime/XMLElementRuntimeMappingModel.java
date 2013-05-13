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
package org.jetel.data.xml.mapping.runtime;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.RecordTransform;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.data.xml.mapping.XMLMappingConstants;
import org.jetel.data.xml.mapping.XMLMappingContext;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.XmlUtils;
import org.jetel.util.string.StringUtils;


/** Class representing a mapping of XML element onto a data record.
 * 
 * TODO: This class should be split into a hierarchy of classes, representing a mapping type (e.g. xml field -> clover field mapping vs. input field->output field mapping)
 * 
 * @author ?, Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 20.6.2012
 */
public class XMLElementRuntimeMappingModel extends XMLRuntimeMappingModel {
	private static final Log logger = LogFactory.getLog(XMLElementRuntimeMappingModel.class);

    public static final String XML_TEMPLATE_ID = "templateId";
    public static final String XML_TEMPLATE_REF = "templateRef";
    public static final String XML_TEMPLATE_DEPTH = "nestedDepth";	
	
	
	// === Mapping definition ===
	
	/** 
	 * Name of the XML element to be mapped.
	 */
	private String elementName;
	
	/**
	 * Output port on which the data record should be send.
	 */
	private int outputPortNumber;
	
	/**
	 * Output record to be filled with this mapping
	 */
	private DataRecord outputRecord;
	
	/**
	 * Names of the parent key fields.
	 */
	private String[] parentKeyFields;
	
	/** 
	 * Names of the fields where the generated key is stored;
	 */
	private String[] generatedKeyFields;
	
	/**
	 * Map associating an element name with a child mapping. Used to process nested mappings. 
	 */
	private Map<String, XMLElementRuntimeMappingModel> children;

	/**
	 * Reference to a parent mapping.
	 */
	private WeakReference<XMLElementRuntimeMappingModel> parent;
	
	/**
	 * Nested mapping depth in the mapping definition.
	 */
	private int level;
	
	/**
	 * Field where a generated sequence number should be stored.
	 */
	private String sequenceField;
	
	/**
	 * Sequence to be used for generating key.
	 */
	private String sequenceId;
	private Sequence sequence;

	/** 
	 * Map associating XML element/attribute/input field name to an output field name. 
	 */ 
	private Map<String, String> fieldsMap = new HashMap<String, String>();

	/** 
	 * List of clover fields (among else) which will be filled from ancestor.
	 */
	private List<AncestorFieldMapping> fieldsFromAncestor;

	/** 
	 * Map associating an XML element/attribute name with Clover field name; these XML fields are referenced by descendant mappings. 
	 */
	private Map<String, String> descendantReferences = new HashMap<String, String>();

	/**
	 * Set of Clover fields which are mapped explicitly (using xmlFields & cloverFields attributes). It is union of
	 * xml2CloverFieldsMap.values() and Clover fields from fieldsFromAncestor list. Its purpose: quick lookup
	 */
	private Set<String> explicitCloverFields = new HashSet<String>();
	
	/**
	 * Number of records to be skipped in the parsed XML.
	 */
	private int skipRecordsCount;
	
	/**
	 * Number of records to be extracted.
	 */
	private int recordsCountLimit = Integer.MAX_VALUE;
	
	/**
	 * Flag indicating, whether this mapping is an input->output mapping
	 */
	private boolean isInputOutputMapping;
	
	
	// === State data ===

	/**
	 * Counter for number of records produced by this mapping.
	 */
	private int currentRecord;
	
	/**
	 * Flag indicating, whether we should take skip and record count into account
	 */
	private boolean processSkipOrLimitRecords; 
	
	/**
	 * Flag indicating, whether we should map processed element. Depends on processSkipOrNumRecords.
	 */
	private boolean doMap = true;
	
	/**
	 * Flag indicating, whether we should reset nested mappings
	 */
	private boolean resetNestedMappings;
	
	/**
	 * Input field -> output field transformation.
	 */
	private RecordTransform fieldTransformation;
	
	/**
	 * Represents a nearest parent mapping, that produces a record.
	 */
	private XMLElementRuntimeMappingModel producingParent;
	
	/**
	 * Flag indicating whether the mapping should be treated as nested.
	 */
	private boolean usingParentRecord = false;

	/**
	 * Flag indicating whether the implicit mapping should used.
	 */
	private boolean implicit = true;
	
	
	/**
	 * Flag indicating whether the element text content has already been processed
	 */
	private boolean charactersProcessed = false;
	
	
	private String[] subtreeKeys = null;
	/** Class representing an anector field mapping.
	 * 
	 * @author Unkown, Tomas Laurincik (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created 20.6.2012
	 */
	public static class AncestorFieldMapping {
		/**
		 * Ancestor mapping, from which the field will be gathered.
		 */
		private final XMLElementRuntimeMappingModel ancestor;
		
		/**
		 * Ancestor field to be mapped.
		 */
		private final String ancestorField;
		
		/**
		 * Current field.
		 */
		private final String currentField;
		
		/**
		 * Original field reference.
		 */
		private final String originalFieldReference;

		public AncestorFieldMapping(XMLElementRuntimeMappingModel ancestor, String ancestorField, String currentField,
				String originalFieldReference) {
			this.ancestor = ancestor;
			this.ancestorField = ancestorField;
			this.currentField = currentField;
			this.originalFieldReference = originalFieldReference;
		}

		public XMLElementRuntimeMappingModel getAncestor() {
			return ancestor;
		}

		public String getAncestorField() {
			return ancestorField;
		}

		public String getCurrentField() {
			return currentField;
		}

		public String getOriginalFieldReference() {
			return originalFieldReference;
		}
	}

	/** Creates empty instance of the element mapping 
	 * 
	 * @param context
	 */
	public XMLElementRuntimeMappingModel(XMLMappingContext context) {
		super(context);
	}
	
	/** Copy constructor - created a deep copy of all attributes and children elements
	 * 
	 * @param otherMapping - mapping to be copied.
	 * @param parent - parent mapping.
	 */
	public XMLElementRuntimeMappingModel(XMLElementRuntimeMappingModel otherMapping, XMLElementRuntimeMappingModel parent) {
		super (otherMapping, parent);
		
		this.elementName = otherMapping.elementName;
		this.outputPortNumber = otherMapping.outputPortNumber;
		this.parentKeyFields = otherMapping.parentKeyFields == null ? null : Arrays.copyOf(otherMapping.parentKeyFields, otherMapping.parentKeyFields.length);
		this.generatedKeyFields = otherMapping.generatedKeyFields == null ? null : Arrays.copyOf(otherMapping.generatedKeyFields, otherMapping.generatedKeyFields.length);
		this.sequenceField = otherMapping.sequenceField;
		this.sequenceId = otherMapping.sequenceId;
		this.skipRecordsCount = otherMapping.skipRecordsCount;
		this.recordsCountLimit = otherMapping.recordsCountLimit;
		this.fieldsMap = new HashMap<String, String>(otherMapping.fieldsMap);

		// Create deep copy of children elements
		if (otherMapping.children != null) {
			this.children = new HashMap<String, XMLElementRuntimeMappingModel>();
			for (String key : otherMapping.children.keySet()) {
				final XMLElementRuntimeMappingModel child = new XMLElementRuntimeMappingModel(otherMapping.children.get(key), this);
				this.children.put(key, child);
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
	public XMLElementRuntimeMappingModel(String elementName, int outPort, XMLElementRuntimeMappingModel parent, XMLMappingContext context) {
		super(context);
		
		this.elementName = elementName;
		this.outputPortNumber = outPort;
		this.parent = new WeakReference<XMLElementRuntimeMappingModel>(parent);
		if (parent != null) {
			parent.addChildMapping(this);
		}
	}

	/**
	 * Gives the optional attributes parentKey and generatedKey.
	 */
	public XMLElementRuntimeMappingModel(String element, int outPort, String parentKey[], String[] generatedKey, XMLElementRuntimeMappingModel parent, XMLMappingContext context) {
		this(element, outPort, parent, context);

		this.parentKeyFields = parentKey;
		this.generatedKeyFields = generatedKey;
	}

	/**
	 * Gets direct children for this mapping.
	 * 
	 * @param element
	 * @return
	 */
	public XMLElementRuntimeMappingModel getChildMapping(String element) {
		if (children == null) {
			return null;
		}
		return children.get(element);
	}

	/**
	 * Adds a direct child for this mapping.
	 * 
	 * @param mapping
	 */
	public void addChildMapping(XMLElementRuntimeMappingModel mapping) {
		if (children == null) {
			children = new HashMap<String, XMLElementRuntimeMappingModel>();
		}
		children.put(mapping.getElementName(), mapping);
	}

	/**
	 * Removes a direct child for this mapping.
	 * 
	 * @param mapping
	 */
	public void removeChildMapping(XMLElementRuntimeMappingModel mapping) {
		if (children == null) {
			return;
		}
		children.remove(mapping.getElementName());
	}

	/** Associates given source field (XML field, input field) to a given target field. 
	 * 
	 * @param sourceField
	 * @param targetField
	 */
	public void putFieldMap(String sourceField, String targetField) {
		fieldsMap.put(XmlUtils.createQualifiedName(sourceField, getContext().getNamespaceBindings()), targetField);
		explicitCloverFields.add(targetField);
	}
	
	protected OutputPort getOutputPort(int outPortIndex) {
		return getContext().getParentComponent().getOutputPort(outPortIndex);
	}
	
	/**
	 * Gets an output record.
	 * 
	 * @return
	 */
	public DataRecord getOutputRecord() {
		// are we contributing to a parent record? If so, return that one.
		if (usingParentRecord && producingParent != null) {
			return producingParent.getOutputRecord();
		} 
		
		// otherwise create a record for this mapping
		if (outputRecord == null) {
			OutputPort outPort = getOutputPort(getOutputPortNumber());
			if (outPort != null) {
				DataRecordMetadata dataRecordMetadata = outPort.getMetadata();
				getContext().getAutoFilling().addAutoFillingFields(dataRecordMetadata);
				outputRecord = DataRecordFactory.newRecord(dataRecordMetadata);
				outputRecord.init();
				outputRecord.reset();
			} // Original code is commented, it is valid to have null port now
			/*
			 * else { LOG .warn(getId() + ": Port " + getOutPort() +
			 * " does not have an edge connected.  Please connect the edge or remove the mapping."); }
			 */
		}

		return outputRecord;
	}

	/**
	 * Gets a parent mapping.
	 * 
	 * @return
	 */
	public XMLElementRuntimeMappingModel getParent() {
		if (parent != null) {
			return parent.get();
		} else {
			return null;
		}
	}

	/**
	 * Sets a parent mapping.
	 * 
	 * @param parent
	 */
	public void setParent(XMLElementRuntimeMappingModel parent) {
		this.parent = new WeakReference<XMLElementRuntimeMappingModel>(parent);
	}

	/** Creates a primitive sequence.
	 * 
	 * @param id - id of the sequence
	 * @param name - name of the sequence
	 * @return new primitive sequence
	 */
	protected Sequence createPrimitiveSequence(String id, String name) {
		TransformationGraph graph = getContext().getGraph();
		return SequenceFactory.createSequence(graph, XMLMappingConstants.PRIMITIVE_SEQUENCE, new Object[] { id, graph, name}, new Class[] { String.class, TransformationGraph.class, String.class });
	}	
	
	/**
	 * Gets a Sequence (simple sequence, db sequence, ...).
	 * 
	 * @return
	 */
	public Sequence getSequence() {
		if (sequence == null) {
			String element = StringUtils.normalizeName(StringUtils.trimXmlNamespace(getElementName()));

			if (getSequenceId() == null) {
				sequence = createPrimitiveSequence(element, element);
			} else {
				sequence = getContext().getGraph().getSequence(getSequenceId());

				if (sequence == null) {
					logger.warn(getContext().getParentComponent().getId() + ": Sequence " + getSequenceId() + " does not exist in " + "transformation graph. Primitive sequence is used instead.");
					sequence = createPrimitiveSequence(element, element);
				}
			}
		}

		return sequence;
	}

	/**
	 * processSkipOrNumRecords is true - mapping can be skipped
	 */
	public boolean getProcessSkipOrNumRecords() {
		if (processSkipOrLimitRecords)
			return true;
		XMLElementRuntimeMappingModel parent = getParent();
		if (parent == null) {
			return processSkipOrLimitRecords;
		}
		return parent.getProcessSkipOrNumRecords();
	}

	/**
	 * Sets inner variables for processSkipOrNumRecords.
	 */
	public void prepareProcessSkipOrNumRecords() {
		XMLElementRuntimeMappingModel parentMapping = getParent();
		processSkipOrLimitRecords = parentMapping != null && parentMapping.getProcessSkipOrNumRecords() || (skipRecordsCount > 0 || recordsCountLimit < Integer.MAX_VALUE);
	}

	/**
	 * Sets inner variables for bReset4CurrentRecord4Mapping.
	 */
	public void prepareReset4CurrentRecord4Mapping() {
		resetNestedMappings = processSkipOrLimitRecords;
		if (children != null) {
			XMLElementRuntimeMappingModel mapping;
			for (Iterator<Entry<String, XMLElementRuntimeMappingModel>> it = children.entrySet().iterator(); it.hasNext();) {
				mapping = it.next().getValue();
				if (mapping.processSkipOrLimitRecords) {
					resetNestedMappings = true;
					break;
				}
			}
		}
	}

	/**
	 * Counter for this mapping.
	 */
	public void incCurrentRecord4Mapping() {
		currentRecord++;
	}

	/**
	 * Resets submappings.
	 */
	public void resetCurrentRecord4ChildMapping() {
		if (!resetNestedMappings)
			return;
		if (children != null) {
			XMLElementRuntimeMappingModel mapping;
			for (Iterator<Entry<String, XMLElementRuntimeMappingModel>> it = children.entrySet().iterator(); it.hasNext();) {
				mapping = it.next().getValue();
				mapping.currentRecord = 0;
				mapping.resetCurrentRecord4ChildMapping();
			}
		}
	}

	/**
	 * Sets if this and child mapping should be skipped.
	 */
	public void prepareDoMap() {
		if (!processSkipOrLimitRecords)
			return;
		XMLElementRuntimeMappingModel parent = getParent();
		doMap = (parent == null || parent.doMap()) && currentRecord >= skipRecordsCount && currentRecord - skipRecordsCount < recordsCountLimit;
		if (children != null) {
			XMLElementRuntimeMappingModel mapping;
			for (Iterator<Entry<String, XMLElementRuntimeMappingModel>> it = children.entrySet().iterator(); it.hasNext();) {
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
		return !processSkipOrLimitRecords || (processSkipOrLimitRecords && doMap);
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

	public void addAncestorFieldMapping(String ancestorFieldRef, String currentField) {
		String ancestorField = ancestorFieldRef;
		ancestorField = normalizeAncestorValueRef(ancestorField);
		XMLElementRuntimeMappingModel ancestor = this;
		while (ancestorField.startsWith(XMLMappingConstants.PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR)) {
			ancestor = ancestor.getParent();
			if (ancestor == null) {
				// User may want this in template declaration
				logger.warn("Invalid ancestor XML field reference " + ancestorFieldRef + " in mapping of element <" + this.getElementName() + ">");
				break;
			}
			ancestorField = ancestorField.substring(XMLMappingConstants.PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR.length());
		}

		// After the ancestor prefix has been stripped, process the namespace
		ancestorField = XmlUtils.createQualifiedName(ancestorField, getContext().getNamespaceBindings());
		if (ancestor != null) {
			addAncestorField(new AncestorFieldMapping(ancestor, ancestorField, currentField, ancestorFieldRef));
		} else {
			// This AncestorFieldMapping makes sense in templates - invalid ancestor reference may become valid in
			// template reference
			addAncestorField(new AncestorFieldMapping(null, null, currentField, ancestorFieldRef));
		}
	}

	/**
	 * If <code>ancestorField</code> is reference to ancestor element value, returns its normalized version, otherwise
	 * returns unchanged original parameter. Normalized ancestor field reference always ends with "../.": suffix. Valid
	 * unnormalized ancestor element value references are i.e.: ".." or "../"
	 */
	private String normalizeAncestorValueRef(String ancestorField) {
		if (XMLMappingConstants.PARENT_MAPPING_REFERENCE_PREFIX.equals(ancestorField)) {
			return XMLMappingConstants.PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR + XMLMappingConstants.ELEMENT_VALUE_REFERENCE;
		}

		if (ancestorField.startsWith(XMLMappingConstants.PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR)) {
			if (ancestorField.endsWith(XMLMappingConstants.PARENT_MAPPING_REFERENCE_PREFIX)) {
				ancestorField += XMLMappingConstants.PARENT_MAPPING_REFERENCE_SEPARATOR + XMLMappingConstants.ELEMENT_VALUE_REFERENCE;
			} else if (ancestorField.endsWith(XMLMappingConstants.PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR)) {
				ancestorField += XMLMappingConstants.ELEMENT_VALUE_REFERENCE;
			}
		}
		return ancestorField;
	}
	
	// === getters and setters

	public String getElementName() {
		return elementName;
	}

	public void setElementName(String elementName) {
		this.elementName = elementName;
	}

	public int getOutputPortNumber() {
		return outputPortNumber;
	}

	public void setOutputPortNumber(int outputPortNumber) {
		this.outputPortNumber = outputPortNumber;
	}

	public void setOutputRecord(DataRecord outputRecord) {
		this.outputRecord = outputRecord;
	}

	public String[] getParentKeyFields() {
		return parentKeyFields;
	}

	public void setParentKeyFields(String[] parentKeyFields) {
		this.parentKeyFields = parentKeyFields;
	}

	public String[] getGeneratedKeyFields() {
		return generatedKeyFields;
	}

	public void setGeneratedKeyFields(String[] generatedKeyFields) {
		this.generatedKeyFields = generatedKeyFields;
	}

	public Map<String, XMLElementRuntimeMappingModel> getChildren() {
		return children;
	}

	public void setChildren(Map<String, XMLElementRuntimeMappingModel> children) {
		this.children = children;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public String getSequenceField() {
		return sequenceField;
	}

	public void setSequenceField(String sequenceField) {
		this.sequenceField = sequenceField;
	}

	public String getSequenceId() {
		return sequenceId;
	}

	public void setSequenceId(String sequenceId) {
		this.sequenceId = sequenceId;
	}

	public Map<String, String> getFieldsMap() {
		return fieldsMap;
	}
	
	public String[] getSubtreeKeys() {
		if(this.subtreeKeys==null) {
			List<String> tmpList = new ArrayList<String>(4);
			if(this.fieldsMap!=null) {
				if(this.fieldsMap.containsKey(XMLMappingConstants.ELEMENT_VALUE_REFERENCE)) {tmpList.add(XMLMappingConstants.ELEMENT_VALUE_REFERENCE);}
				if(this.fieldsMap.containsKey(XMLMappingConstants.ELEMENT_CONTENTS_AS_TEXT)) {tmpList.add(XMLMappingConstants.ELEMENT_CONTENTS_AS_TEXT);}
				if(this.fieldsMap.containsKey(XMLMappingConstants.ELEMENT_AS_TEXT)) {tmpList.add(XMLMappingConstants.ELEMENT_AS_TEXT);}
				subtreeKeys = tmpList.toArray(new String[]{});
			}else{
				subtreeKeys = new String[] {};
			}
		}
		return subtreeKeys;
	}

	public void setFieldsMap(Map<String, String> fieldsMap) {
		this.fieldsMap = fieldsMap;
	}

	public Map<String, String> getDescendantReferences() {
		return descendantReferences;
	}

	public void setDescendantReferences(Map<String, String> descendantReferences) {
		this.descendantReferences = descendantReferences;
	}

	public Set<String> getExplicitCloverFields() {
		return explicitCloverFields;
	}

	public void setExplicitCloverFields(Set<String> explicitCloverFields) {
		this.explicitCloverFields = explicitCloverFields;
	}

	public int getSkipRecordsCount() {
		return skipRecordsCount;
	}

	public void setSkipRecordsCount(int skipRecordsCount) {
		this.skipRecordsCount = skipRecordsCount;
	}

	public int getRecordsCountLimit() {
		return recordsCountLimit;
	}

	public void setRecordsCountLimit(int recordsCountLimit) {
		this.recordsCountLimit = recordsCountLimit;
	}

	public int getCurrentRecord() {
		return currentRecord;
	}

	public void setCurrentRecord(int currentRecord) {
		this.currentRecord = currentRecord;
	}

	public boolean isProcessSkipOrLimitRecords() {
		return processSkipOrLimitRecords;
	}

	public void setProcessSkipOrLimitRecords(boolean processSkipOrLimitRecords) {
		this.processSkipOrLimitRecords = processSkipOrLimitRecords;
	}

	public boolean isResetNestedMappings() {
		return resetNestedMappings;
	}

	public void setResetNestedMappings(boolean resetNestedMappings) {
		this.resetNestedMappings = resetNestedMappings;
	}

	public boolean isInputOutputMapping() {
		return isInputOutputMapping;
	}

	public void setInputOutputMapping(boolean isInputOutputMapping) {
		this.isInputOutputMapping = isInputOutputMapping;
	}

	public RecordTransform getFieldTransformation() {
		return fieldTransformation;
	}

	public void setFieldTransformation(RecordTransform fieldTransformation) {
		this.fieldTransformation = fieldTransformation;
	}

	public XMLElementRuntimeMappingModel getProducingParent() {
		return producingParent;
	}

	public void setProducingParent(XMLElementRuntimeMappingModel producingParent) {
		this.producingParent = producingParent;
	}

	public boolean isUsingParentRecord() {
		return usingParentRecord;
	}

	public void setUsingParentRecord(boolean nested) {
		this.usingParentRecord = nested;
	}

	public boolean isImplicit() {
		return implicit;
	}

	public void setImplicit(boolean implicit) {
		this.implicit = implicit;
	}

	public boolean isCharactersProcessed() {
		return charactersProcessed;
	}

	public void setCharactersProcessed(boolean charactersProcessed) {
		this.charactersProcessed = charactersProcessed;
	}
	
	
}