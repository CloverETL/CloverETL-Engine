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
package org.jetel.data.xml.mapping;

import java.util.Arrays;



/**  Represents a mapping from an XML structure to a clover record.
 *  Describes how a data (be it attributes, nested elements or text value) from given XML element are mapped to a 
 *  fields of clover record.  
 * 
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 22.6.2012
 */
public class XMLElementMappingDefinition extends XMLMappingDefinition {

	/**
	 * Name of the XML element to be mapped. 
	 */
	private String elementName;
	private String elementNameSource;
	
	/**
	 * Output port for the output record. 
	 */
	private int outputPort = -1;
	private String outputPortSource;
	
	/**
	 * Output fields, where the content of the mapped elements should be stored. 
	 */
	private String[] outputFields;
	private String outputFieldsSource;

	/**
	 * Names of the XML elements containing the data to be mapped.
	 */
	private String[] xmlElements;
	private String xmlElementsSource;

	/**
	 * 	Represents a generated key.
	 */
	private String[] generatedKey;
	private String generatedKeySource;
	
	/**
	 * Represents a parent key. 	
	 */
	private String[] parentKey;
	private String parentKeySource;

	/**
	 * 	Represents a field, where the sequence number should be stored.
	 */
	private String sequenceField;
	
	/**
	 * 	Represents a sequence ID to be used.
	 */
	private String sequenceID;
	
	/**
	 * 	Number of records to skip.
	 */
	private int skipRecordsCount = 0;
	private String skipRecordsCountSource;

	/**
	 * 	Limit on number of records parsed. 
	 */
	private int recordCountLimit = Integer.MAX_VALUE;
	private String recordCountSource;

	/**
	 * Template ID. Mapping can define a template ID, that will serve as a identifier of this mapping.
	 * It can be then referenced by other mappings, that will reflect the properties from this template. 
	 */
	private String templateID;
	
	/**
	 * Template reference ID. When set, this mapping will behave as a mapping with given templateID.
	 */
	private String templateReference;
	
	/**
	 * When referencing a template from a nested element of that template an infinite reference loop will be created. 
	 * This attribute serves to break this infinite reference loop by using only a specified number of recursive steps.
	 */
	private int templateNestedDepth = -1;
	private String templateNestedDepthSource;
	
	/**
	 * Flag indicating, whether the mapping fills parent record. 
	 * If this attribute is <code>true</code>, the element will supply the output record with data, but will not 
	 * be a final mapping - the output record will not be send. The data record being filled with data is the 
	 * record of the nearest parent with output port defined. 
	 */
	private boolean usingParentRecord = false;
	private String usingParentRecordSource;
	
	/**
	 * Flag indicating, whether the implicit mapping should be used. 
	 */
	private boolean implicit = true;
	private String implicitSource;
	
	
	/**
	 * Creates a new instance of the element to record mapping as root.
	 */
	public XMLElementMappingDefinition() {
		super();
	}

	/**
	 * Creates a new instance of the element to record mapping with given parent.
	 * 
	 * @param parent
	 */
	public XMLElementMappingDefinition(XMLMappingDefinition parent) {
		super(parent);
	}

	/** Copies the attributes of this mapping to a given mapping.
	 * 
	 * @param mapping
	 */
	public void copyTo(XMLElementMappingDefinition mapping) {
		super.copyTo(mapping);
		
		mapping.setElementName(elementName);
		mapping.setOutputPort(outputPort);
		mapping.setParentKey(parentKey == null ? null : Arrays.copyOf(parentKey, parentKey.length));
		mapping.setGeneratedKey(generatedKey == null ? null : Arrays.copyOf(generatedKey, generatedKey.length));
		mapping.setSequenceField(sequenceField);
		mapping.setSequenceID(sequenceID);
		mapping.setSkipRecordsCount(skipRecordsCount);
		mapping.setRecordCountLimit(recordCountLimit);
		mapping.setOutputFields(outputFields == null ? null : Arrays.copyOf(outputFields, outputFields.length));
		mapping.setXmlElements(xmlElements == null ? null : Arrays.copyOf(xmlElements, xmlElements.length));
		mapping.setUsingParentRecord(usingParentRecord);
	}

	@Override
	public XMLMappingDefinition createCopy() {
		XMLElementMappingDefinition mapping = new XMLElementMappingDefinition();
		this.copyTo(mapping);
		
		return mapping;
	}

	public String getElementName() {
		return elementName;
	}

	public void setElementName(String elementName) {
		this.elementName = elementName;
	}

	public int getOutputPort() {
		return outputPort;
	}

	public void setOutputPort(int outputPort) {
		this.outputPort = outputPort;
	}
	
	public String[] getOutputFields() {
		return outputFields;
	}

	public void setOutputFields(String[] outputFields) {
		this.outputFields = outputFields;
	}

	public String[] getXmlElements() {
		return xmlElements;
	}

	public void setXmlElements(String[] xmlElements) {
		this.xmlElements = xmlElements;
	}

	public String[] getGeneratedKey() {
		return generatedKey;
	}

	public void setGeneratedKey(String[] generatedKey) {
		this.generatedKey = generatedKey;
	}

	public String[] getParentKey() {
		return parentKey;
	}

	public void setParentKey(String[] parentKey) {
		this.parentKey = parentKey;
	}

	public String getSequenceField() {
		return sequenceField;
	}

	public void setSequenceField(String sequenceField) {
		this.sequenceField = sequenceField;
	}

	public String getSequenceID() {
		return sequenceID;
	}

	public void setSequenceID(String sequenceID) {
		this.sequenceID = sequenceID;
	}

	public int getSkipRecordsCount() {
		return skipRecordsCount;
	}

	public void setSkipRecordsCount(int skipRecordsCount) {
		this.skipRecordsCount = skipRecordsCount;
	}

	public int getRecordCountLimit() {
		return recordCountLimit;
	}

	public void setRecordCountLimit(int recordCountLimit) {
		this.recordCountLimit = recordCountLimit;
	}

	public String getTemplateID() {
		return templateID;
	}

	public void setTemplateID(String templateID) {
		this.templateID = templateID;
	}

	public String getTemplateReference() {
		return templateReference;
	}

	public void setTemplateReference(String templateReference) {
		this.templateReference = templateReference;
	}

	public int getTemplateNestedDepth() {
		return templateNestedDepth;
	}

	public void setTemplateNestedDepth(int templateNestedDepth) {
		this.templateNestedDepth = templateNestedDepth;
	}

	public String getOutputPortSource() {
		return outputPortSource;
	}

	public void setOutputPortSource(String outputPortSource) {
		this.outputPortSource = outputPortSource;
	}

	public String getOutputFieldsSource() {
		return outputFieldsSource;
	}

	public void setOutputFieldsSource(String outputFieldsSource) {
		this.outputFieldsSource = outputFieldsSource;
	}

	public String getXmlElementsSource() {
		return xmlElementsSource;
	}

	public void setXmlElementsSource(String xmlElementsSource) {
		this.xmlElementsSource = xmlElementsSource;
	}

	public String getGeneratedKeySource() {
		return generatedKeySource;
	}

	public void setGeneratedKeySource(String generatedKeySource) {
		this.generatedKeySource = generatedKeySource;
	}

	public String getParentKeySource() {
		return parentKeySource;
	}

	public void setParentKeySource(String parentKeySource) {
		this.parentKeySource = parentKeySource;
	}

	public String getSkipRecordsCountSource() {
		return skipRecordsCountSource;
	}

	public void setSkipRecordsCountSource(String skipRecordsCountSource) {
		this.skipRecordsCountSource = skipRecordsCountSource;
	}

	public String getRecordCountSource() {
		return recordCountSource;
	}

	public void setRecordCountSource(String recordCountSource) {
		this.recordCountSource = recordCountSource;
	}

	public String getTemplateNestedDepthSource() {
		return templateNestedDepthSource;
	}

	public void setTemplateNestedDepthSource(String templateNestedDepthSource) {
		this.templateNestedDepthSource = templateNestedDepthSource;
	}

	public String getElementNameSource() {
		return elementNameSource;
	}

	public void setElementNameSource(String elementNameSource) {
		this.elementNameSource = elementNameSource;
	}

	public boolean isUsingParentRecord() {
		return usingParentRecord;
	}

	public void setUsingParentRecord(boolean usingParentRecord) {
		this.usingParentRecord = usingParentRecord;
	}

	public String getNestedSource() {
		return usingParentRecordSource;
	}

	public void setNestedSource(String usingParentRecordSource) {
		this.usingParentRecordSource = usingParentRecordSource;
	}

	public boolean isImplicit() {
		return implicit;
	}

	public void setImplicit(boolean implicit) {
		this.implicit = implicit;
	}

	public String getImplicitSource() {
		return implicitSource;
	}

	public void setImplicitSource(String implicitSource) {
		this.implicitSource = implicitSource;
	}
}
