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
package org.jetel.data.xml.mapping.runtime.factory;

import java.util.LinkedList;
import java.util.List;

import org.jetel.component.RecordTransform;
import org.jetel.component.RecordTransformDescriptor;
import org.jetel.component.TransformFactory;
import org.jetel.data.xml.mapping.InputFieldMappingDefinition;
import org.jetel.data.xml.mapping.XMLElementMappingDefinition;
import org.jetel.data.xml.mapping.XMLMappingConstants;
import org.jetel.data.xml.mapping.XMLMappingContext;
import org.jetel.data.xml.mapping.XMLMappingDefinition;
import org.jetel.data.xml.mapping.runtime.XMLElementRuntimeMappingModel;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.graph.InputPort;
import org.jetel.graph.OutputPort;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.string.StringUtils;

/** Class responsible for building runtime mapping model. The model is created based on the mapping definition.
 *  
 * 
 * @author Tomas Laurincik (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 25.6.2012
 */
public class RuntimeMappingModelFactory {
	
	// === Mapping context ===
	
    /**
     * Context of the mapping.
     */
	private XMLMappingContext context;
		
	/**
	 * @param context
	 */
	public RuntimeMappingModelFactory(XMLMappingContext context) {
		super();
		this.context = context;
	}

	/** Creates a runtime mapping model.
	 * 
	 * @param mappingDefinition
	 * @param parent
	 * @param context
	 */
	public XMLElementRuntimeMappingModel createRuntimeMappingModel(XMLMappingDefinition mappingDefinition) {
		return createRuntimeMappingModel(mappingDefinition, null);
	}

	/** Creates a runtime mapping model.
	 * 
	 * @param mappingDefinition
	 * @param parent
	 * @param context
	 */
	public XMLElementRuntimeMappingModel createRuntimeMappingModel(XMLMappingDefinition definition, XMLElementRuntimeMappingModel parent) {
		if (definition instanceof XMLElementMappingDefinition) {
			XMLElementMappingDefinition mappingDefinition = (XMLElementMappingDefinition)definition;
			
			XMLElementRuntimeMappingModel mapping = new XMLElementRuntimeMappingModel(context);
			mapping.setParent(parent);
			mapping.setElementName(mappingDefinition.getElementName());
			mapping.setImplicit(mappingDefinition.isImplicit());
			mapping.setUsingParentRecord(mappingDefinition.isUsingParentRecord());
			mapping.setOutputPortNumber(mappingDefinition.getOutputPort());
			mapping.setParentKeyFields(mappingDefinition.getParentKey());
			mapping.setGeneratedKeyFields(mappingDefinition.getGeneratedKey());
			mapping.setSequenceField(mappingDefinition.getSequenceField());
			mapping.setSequenceId(mappingDefinition.getSequenceID());
			mapping.setSkipRecordsCount(mappingDefinition.getSkipRecordsCount());
			mapping.setRecordsCountLimit(mappingDefinition.getRecordCountLimit());

			// mapping between xml fields and clover fields initialization
			if (mappingDefinition.getOutputFields() != null && mappingDefinition.getXmlElements() != null) {
				String[] xmlFields = mappingDefinition.getXmlElements();
				String[] cloverFields = mappingDefinition.getOutputFields();

				for (int i = 0; i < xmlFields.length; i++) {
					if (xmlFields[i].startsWith(XMLMappingConstants.PARENT_MAPPING_REFERENCE_PREFIX_WITHSEPARATOR) || xmlFields[i].equals(XMLMappingConstants.PARENT_MAPPING_REFERENCE_PREFIX)) {
						mapping.addAncestorFieldMapping(xmlFields[i], cloverFields[i]);
					} else {
						mapping.putFieldMap(xmlFields[i], cloverFields[i]);
					}
				}
			}

			// prepare variables for skip and numRecords for this mapping
			mapping.prepareProcessSkipOrNumRecords();

			// expand template
			if (mappingDefinition.getTemplateReference() != null && mappingDefinition.getTemplateNestedDepth() != -1) {
				int depth = mappingDefinition.getTemplateNestedDepth() == -1? 1 : mappingDefinition.getTemplateNestedDepth();
				XMLElementRuntimeMappingModel currentMapping = mapping;
				while (depth > 0) {
					currentMapping = new XMLElementRuntimeMappingModel(currentMapping, currentMapping);
					currentMapping.prepareProcessSkipOrNumRecords();
					depth--;
				}
				while (currentMapping != mapping) {
					currentMapping.prepareReset4CurrentRecord4Mapping();
					currentMapping = currentMapping.getParent();
				}
			}

			List<InputFieldMappingDefinition> fieldMappings = new LinkedList<InputFieldMappingDefinition>();
			
			// Process all nested mappings
			for (XMLMappingDefinition recordMapping : mappingDefinition.getChildren()) {
				// if this is a definition of input field -> output field mapping, save it for later processing.
				if (recordMapping instanceof InputFieldMappingDefinition) {
					fieldMappings.add((InputFieldMappingDefinition) recordMapping);
					
				} else if (recordMapping instanceof XMLElementMappingDefinition) {
					mapping.addChildMapping(createRuntimeMappingModel(recordMapping, mapping));
				}
			}

			mapping.setFieldTransformation(buildTransformation(fieldMappings, mapping));
			
			// prepare variable reset of skip and numRecords' attributes
			mapping.prepareReset4CurrentRecord4Mapping();

			mapping.setProducingParent(findNearestProducingParent(mapping));
			
			return mapping;
		} 
		
		return null;
	}
	
	/** Tries to find a nearest parent that produces an output record.
	 * 
	 * @param mapping
	 * @return a nearest parent that produces an output record.
	 */
	private XMLElementRuntimeMappingModel findNearestProducingParent(XMLElementRuntimeMappingModel mapping) {
		if (mapping.getParent() == null) {
			return null;
		}
		
		if (mapping.getParent().getOutputPortNumber() != -1) {
			return mapping.getParent();
		}
		
		return findNearestProducingParent(mapping.getParent());
	}
	
	
	/** Builds a record transformation representing this mapping
	 * 
	 * @return a record transformation representing this mapping
	 */
	private RecordTransform buildTransformation(List<InputFieldMappingDefinition> inputFieldToRecordMappings, XMLElementRuntimeMappingModel parentRuntimeMapping) {
		if (inputFieldToRecordMappings.isEmpty()) {
			return null;
		}

		//we need to use port number 0 - this mini transformation has always one input and one output 
		String transformationCode = buildTransformationCode(inputFieldToRecordMappings, 0);
		
		OutputPort outPort = context.getParentComponent().getOutputPort(parentRuntimeMapping.getOutputPortNumber());
		InputPort inPort = context.getParentComponent().getInputPort(0);
		
		RecordTransform result = null;
		if (!StringUtils.isEmpty(transformationCode)) {
			try {
	        	TransformFactory<RecordTransform> transformFactory = TransformFactory.createTransformFactory(RecordTransformDescriptor.newInstance());
	        	transformFactory.setTransform(transformationCode);
	        	transformFactory.setComponent(context.getParentComponent());
	        	transformFactory.setInMetadata(inPort.getMetadata());
	        	transformFactory.setOutMetadata(outPort.getMetadata());
	        	result = transformFactory.createTransform();
			} catch (Exception e) {
				throw new JetelRuntimeException("Output mapping transformation is invalid", e); 
			}
			
			try {
				// initialise input mapping transformation
		        if (!result.init(null, new DataRecordMetadata[]{inPort.getMetadata()},  new DataRecordMetadata[]{outPort.getMetadata()})) {
		            throw new ComponentNotReadyException("Error initializing mapping");
		        }
			} catch (Exception e) {
				throw new JetelRuntimeException("Mapping tranformation initialization failed", e); 
			}
		}
		
		return result;
	}
	
	/** Builds a string representing a transformation described by this mapping.
	 * 
	 * @return a string representing a transformation described by this mapping.
	 */
	private String buildTransformationCode(List<InputFieldMappingDefinition> inputFieldToRecordMappings, int outputPort) {
		StringBuilder result = new StringBuilder();
		
		result.append("function integer transform() {").append("\n");
		
		for (InputFieldMappingDefinition fieldMapping : inputFieldToRecordMappings) {
			result.append("    $").append(outputPort).append(".").append(fieldMapping.getOutputField()).append("=").append("$0.").append(fieldMapping.getInputField()).append(";\n");
		}
		
		result.append("    return OK;").append("\n");
		result.append("}").append("\n");
		
		
		return result.toString();
	}	
}
