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

import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.validator.EngineGraphWrapper;
import org.jetel.component.validator.GraphWrapper;
import org.jetel.component.validator.ReadynessErrorAcumulator;
import org.jetel.component.validator.ValidationError;
import org.jetel.component.validator.ValidationErrorAccumulator;
import org.jetel.component.validator.ValidationGroup;
import org.jetel.component.validator.ValidationNode;
import org.jetel.component.validator.params.ValidationParamNode;
import org.jetel.component.validator.utils.ValidationRulesPersister;
import org.jetel.component.validator.utils.ValidationRulesPersisterException;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.DateDataField;
import org.jetel.data.Defaults;
import org.jetel.data.IntegerDataField;
import org.jetel.data.ListDataField;
import org.jetel.data.MapDataField;
import org.jetel.data.StringDataField;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPortDirect;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CTLMapping;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 * Component for validation incoming records against user defined set of rules.
 * Records are taken one by one, there is no inner state of this component.
 *
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 23.10.2012
 */
public class Validator extends Node {
	
	private final static Log logger = LogFactory.getLog(Validator.class);
	private final static String COMPONENT_TYPE = "VALIDATOR";
	
	public final static String XML_RULES_ATTRIBUTE = "rules";
	public final static String XML_EXTERNAL_RULES_URL_ATTRIBUTE = "externalRulesURL";
	public final static String XML_ERROR_MAPPING = "errorMapping";
	
	public final static String MAPPING_INPUT_RECORD = "record";
	public final static String MAPPING_INPUT_ERROR = "error";
	public final static String MAPPING_OUTPUT_RECORD = "output";
	
	public final static String ERROR_OUTPUT_METADATA_NAME = "Report";
	public final static String ERROR_OUTPUT_CODE = "code";
	public final static String ERROR_OUTPUT_MESSAGE = "message";
	public final static String ERROR_OUTPUT_NAME = "name";
	public final static String ERROR_OUTPUT_FIELDS = "fields";
	public final static String ERROR_OUTPUT_VALUES = "values";
	public final static String ERROR_OUTPUT_PARAMS = "params";
	public final static String ERROR_OUTPUT_GRAPH = "graph";
	public final static String ERROR_OUTPUT_SERIAL_NUMBER = "serial_number";
	public final static String ERROR_OUTPUT_CREATED = "created";
	
	public final static String CUSTOM_RULE_RESULT = "result";
	public final static String CUSTOM_RULE_MESSAGE = "message";
	
	private final static int INPUT_PORT = 0;
	private final static int VALID_OUTPUT_PORT = 0;
	private final static int INVALID_OUTPUT_PORT = 1;
	
	private String rules;
	private String externalRulesURL;
	private String errorMappingCode;
	
	private int processedRecords = 0;
	private ValidationGroup rootGroup;
	
	private CTLMapping errorMapping;
	private DataRecord inputRecord;
	private DataRecord errorRecord;
	private DataRecord outputRecord;
	
	/**
	 * Minimalistic constructor to ensure component name init
	 * @param id
	 */
	public Validator(String id) {
		super(id);
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}
	
	public static Node fromXML(TransformationGraph graph, Element xmlElement)throws XMLConfigurationException {
		Validator validator;
	
		ComponentXMLAttributes attrs = new ComponentXMLAttributes(xmlElement, graph);
		try {
			validator = new Validator(attrs.getString(XML_ID_ATTRIBUTE));
			validator.setGraph(graph);
			if(attrs.exists(XML_RULES_ATTRIBUTE)) {
				validator.setRules(attrs.getString(XML_RULES_ATTRIBUTE));
			}
			if(attrs.exists(XML_EXTERNAL_RULES_URL_ATTRIBUTE)) {
				validator.setExternalRulesURL(attrs.getString(XML_EXTERNAL_RULES_URL_ATTRIBUTE));
			}
			if(attrs.exists(XML_ERROR_MAPPING)) {
				validator.setErrorMappingCode(attrs.getString(XML_ERROR_MAPPING));
			}
			return validator;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ": Invalid XML configuration.", ex);
		}
	}
	private void setRules(String value) {
		rules = value;
	}
	
	private void setExternalRulesURL(String value) {
		externalRulesURL = value;
	}
	private void setErrorMappingCode(String value) {
		errorMappingCode = value;
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		InputPort inputPort = getInputPort(INPUT_PORT);
		if(inputPort == null || inputPort.getMetadata() == null) {
			ConfigurationProblem problem = new ConfigurationProblem("No input metadata.", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.HIGH);
			status.add(problem);
			return status;
		}
		
		String tempRules;
		
		if(externalRulesURL != null) {
			tempRules = FileUtils.getStringFromURL(getGraph().getRuntimeContext().getContextURL(), externalRulesURL,null);
		} else {
			tempRules = rules;
		}
		
		if(tempRules == null || tempRules.isEmpty()) {
			ConfigurationProblem problem = new ConfigurationProblem("No validation rules.", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.HIGH);
			status.add(problem);
			return status;
		}
		
		try {
			rootGroup = ValidationRulesPersister.deserialize(tempRules);
		} catch (ValidationRulesPersisterException e) {
			ConfigurationProblem problem = new ConfigurationProblem("Cannot parse validation rules.", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.HIGH);
			status.add(problem);
		}
		
		ReadynessErrorAcumulator accumulator = new ReadynessErrorAcumulator();
		if(rootGroup != null && !rootGroup.isReady(inputPort.getMetadata(), accumulator)) {
			String tempName = new String();
			for(Entry<ValidationParamNode, List<String>> errors: accumulator.getErrors().entrySet()) {
				for(String message : errors.getValue()) {
					if(accumulator.getParentRule(errors.getKey()) != null) {
						tempName = accumulator.getParentRule(errors.getKey()).getName() + ": ";
					}
					ConfigurationProblem problem = new ConfigurationProblem(tempName + message, ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.HIGH);
					status.add(problem);
				}
			}
		}
		
		if(!getInputPort(INPUT_PORT).getMetadata().equals(getOutputPort(INVALID_OUTPUT_PORT)) && (errorMappingCode == null || errorMappingCode.isEmpty())) {
			ConfigurationProblem problem = new ConfigurationProblem("Metadata on error output port differs from input metadata but no error mapping was provided.", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.HIGH);
			status.add(problem);
			return status;
		}
		
		if(errorMappingCode != null && !errorMappingCode.isEmpty() && getOutputPort(INVALID_OUTPUT_PORT) != null) {
			errorMapping = new CTLMapping("Error output", this);
			errorMapping.setTransformation(errorMappingCode);
			inputRecord = errorMapping.addInputMetadata(MAPPING_INPUT_RECORD, getInputPort(INPUT_PORT).getMetadata());
			errorRecord = errorMapping.addInputMetadata(MAPPING_INPUT_ERROR, createErrorOutputMetadata());
			errorMapping.addOutputMetadata("DUMMY", null);	// Dummy metadata to reach to error port 
			errorMapping.addOutputMetadata(MAPPING_OUTPUT_RECORD, getOutputPort(INVALID_OUTPUT_PORT).getMetadata());
			
			try {
				errorMapping.init(XML_ERROR_MAPPING);
			} catch(Exception e) {
				ConfigurationProblem problem = new ConfigurationProblem("Cannot initialize error output mapping", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.HIGH);
				status.add(problem);
				return status;
			}
			errorMapping.preExecute();
		} else {
			inputRecord = DataRecordFactory.newRecord(getInputPort(INPUT_PORT).getMetadata());
			inputRecord.init();
		}
		return status;
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		
		
	}

	@Override
	protected Result execute() throws Exception {
		logger.trace("Executing Validator component");
		
		// Prepare validation tree
		ValidationGroup root = rootGroup;
		
		// Prepare ports and structures for records
		InputPortDirect inPort = getInputPortDirect(INPUT_PORT);
		OutputPortDirect validPort = getOutputPortDirect(VALID_OUTPUT_PORT);
		OutputPortDirect invalidPort = getOutputPortDirect(INVALID_OUTPUT_PORT);
	
		CloverBuffer recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
	
		// Prepare provider for accessing graph
		GraphWrapper graphWrapper = new EngineGraphWrapper(getGraph());
		ValidationErrorAccumulator errorAccumulator = new ValidationErrorAccumulator();
		
		// Iterate over data
		boolean hasData = true;
		while(hasData && runIt) {
			errorAccumulator.reset();
			if(!inPort.readRecordDirect(recordBuffer)) {
				hasData = false;
				continue;
			}
			processedRecords++;
			logger.trace("Validation of record number " + processedRecords + " has started.");
			inputRecord.reset();
			inputRecord.init();
			inputRecord.deserialize(recordBuffer);
			if(root.isValid(inputRecord,errorAccumulator, graphWrapper) != ValidationNode.State.INVALID) {
				MiscUtils.sendRecordToPort(validPort, inputRecord);
				logger.trace("Record number " + processedRecords + " is VALID.");
			} else {
				if(invalidPort != null) {
					if(errorMapping != null) {
						for(ValidationError error : errorAccumulator) {
							populateErrorRecord(error);
							errorMapping.execute();
							MiscUtils.sendRecordToPort(invalidPort, errorMapping.getOutputRecord(MAPPING_OUTPUT_RECORD));
						}
					} else {
						MiscUtils.sendRecordToPort(invalidPort, inputRecord);	
					}
				}
				logger.trace("Record number " + processedRecords + " is INVALID.");
			}
			logger.trace("Validation of record number " + processedRecords + " has finished.");
		}
		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	public static DataRecordMetadata createErrorOutputMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(ERROR_OUTPUT_METADATA_NAME);
		metadata.addField(new DataFieldMetadata(ERROR_OUTPUT_CODE, DataFieldType.INTEGER, ""));
		metadata.addField(new DataFieldMetadata(ERROR_OUTPUT_SERIAL_NUMBER, DataFieldType.INTEGER, ""));
		metadata.addField(new DataFieldMetadata(ERROR_OUTPUT_MESSAGE, DataFieldType.STRING, ""));
		metadata.addField(new DataFieldMetadata(ERROR_OUTPUT_NAME, DataFieldType.STRING, ""));
		metadata.addField(new DataFieldMetadata(ERROR_OUTPUT_FIELDS, DataFieldType.STRING, "", DataFieldContainerType.LIST));
		metadata.addField(new DataFieldMetadata(ERROR_OUTPUT_VALUES, DataFieldType.STRING, "", DataFieldContainerType.MAP));
		metadata.addField(new DataFieldMetadata(ERROR_OUTPUT_PARAMS, DataFieldType.STRING, "", DataFieldContainerType.MAP));
		metadata.addField(new DataFieldMetadata(ERROR_OUTPUT_CREATED, DataFieldType.DATE, ""));
		metadata.addField(new DataFieldMetadata(ERROR_OUTPUT_GRAPH, DataFieldType.STRING, ""));
			
		return metadata;
	}
	
	private void populateErrorRecord(ValidationError error) {
		errorRecord.reset();
		errorRecord.init();
		
		IntegerDataField serial_number = new IntegerDataField(errorRecord.getField(ERROR_OUTPUT_SERIAL_NUMBER).getMetadata(), (processedRecords -1));
		errorRecord.getField(ERROR_OUTPUT_SERIAL_NUMBER).setValue(serial_number);
		
		StringDataField graphName = new StringDataField(errorRecord.getField(ERROR_OUTPUT_GRAPH).getMetadata(), getGraph().getName());
		errorRecord.getField(ERROR_OUTPUT_GRAPH).setValue(graphName);
		
		IntegerDataField code = new IntegerDataField(errorRecord.getField(ERROR_OUTPUT_CODE).getMetadata(), error.getCode());
		errorRecord.getField(ERROR_OUTPUT_CODE).setValue(code);
		
		StringDataField message = new StringDataField(errorRecord.getField(ERROR_OUTPUT_MESSAGE).getMetadata(), error.getMessage());
		errorRecord.getField(ERROR_OUTPUT_MESSAGE).setValue(message);
		
		StringDataField name = new StringDataField(errorRecord.getField(ERROR_OUTPUT_NAME).getMetadata(), error.getName());
		errorRecord.getField(ERROR_OUTPUT_NAME).setValue(name);
		
		((ListDataField) errorRecord.getField(ERROR_OUTPUT_FIELDS)).setValue(error.getFields());
		((MapDataField) errorRecord.getField(ERROR_OUTPUT_VALUES)).setValue(error.getValues());
		((MapDataField) errorRecord.getField(ERROR_OUTPUT_PARAMS)).setValue(error.getParams());
		
		DateDataField created = new DateDataField(errorRecord.getField(ERROR_OUTPUT_CREATED).getMetadata(), error.getTimestamp());
		errorRecord.getField(ERROR_OUTPUT_CREATED).setValue(created);
	}
	
	public static DataRecordMetadata createCustomRuleOutputMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(ERROR_OUTPUT_METADATA_NAME);
		metadata.addField(new DataFieldMetadata(CUSTOM_RULE_RESULT, DataFieldType.BOOLEAN, ""));
		metadata.addField(new DataFieldMetadata(CUSTOM_RULE_MESSAGE, DataFieldType.STRING, ""));
		return metadata;
	}

}
