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
import org.jetel.component.validator.rules.CustomValidationRule;
import org.jetel.component.validator.utils.ValidationRulesPersister;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.InputPortDirect;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
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
 * <p>Component for validation incoming records against user defined set of rules.
 * Records are taken one by one, there is no inner state in this component.</p>
 * 
 * <p>Validator takes configuration from graph configuration and deserialize
 * validation tree against all incoming records are validated.</p>
 * 
 * <p>Component uses error output mapping provided by user or automapping. On
 * normal output there are copies of original record. On error output there can be
 * user defined metadata. If these metadata contains fields from reporting metadata
 * then the record is multiplicated with each error raised during validation.
 * Otherwise the record is send to error output only once!</p>
 * 
 * @author drabekj (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * @created 23.10.2012
 * @see ValidationGroup
 * @see ValidationError
 * @see ValidationErrorAcummulator
 * @see ValidationRulesPersister
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
	
	private static enum ErrorPortField {
		CODE(DataFieldType.INTEGER),
		SERIAL_NUMBER(DataFieldType.INTEGER),
		MESSAGE(DataFieldType.STRING),
		NAME(DataFieldType.STRING),
		PATH(DataFieldType.STRING, DataFieldContainerType.LIST),
		FIELDS(DataFieldType.STRING, DataFieldContainerType.LIST),
		VALUES(DataFieldType.STRING, DataFieldContainerType.MAP),
		PARAMS(DataFieldType.STRING, DataFieldContainerType.MAP),
		CREATED(DataFieldType.DATE),
		GRAPH(DataFieldType.STRING);
		
		private final DataFieldType dataFieldType;
		private final DataFieldContainerType dataFieldContainerType;
		
		private ErrorPortField(DataFieldType dataFieldType) {
			this.dataFieldType = dataFieldType;
			this.dataFieldContainerType = DataFieldContainerType.SINGLE;
		}
		
		private ErrorPortField(DataFieldType dataFieldType, DataFieldContainerType dataFieldContainerType) {
			this.dataFieldType = dataFieldType;
			this.dataFieldContainerType = dataFieldContainerType;
		}
		
		public String getFieldName() {
			return name().toLowerCase();
		}
		
		public DataFieldType getDataFieldType() {
			return dataFieldType;
		}
		
		public DataFieldContainerType getDataFieldContainerType() {
			return dataFieldContainerType;
		}
		
		public int getFieldIndex() {
			return ordinal();
		}
	}
	
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
	private boolean errorInfoMapped = false;
	private GraphWrapper graphWrapper;
	
	/**
	 * Minimalistic constructor to ensure component name to init
	 * @param id
	 */
	public Validator(String id) {
		super(id);
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}
	
	/**
	 * Create new instance of Validator from given configuration
	 * @param graph Transformation graph to attach to Validator
	 * @param xmlElement Parsed XML configuration
	 * @return New instance of Validator
	 * @throws XMLConfigurationException when some XML configuration is corrupted (some mandatory element missing)
	 */
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
	
	/**
	 * Sets string with serialized validation rules
	 * @param Value validation rules to set
	 */
	private void setRules(String value) {
		rules = value;
	}
	
	/**
	 * Sets URL of file with serialized validation rules
	 * @param value URL of file
	 */
	private void setExternalRulesURL(String value) {
		externalRulesURL = value;
	}
	
	/**
	 * Sets code of error mapping
	 * @param value Error mapping
	 */
	private void setErrorMappingCode(String value) {
		errorMappingCode = value;
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		// Force input port to be connected and provided with metadata
		InputPort inputPort = getInputPort(INPUT_PORT);
		if(inputPort == null || inputPort.getMetadata() == null) {
			ConfigurationProblem problem = new ConfigurationProblem("No input metadata.", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.HIGH);
			status.add(problem);
			return status;
		}
		
		OutputPort validOutputPort = getOutputPort(VALID_OUTPUT_PORT);
		if (validOutputPort != null) {
			if (!validOutputPort.getMetadata().equals(inputPort.getMetadata())) {
				ConfigurationProblem problem = new ConfigurationProblem("Valid port metadata does not match input port metadata.",
						ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
				status.add(problem);
			}
		}
		
		OutputPort invalidOutputPort = getOutputPort(INVALID_OUTPUT_PORT);
		if (invalidOutputPort != null) {
			if (errorMappingCode == null || errorMappingCode.isEmpty()) {
				if (!invalidOutputPort.getMetadata().equals(inputPort.getMetadata())) {
					ConfigurationProblem problem = new ConfigurationProblem("Invalid port metadata does not match input port metadata and mapping is not provided.",
							ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
					status.add(problem);
				}
			}
		}
		
		try {
			initRootGroup();
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.HIGH);
			status.add(problem);
			return status;
		}

		ReadynessErrorAcumulator accumulator = new ReadynessErrorAcumulator();
		GraphWrapper graphWrapper = new EngineGraphWrapper(getGraph());
		graphWrapper.init(rootGroup);
		if(rootGroup != null && !rootGroup.isReady(inputPort.getMetadata(), accumulator, graphWrapper)) {
			String tempName = new String();
			// Put all error messages together in nice way
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
		
		try {
			initBufferRecordsAndMappings();
		} catch(Exception e) {
			ConfigurationProblem problem = new ConfigurationProblem(e, ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.HIGH, XML_ERROR_MAPPING);
			status.add(problem);
			return status;
		}
		return status;
	}

	private void initRootGroup() throws ComponentNotReadyException {
		// URL for external validation rules have higher priority
		String tempRules;
		if(externalRulesURL != null) {
			tempRules = FileUtils.getStringFromURL(getGraph().getRuntimeContext().getContextURL(), externalRulesURL, null);
		} else {
			tempRules = rules;
		}
		
		if(tempRules == null || tempRules.isEmpty()) {
			throw new ComponentNotReadyException("No validation rules.");
		}

		rootGroup = ValidationRulesPersister.deserialize(tempRules);
	}

	private void initBufferRecordsAndMappings() throws ComponentNotReadyException {
		errorInfoMapped = false;
		errorMapping = null;

		if(getOutputPort(INVALID_OUTPUT_PORT) != null && errorMappingCode != null && !errorMappingCode.isEmpty()) {
			errorMapping = new CTLMapping("Error output", this);
			inputRecord = errorMapping.addInputMetadata(MAPPING_INPUT_RECORD, getInputPort(INPUT_PORT).getMetadata());
			errorRecord = errorMapping.addInputMetadata(MAPPING_INPUT_ERROR, createErrorOutputMetadata());
			errorMapping.addOutputMetadata("DUMMY", null);	// Dummy metadata to reach to error port 
			errorMapping.addOutputMetadata(MAPPING_OUTPUT_RECORD, getOutputPort(INVALID_OUTPUT_PORT).getMetadata());
			errorMapping.setTransformation(errorMappingCode);
			
			errorMapping.init(XML_ERROR_MAPPING);
			
			List<DataFieldMetadata> usedInputFields = errorMapping.findUsedInputFields(getGraph());
			outerloop:
			for(DataFieldMetadata inField : usedInputFields) {
				for(DataFieldMetadata errInField : errorRecord.getMetadata()) {
					if(inField == errInField) {
						errorInfoMapped = true;
						break outerloop;
					}
				}
			}
		} else {
			inputRecord = DataRecordFactory.newRecord(getInputPort(INPUT_PORT).getMetadata());
			inputRecord.init();
		}
	}

	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		initRootGroup();
		initBufferRecordsAndMappings();
		
		graphWrapper = new EngineGraphWrapper(getGraph());
		graphWrapper.init(rootGroup);
		rootGroup.init(inputRecord, graphWrapper);
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		if (errorMapping != null) {
			errorMapping.preExecute();
		}
		processedRecords = 0;
		rootGroup.preExecute();
	}

	@Override
	protected Result execute() throws Exception {
		logger.trace("Executing Validator component");
		
		// Prepare ports and structures for records
		InputPortDirect inPort = getInputPortDirect(INPUT_PORT);
		OutputPortDirect validPort = getOutputPortDirect(VALID_OUTPUT_PORT);
		OutputPortDirect invalidPort = getOutputPortDirect(INVALID_OUTPUT_PORT);
	
		ValidationErrorAccumulator errorAccumulator = null;
		if (errorInfoMapped) {
			errorAccumulator = new ValidationErrorAccumulator();
		}
		
		DataRecord record = DataRecordFactory.newRecord(inPort.getMetadata());
		record.init();
		CloverBuffer recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
		
		// Iterate over data
		while(runIt) {
			if (!inPort.readRecordDirect(recordBuffer)){
				break;
			}
			inputRecord.deserialize(recordBuffer);
			
			if (errorAccumulator != null) {
				errorAccumulator.reset();
			}
			processedRecords++;
			if(rootGroup.isValid(inputRecord,errorAccumulator, graphWrapper) != ValidationNode.State.INVALID) {
				recordBuffer.rewind();
				validPort.writeRecordDirect(recordBuffer);
			} else {
				if(invalidPort != null) {
					if (errorMapping != null) {
						if(errorAccumulator != null) {
							if (errorAccumulator.isEmpty()) {
								throw new IllegalStateException("Validation marked record as invalid but no errors have been reported");
							}
							for(ValidationError error : errorAccumulator) {
								populateErrorRecord(error);
								errorMapping.execute();
								MiscUtils.sendRecordToPort(invalidPort, errorMapping.getOutputRecord(1));
							}
						} else {
							errorMapping.execute();
							MiscUtils.sendRecordToPort(invalidPort, errorMapping.getOutputRecord(1));	
						}
					}
					else {
						recordBuffer.rewind();
						invalidPort.writeRecordDirect(recordBuffer);
					}
				}
			}
		}
		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		if (errorMapping != null) {
			errorMapping.postExecute();
		}
		rootGroup.postExecute();
	}
	
	/**
	 * Prepare fake metadata for error report
	 * @see ValidationError
	 * @return Metadata which can hold validation error
	 */
	public static DataRecordMetadata createErrorOutputMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(ERROR_OUTPUT_METADATA_NAME);
		
		for (ErrorPortField value : ErrorPortField.values()) {
			DataFieldMetadata dataFieldMetadata = new DataFieldMetadata(value.getFieldName(), value.getDataFieldType(), "", value.getDataFieldContainerType());
			metadata.addField(value.getFieldIndex(), dataFieldMetadata);
		}
			
		return metadata;
	}
	
	/**
	 * Converts given validation error into fake data record
	 * @param error Validation Error
	 */
	private void populateErrorRecord(ValidationError error) {
		errorRecord.getField(ErrorPortField.SERIAL_NUMBER.getFieldIndex()).setValue(processedRecords - 1);
		errorRecord.getField(ErrorPortField.GRAPH.getFieldIndex()).setValue(getGraph().getName());
		errorRecord.getField(ErrorPortField.CODE.getFieldIndex()).setValue(error.getCode());
		errorRecord.getField(ErrorPortField.MESSAGE.getFieldIndex()).setValue(error.getMessage());
		errorRecord.getField(ErrorPortField.NAME.getFieldIndex()).setValue(error.getName());
		errorRecord.getField(ErrorPortField.PATH.getFieldIndex()).setValue(error.getPath());
		errorRecord.getField(ErrorPortField.FIELDS.getFieldIndex()).setValue(error.getFields());
		errorRecord.getField(ErrorPortField.VALUES.getFieldIndex()).setValue(error.getValues());
		errorRecord.getField(ErrorPortField.PARAMS.getFieldIndex()).setValue(error.getParams());
		errorRecord.getField(ErrorPortField.CREATED.getFieldIndex()).setValue(error.getTimestamp());
	}
	
	/**
	 * Prepare fake metadata for output record used for custom rules
	 * @see CustomValidationRule
	 * @return
	 */
	public static DataRecordMetadata createCustomRuleOutputMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(ERROR_OUTPUT_METADATA_NAME);
		metadata.addField(new DataFieldMetadata(CUSTOM_RULE_RESULT, DataFieldType.BOOLEAN, ""));
		metadata.addField(new DataFieldMetadata(CUSTOM_RULE_MESSAGE, DataFieldType.STRING, ""));
		return metadata;
	}

}
