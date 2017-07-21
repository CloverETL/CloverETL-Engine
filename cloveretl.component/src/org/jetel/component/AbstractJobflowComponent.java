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

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CTLMapping;
import org.jetel.util.CTLMapping.MissingRecordFieldMessage;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Apr 23, 2013
 */
public abstract class AbstractJobflowComponent extends Node {

    /** the name of an XML attribute used to define input mapping */
	public static final String XML_INPUT_MAPPING_ATTRIBUTE = "inputMapping";

    /** the name of an XML attribute used to define output mapping to the first standard output port */
	public static final String XML_OUTPUT_MAPPING_ATTRIBUTE = "outputMapping";

    /** the name of an XML attribute used to define output mapping to the second error output port */
	public static final String XML_ERROR_MAPPING_ATTRIBUTE = "errorMapping";

    /** the name of an XML attribute for error output redirection */
	public static final String XML_REDIRECT_ERROR_OUTPUT_ATTRIBUTE = "redirectErrorOutput";

    /** the name of an XML attribute for flag which decides what to do if a operation fails */
	public static final String XML_STOP_ON_FAIL_ATTRIBUTE = "stopOnFail"; //$NON-NLS-1$

    /** the port index used for data record input */
    protected static final int INPUT_PORT_NUMBER = 0;

    /** the port index used for data record output for tokens with successful status */
    protected static final int OUTPUT_PORT_NUMBER = 0;

    /** the port index used for optional data record output for tokens with non-successful status */
    protected static final int ERROR_PORT_NUMBER = 1;

	/** Input record identifier for CTL mappings */
	protected static final String INPUT_RECORD_ID = "input";

	/** Output record identifier for CTL mappings */
	protected static final String OUTPUT_RECORD_ID = "output";

	/** Error record identifier for CTL mappings */
	protected static final String ERROR_RECORD_ID = "error";
	
	/** Input parameters record identifier for CTL mappings */
	protected static final String ATTRIBUTES_RECORD_ID = "attributes";
	
	/** Result record identifier for CTL mappings */
	protected static final String RESULT_RECORD_ID = "result";
	
	/** Error result record identifier for CTL mappings */
	protected static final String ERROR_RESULT_RECORD_ID = "error_result";
	
    public static final String RESULT_METADATA_NAME = "Result";
    public static final String ERROR_METADATA_NAME = "Error";
    public static final String ATTRIBUTES_METADATA_NAME = "Attributes";

    /**
	 * Flag which decides what to do if a job fails.<br>
	 * <b>true</b> - component stops processing of next incoming tokens
	 */
	protected boolean stopOnFail;

    /**
     * Has any of the previous operations failed?
     */
    protected boolean failure = false;

    /**
     * Source code of input mapping.
     */
    protected String inputMappingCode;

	/**
	 * Runtime for input mapping.
	 */
    protected CTLMapping inputMapping;
    
    /**
     * Source code for standard output mapping.
     */
    protected String outputMappingCode;

	/**
	 * Runtime for standard output mapping.
	 */
	protected CTLMapping outputMapping;

    /**
     * Source code for error output mapping.
     */
	protected String errorMappingCode;

	/**
	 * Runtime for error output mapping.
	 */
	protected CTLMapping errorMapping;

    /**
     * only optional input port or <code>null</code> if no input edge is assigned
     */
    protected InputPort inputPort;
    
    /**
     * standard output port or <code>null</code> if no output edge is assigned
     */
    protected OutputPort outputPort;
    
    /**
     * optional error output port or <code>null</code> if no error output edge is assigned
     */
    protected OutputPort errorPort;

    /**
     * Is the optional input port attached.
     */
    protected boolean hasInputPort;
    
    /**
     * Is the optional standard output port attached.
     */
    protected boolean hasOutputPort;
    
    /**
     * Is the optional error output port attached.
     */
    protected boolean hasErrorPort;

    /**
     * Input port record or null if input port is not attached.
     */
    protected DataRecord inputRecord;
    
    protected DataRecord attributesRecord;

    /**
     * Record with result.
     */
    protected DataRecord resultRecord;
    
    /**
     * Record with error result.
     */
    protected DataRecord errorResultRecord;

    protected DataRecord outputRecord;
    
    protected DataRecord errorRecord;
    
    /**
     * Enables redirection of error output to standard output. Disabled by default.
     */
    protected boolean redirectErrorOutput;

    public AbstractJobflowComponent(String id, TransformationGraph graph) {
		super(id, graph);
	}

	public AbstractJobflowComponent(String id) {
		super(id);
	}

	protected abstract DataRecordMetadata createInputMetadata();
	
	protected abstract DataRecordMetadata createOutputMetadata();
	
	protected abstract DataRecordMetadata createErrorMetadata();
	
	/**
	 * @param stopOnFail the stopOnFail to set
	 */
	protected void setStopOnFail(boolean stopOnFail) {
		this.stopOnFail = stopOnFail;
	}

	/**
	 * @param inputMappingCode the inputMappingCode to set
	 */
	protected void setInputMappingCode(String inputMappingCode) {
		this.inputMappingCode = inputMappingCode;
	}

	/**
	 * @param outputMappingCode the outputMappingCode to set
	 */
	protected void setOutputMappingCode(String outputMappingCode) {
		this.outputMappingCode = outputMappingCode;
	}

	/**
	 * @param errorMappingCode the errorMappingCode to set
	 */
	protected void setErrorMappingCode(String errorMappingCode) {
		this.errorMappingCode = errorMappingCode;
	}

	/**
	 * @param redirectErrorOutput the redirectErrorOutput to set
	 */
	protected void setRedirectErrorOutput(boolean redirectErrorOutput) {
		this.redirectErrorOutput = redirectErrorOutput;
	}
	
	protected abstract void setDefaults();

	protected void tryToInit() throws ComponentNotReadyException {
		inputPort = getInputPort(INPUT_PORT_NUMBER);
		outputPort = getOutputPort(OUTPUT_PORT_NUMBER);
		errorPort = getOutputPort(ERROR_PORT_NUMBER);
		
		hasInputPort = (inputPort != null);
		hasOutputPort = (outputPort != null);
		hasErrorPort = (errorPort != null);
		
		if (redirectErrorOutput && hasErrorPort) {
			throw new ComponentNotReadyException(this, XML_REDIRECT_ERROR_OUTPUT_ATTRIBUTE, "Error output is redirected to standard output port, but there is an edge connected to the error port.");
		}
		
		// create input params record, no matter if input edge is connected
		// used to resolve default values
		attributesRecord = DataRecordFactory.newRecord(createInputMetadata());
		attributesRecord.init();
		
		if (hasInputPort) {
			inputRecord = DataRecordFactory.newRecord(inputPort.getMetadata());
			inputRecord.init();
		}
		
		if (hasOutputPort) {
			resultRecord = DataRecordFactory.newRecord(createOutputMetadata());
			resultRecord.init();

			outputRecord = DataRecordFactory.newRecord(outputPort.getMetadata());
			outputRecord.init();
		}
		
		if (hasErrorPort) {
			errorResultRecord = DataRecordFactory.newRecord(createErrorMetadata());
			errorResultRecord.init();
			
			errorRecord = DataRecordFactory.newRecord(errorPort.getMetadata());
			errorRecord.init();
		}
		
		//create input mapping
		inputMapping = new CTLMapping("Input mapping", this); //$NON-NLS-1$
		inputMapping.setTransformation(inputMappingCode);
		if (hasInputPort) {
			inputMapping.addInputRecord(INPUT_RECORD_ID, inputRecord);
		}
		inputMapping.addOutputRecord(ATTRIBUTES_RECORD_ID, attributesRecord);
		
		//create output mapping
		outputMapping = new CTLMapping("Output mapping", this); //$NON-NLS-1$
		outputMapping.setTransformation(outputMappingCode);
		outputMapping.addInputRecord(INPUT_RECORD_ID, inputRecord);
		outputMapping.addInputRecord(RESULT_RECORD_ID, resultRecord);
		outputMapping.addOutputRecord(OUTPUT_RECORD_ID, outputRecord);

		//create error mapping
		errorMapping = new CTLMapping("Error mapping", this); //$NON-NLS-1$
		errorMapping.setTransformation(errorMappingCode);
		errorMapping.addInputRecord(INPUT_RECORD_ID, inputRecord);
		errorMapping.addInputRecord(ERROR_RESULT_RECORD_ID, errorResultRecord);
		errorMapping.addOutputRecord(OUTPUT_RECORD_ID, null);
		errorMapping.addOutputRecord(ERROR_RECORD_ID, errorRecord);
		
		setDefaults();

		//initialize input mapping
		inputMapping.init(XML_INPUT_MAPPING_ATTRIBUTE, 
				MissingRecordFieldMessage.newOutputFieldMessage(ATTRIBUTES_RECORD_ID, "No such attribute")
		);
		
		//initialize output mapping
		outputMapping.init(XML_OUTPUT_MAPPING_ATTRIBUTE,
				MissingRecordFieldMessage.newInputFieldMessage(ATTRIBUTES_RECORD_ID, "No such attribute"),
				MissingRecordFieldMessage.newInputFieldMessage(RESULT_RECORD_ID, "No such result field")
		);
		
		//initialize error mapping
		errorMapping.init(XML_ERROR_MAPPING_ATTRIBUTE,
				MissingRecordFieldMessage.newInputFieldMessage(ATTRIBUTES_RECORD_ID, "No such attribute"),
				MissingRecordFieldMessage.newInputFieldMessage(ERROR_RESULT_RECORD_ID, "No such result field")
		);
	}

	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		tryToInit();
	}
	
	protected boolean checkPorts(ConfigurationStatus status) {
		return checkInputPorts(status, 0, 1) && checkOutputPorts(status, 0, 2, false);
	}
	
	protected void tryToInit(ConfigurationStatus status) {
		try {
        	tryToInit();
        } catch (Exception e) {
        	status.add("Initialization failed", e, Severity.ERROR, this, Priority.NORMAL);
        }
	}
	
	protected void checkMappings(ConfigurationStatus status) {
		if (hasInputPort && StringUtils.isEmpty(inputMappingCode)) {
			status.add("Mapping is not defined, but there is an edge connected", Severity.WARNING, this, Priority.LOW, XML_INPUT_MAPPING_ATTRIBUTE);
		}
		if (hasOutputPort && StringUtils.isEmpty(outputMappingCode)) {
			status.add("Mapping is not defined, but there is an edge connected", Severity.WARNING, this, Priority.LOW, XML_OUTPUT_MAPPING_ATTRIBUTE);
		}
		if (hasErrorPort && StringUtils.isEmpty(errorMappingCode)) {
			status.add("Mapping is not defined, but there is an edge connected", Severity.WARNING, this, Priority.LOW, XML_ERROR_MAPPING_ATTRIBUTE);
		}
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        status = super.checkConfig(status);
        
        if (!checkPorts(status)) {
        	return status;
        }
        
        tryToInit(status);
        
        checkMappings(status);

		if (redirectErrorOutput && !hasOutputPort) {
			status.add("The error port is redirected to the standard output port, but there is no edge connected", Severity.WARNING, this, Priority.LOW, XML_REDIRECT_ERROR_OUTPUT_ATTRIBUTE);
		}
		
        return status;
	}

	protected boolean readRecord() throws InterruptedException, IOException {
		return (inputPort.readRecord(inputRecord) != null);
	}
	
	protected static void fromXML(AbstractJobflowComponent node, ComponentXMLAttributes componentAttributes) throws XMLConfigurationException {
		node.setInputMappingCode(componentAttributes.getStringEx(XML_INPUT_MAPPING_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
		node.setOutputMappingCode(componentAttributes.getStringEx(XML_OUTPUT_MAPPING_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
		node.setErrorMappingCode(componentAttributes.getStringEx(XML_ERROR_MAPPING_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
		node.setRedirectErrorOutput(componentAttributes.getBoolean(XML_REDIRECT_ERROR_OUTPUT_ATTRIBUTE, false));
		node.setStopOnFail(componentAttributes.getBoolean(XML_STOP_ON_FAIL_ATTRIBUTE, true));
	}
	
	protected static DataFieldMetadata createField(String name, DataFieldType type) {
		return new DataFieldMetadata(name, type, null);
	}
	
}
