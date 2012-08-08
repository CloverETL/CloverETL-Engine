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

import org.jetel.component.fileoperation.FileManager;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.graph.runtime.tracker.FileOperationComponentTokenTracker;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.CTLMapping;
import org.jetel.util.CTLMapping.MissingRecordFieldMessage;
import org.jetel.util.MiscUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.string.StringUtils;

public abstract class AbstractFileOperation<R extends org.jetel.component.fileoperation.result.Result> extends Node {

	/** the name of an XML attribute used to define input mapping */
	public static final String XML_INPUT_MAPPING_ATTRIBUTE = "inputMapping"; //$NON-NLS-1$

    /** the name of an XML attribute used to define output mapping to the first standard output port */
	public static final String XML_STANDARD_OUTPUT_MAPPING_ATTRIBUTE = "standardOutputMapping"; //$NON-NLS-1$

    /** the name of an XML attribute used to define output mapping to the second error output port */
	public static final String XML_ERROR_OUTPUT_MAPPING_ATTRIBUTE = "errorOutputMapping"; //$NON-NLS-1$

    /** the name of an XML attribute for verbose output */
	public static final String XML_VERBOSE_OUTPUT = "verboseOutput"; //$NON-NLS-1$

	/** the name of an XML attribute for error output redirection */
	public static final String XML_REDIRECT_ERROR_OUTPUT = "redirectErrorOutput"; //$NON-NLS-1$

	/** the port index used for data record input */
    protected static final int INPUT_PORT_NUMBER = 0;

    /** the port index used for data record output for tokens with successful status */
    protected static final int STANDARD_OUTPUT_PORT_NUMBER = 0;

    /** the port index used for optional data record output for tokens with non-successful status */
    protected static final int ERROR_OUTPUT_PORT_NUMBER = 1;

	/** Input record identifier for CTL mappings */
	private static final String INPUT_RECORD_ID = "input"; //$NON-NLS-1$

	/** Output record identifier for CTL mappings */
	private static final String OUTPUT_RECORD_ID = "output"; //$NON-NLS-1$

	/** Error record identifier for CTL mappings */
	private static final String ERROR_RECORD_ID = "error"; //$NON-NLS-1$
	
	/** Input parameters record identifier for CTL mappings */
	protected static final String PARAMS_RECORD_ID = "params"; //$NON-NLS-1$
	
	/** Result record identifier for CTL mappings */
	private static final String RESULT_RECORD_ID = "result"; //$NON-NLS-1$
	
	/** Error result record identifier for CTL mappings */
	private static final String ERROR_RESULT_RECORD_ID = "error_result"; //$NON-NLS-1$
	
	/** default delimiter string for artificial metadata */
	static final String DUMMY_DELIMITER = ";"; //$NON-NLS-1$

	protected static final String ERROR_RECORD_NAME = "Error"; //$NON-NLS-1$

	protected static final int ERR_RESULT_INDEX = 0;
	protected static final int ERR_ERROR_MESSAGE_INDEX = 1;
	
	protected static final String ERR_RESULT_NAME = "result"; //$NON-NLS-1$
	protected static final String ERR_ERROR_MESSAGE_NAME = "errorMessage"; //$NON-NLS-1$

    /**
     * only optional input port or <code>null</code> if no input edge is assigned
     */
    protected InputPort inputPort;
    
    /**
     * standard output port or <code>null</code> if no output edge is assigned
     */
    protected OutputPort standardOutputPort;
    
    /**
     * optional error output port or <code>null</code> if no error output edge is assigned
     */
    protected OutputPort errorOutputPort;
    
    /**
     * Is the optional input port attached.
     */
    protected boolean hasInputPort;
    
    /**
     * Is the optional standard output port attached.
     */
    protected boolean hasStandardOutputPort;
    
    /**
     * Is the optional error output port attached.
     */
    protected boolean hasErrorOutputPort;

    /**
     * Source code of input mapping.
     */
    protected String inputMappingCode;

	/**
	 * Runtime for input mapping.
	 */
    protected RecordTransform inputMappingTransformation;

    /**
     * Source code for standard output mapping.
     */
    protected String outputMappingCode;

	/**
	 * Runtime for standard output mapping.
	 */
    protected RecordTransform standardOutputMappingTransformation;

    /**
     * Source code for error output mapping.
     */
    protected String errorMappingCode;
    
	/**
	 * Runtime for input mapping.
	 */
    protected CTLMapping inputMapping;
    
	/**
	 * Runtime for standard output mapping.
	 */
    protected CTLMapping outputMapping;

	/**
	 * Runtime for error output mapping.
	 */
    protected CTLMapping errorMapping;

    /**
     * If enabled, individual files will be sent 
     * as separate records to the output port.
     * 
     * Otherwise, results are grouped and for each
     * input record, one output record is created.
     *  
     * Disabled by default.
     */
    protected boolean verboseOutput;

    /**
     * Enables redirection of error output to standard output. Disabled by default.
     */
    protected boolean redirectErrorOutput;

	/**
	 * Runtime for error output mapping.
	 */
    protected RecordTransform errorOutputMappingTransformation;

	/**
     * Record which is used for input mapping as other configuration attributes.
     */
    protected DataRecord inputParamsRecord;
    
    /**
     * Record with run status information.
     */
    protected DataRecord resultRecord;
    
    /**
     * Record with run status information.
     */
    protected DataRecord errorRecord;
    
    /**
     * Input port record or null if input port is not attached.
     */
    protected DataRecord inputRecord;
    
    /**
     * Standard output port record or null if standard output port is not attached.
     */
    protected DataRecord standardOutputRecord;
    
    /**
     * Error output port record or null if error output port is not attached.
     */
    protected DataRecord errorOutputRecord;
    
    /**
     * Input records for input mapping transformation.
     */
    protected DataRecord[] inputMappingInRecords;

    /**
     * Output records for input mapping transformation.
     */
    protected DataRecord[] inputMappingOutRecords;

    /**
     * Input records for output mapping transformation - standard output mapping.
     */
    protected DataRecord[] standardOutputMappingInRecords;

    /**
     * Input records for output mapping transformation - error output mapping.
     */
    protected DataRecord[] errorOutputMappingInRecords;

    /**
     * Output records for standard output mapping transformation.
     */
    protected DataRecord[] standardOutputMappingOutRecords;

    /**
     * Output records for error output mapping transformation.
     */
    protected DataRecord[] errorOutputMappingOutRecords;

    /**
     * File manager singleton reference.
     */
    protected FileManager manager = FileManager.getInstance();
    
    protected PropertyRefResolver resolver;
    
    private FileOperationComponentTokenTracker foTokenTracker;
	
    protected R result;
    protected int index;
    
	public AbstractFileOperation(String id, TransformationGraph graph) {
		super(id, graph);
		this.resolver = new PropertyRefResolver(graph.getGraphProperties());
	}

	public AbstractFileOperation(String id) {
		super(id);
	}

	protected void setInputMapping(String inputMapping) {
		this.inputMappingCode = inputMapping;
	}

	protected void setStandardOutputMapping(String standardOutputMapping) {
		this.outputMappingCode = standardOutputMapping;
	}

	protected void setErrorOutputMapping(String errorOutputMapping) {
		this.errorMappingCode = errorOutputMapping;
	}
    
	protected void setRedirectErrorOutput(boolean redirectErrorOutput) {
		this.redirectErrorOutput = redirectErrorOutput;
	}

	protected void setVerboseOutput(boolean verboseOutput) {
		this.verboseOutput = verboseOutput;
	}

	static int performTransformation(RecordTransform transformation, DataRecord[] inRecords, DataRecord[] outRecords, String errMessage) {
		try {
			return transformation.transform(inRecords, outRecords);
		} catch (Exception exception) {
			try {
				return transformation.transformOnError(exception, inRecords, outRecords);
			} catch (TransformException e) {
				throw new JetelRuntimeException(errMessage, e);
			}
		}
	}
	
	static void fromXML(AbstractFileOperation<?> node, ComponentXMLAttributes componentAttributes) throws XMLConfigurationException {
        try {
    		node.setInputMapping(componentAttributes.getString(XML_INPUT_MAPPING_ATTRIBUTE, null));
    		node.setStandardOutputMapping(componentAttributes.getString(XML_STANDARD_OUTPUT_MAPPING_ATTRIBUTE, null));
    		node.setErrorOutputMapping(componentAttributes.getString(XML_ERROR_OUTPUT_MAPPING_ATTRIBUTE, null));
    		node.setRedirectErrorOutput(componentAttributes.getBoolean(XML_REDIRECT_ERROR_OUTPUT, false));
    		node.setVerboseOutput(componentAttributes.getBoolean(XML_VERBOSE_OUTPUT, false));	
        } catch (Exception exception) {
            throw new XMLConfigurationException(FileOperationComponentMessages.getString("AbstractFileOperation.error_creating_component"), exception); //$NON-NLS-1$
        }
	}
	
	protected void processInputMapping() {
		inputMapping.execute();
		
		processInputParamsRecord();
	}
	
	protected abstract void setDefaultParameters();
	
	protected boolean mainExecuteOperation() throws InterruptedException {
		//prepare input parameters
		processInputMapping();
		
		result = executeOperation();
		boolean spawnNewTokens = (foTokenTracker != null) && (result.totalCount() != 1); 
		if (spawnNewTokens) {
			foTokenTracker.setNoUnify();
		}
		processResult();
		if (spawnNewTokens) {
			foTokenTracker.freeToken(inputRecord);
		}
		return true;
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		tryToInit();
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		
		inputMapping.preExecute();
		outputMapping.preExecute();
		errorMapping.preExecute();
	}

	@Override
	public Result execute() throws Exception {
		boolean success = true;
		if (hasInputPort) {
			while (inputPort.readRecord(inputRecord) != null && runIt && success) {
				success &= mainExecuteOperation();
				SynchronizeUtils.cloverYield();
			}
		} else {
			if (foTokenTracker != null) {
				inputRecord = foTokenTracker.createToken(INPUT_RECORD_ID);
			}
			success &= mainExecuteOperation();
		}

		broadcastEOF();

        return (runIt ? (success ? Result.FINISHED_OK : Result.ERROR) : Result.ABORTED);
	}
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();

		inputMapping.postExecute();
		outputMapping.postExecute();
		errorMapping.postExecute();
	}

	/**
	 * This method is invoked from component configuration check and from component initialization. 
	 * @throws ComponentNotReadyException 
	 */
	protected void tryToInit() throws ComponentNotReadyException {
		//find the attached ports (input and output)
		inputPort = getInputPortDirect(INPUT_PORT_NUMBER);
		standardOutputPort = getOutputPort(STANDARD_OUTPUT_PORT_NUMBER);
		errorOutputPort = getOutputPort(ERROR_OUTPUT_PORT_NUMBER);

		//which ports are attached?
		hasInputPort = (inputPort != null);
		hasStandardOutputPort = (standardOutputPort != null);
		hasErrorOutputPort = (errorOutputPort != null);
		
		if (redirectErrorOutput && hasErrorOutputPort) {
			throw new ComponentNotReadyException(FileOperationComponentMessages.getString("AbstractFileOperation.err_out_redirected")); //$NON-NLS-1$
		}
		
		//create input mapping
		inputMapping = new CTLMapping("Input mapping", this); //$NON-NLS-1$
		inputMapping.setTransformation(inputMappingCode);
		inputMapping.setClasspath(getGraph().getRuntimeContext().getClassPath());
		inputMapping.setClassLoader(this.getClass().getClassLoader());
		
		//create output mapping
		outputMapping = new CTLMapping("Output mapping", this); //$NON-NLS-1$
		outputMapping.setTransformation(outputMappingCode);
		outputMapping.setClasspath(getGraph().getRuntimeContext().getClassPath());
		outputMapping.setClassLoader(this.getClass().getClassLoader());

		//create error mapping
		errorMapping = new CTLMapping("Error mapping", this); //$NON-NLS-1$
		errorMapping.setTransformation(errorMappingCode);
		errorMapping.setClasspath(getGraph().getRuntimeContext().getClassPath());
		errorMapping.setClassLoader(this.getClass().getClassLoader());

		// create input params record, no matter if input edge is connected
		// used to resolve default values
		inputParamsRecord = inputMapping.addOutputMetadata(PARAMS_RECORD_ID, createInputParamsMetadata());

		if (hasInputPort) {
			inputRecord = inputMapping.addInputMetadata(INPUT_RECORD_ID, inputPort.getMetadata());
		}
		
		if (hasStandardOutputPort) {
			outputMapping.addInputRecord(INPUT_RECORD_ID, inputRecord);
			resultRecord = outputMapping.addInputMetadata(RESULT_RECORD_ID, createResultMetadata());
			standardOutputRecord = outputMapping.addOutputMetadata(OUTPUT_RECORD_ID, standardOutputPort.getMetadata());
		}

		if (hasErrorOutputPort) {
			errorMapping.addInputRecord(INPUT_RECORD_ID, inputRecord);
			errorRecord = errorMapping.addInputMetadata(ERROR_RECORD_ID, createErrorMetadata());
			errorMapping.addOutputMetadata(OUTPUT_RECORD_ID, null); // dummy metadata to ensure the error metadata will be available under index 1, for example $out.1.errMsg
			errorOutputRecord = errorMapping.addOutputMetadata(ERROR_RESULT_RECORD_ID, errorOutputPort.getMetadata());
		}
		
		setDefaultParameters();
		
		//initialize input mapping
		inputMapping.init(XML_INPUT_MAPPING_ATTRIBUTE, 
				MissingRecordFieldMessage.newOutputFieldMessage(PARAMS_RECORD_ID, FileOperationComponentMessages.getString("AbstractFileOperation.mapping_no_such_attribute")) //$NON-NLS-1$
		);
		
		//initialize output mapping
		outputMapping.init(XML_STANDARD_OUTPUT_MAPPING_ATTRIBUTE,
				MissingRecordFieldMessage.newInputFieldMessage(RESULT_RECORD_ID, FileOperationComponentMessages.getString("AbstractFileOperation.mapping_no_such_result_field")), //$NON-NLS-1$
				MissingRecordFieldMessage.newInputFieldMessage(PARAMS_RECORD_ID, FileOperationComponentMessages.getString("AbstractFileOperation.mapping_no_such_attribute")) //$NON-NLS-1$
		);
		
		//initialize error mapping
		errorMapping.init(XML_ERROR_OUTPUT_MAPPING_ATTRIBUTE,
				MissingRecordFieldMessage.newInputFieldMessage(ERROR_RESULT_RECORD_ID, FileOperationComponentMessages.getString("AbstractFileOperation.mapping_no_such_result_field")), //$NON-NLS-1$
				MissingRecordFieldMessage.newInputFieldMessage(PARAMS_RECORD_ID, FileOperationComponentMessages.getString("AbstractFileOperation.mapping_no_such_attribute")) //$NON-NLS-1$
		);
	}
	
	@Override
	protected FileOperationComponentTokenTracker createComponentTokenTracker() {
		return foTokenTracker = new FileOperationComponentTokenTracker(this);
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);

        if (!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 0, 2, false)) {
            return status;
        }

        try {
        	tryToInit();
        } catch (Exception e) {
        	status.add(FileOperationComponentMessages.getString("AbstractFileOperation.init_failed"), e, Severity.ERROR, this, Priority.NORMAL); //$NON-NLS-1$
        }
    	
		if (hasInputPort && StringUtils.isEmpty(inputMappingCode)) {
			status.add(FileOperationComponentMessages.getString("AbstractFileOperation.mapping_not_defined"), Severity.WARNING, this, Priority.LOW, XML_INPUT_MAPPING_ATTRIBUTE);
		}
		if (hasStandardOutputPort && StringUtils.isEmpty(outputMappingCode)) {
			status.add(FileOperationComponentMessages.getString("AbstractFileOperation.mapping_not_defined"), Severity.WARNING, this, Priority.LOW, XML_STANDARD_OUTPUT_MAPPING_ATTRIBUTE);
		}
		if (hasErrorOutputPort && StringUtils.isEmpty(errorMappingCode)) {
			status.add(FileOperationComponentMessages.getString("AbstractFileOperation.mapping_not_defined"), Severity.WARNING, this, Priority.LOW, XML_ERROR_OUTPUT_MAPPING_ATTRIBUTE);
		}
		
        return status;
	}

	protected abstract void processInputParamsRecord();

	protected abstract R executeOperation() throws InterruptedException;
	
	protected void processSuccess() throws InterruptedException {
		logSuccess();
		
		if (hasStandardOutputPort) {
			processStandardOutputMapping();
		}
	}
	
	protected void processError() throws InterruptedException {
		logError();
		
		if (hasErrorOutputPort || (hasStandardOutputPort && redirectErrorOutput)) {
			processErrorOutputMapping();
		} else {
			fail();
		}
	}
	
	protected void processResult() throws InterruptedException {
		if (result.getException() != null) {
			processError();
		} else if (verboseOutput || (result.totalCount() == 1)) {
			for (index = 0; index < result.totalCount(); index++) {
				if (result.success(index)) {
					processSuccess();
				} else {
					processError();
				}
			}
		} else if (result.success()) {
			processSuccess();
		} else {
			processError();
		}
	}

	protected abstract void logSuccess();
	
	protected abstract void logError();

	protected abstract void populateResultRecord();
	
	protected abstract void populateErrorRecord();
	
	protected abstract DataRecordMetadata createInputParamsMetadata();
	
	protected abstract DataRecordMetadata createResultMetadata();
	
	protected abstract DataRecordMetadata createErrorMetadata();
	
	protected void processStandardOutputMapping() throws InterruptedException {
		if (hasStandardOutputPort) {
			resultRecord.reset();
			inputParamsRecord.reset();
			populateResultRecord();

			outputMapping.execute();

			MiscUtils.sendRecordToPort(standardOutputPort, standardOutputRecord);
		}
	}
	
	protected abstract void fail() throws JetelRuntimeException;

	protected void processErrorOutputMapping() throws InterruptedException {
		if (hasErrorOutputPort) {
			errorRecord.reset();
			inputParamsRecord.reset();
			populateErrorRecord();

			errorMapping.execute();
			
			MiscUtils.sendRecordToPort(errorOutputPort, errorOutputRecord);
		} else if (hasStandardOutputPort) {
			processStandardOutputMapping();
		}
	}

}
