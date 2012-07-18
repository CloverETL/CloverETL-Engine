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

import java.text.MessageFormat;

import org.apache.log4j.Level;
import org.jetel.component.fileoperation.SimpleParameters.CopyParameters;
import org.jetel.component.fileoperation.SimpleParameters.OverwriteMode;
import org.jetel.component.fileoperation.SingleCloverURI;
import org.jetel.component.fileoperation.result.CopyResult;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * @author krivanekm (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 4.4.2012
 */
public class CopyFiles extends AbstractFileOperation<CopyResult> {

	/** the type of the component */
    private static final String COMPONENT_TYPE = "COPY_FILES"; //$NON-NLS-1$

	// XML attribute names
	private static final String XML_SOURCE_ATTRIBUTE = "source"; //$NON-NLS-1$
	private static final String XML_TARGET_ATTRIBUTE = "target"; //$NON-NLS-1$
	private static final String XML_RECURSIVE_ATTRIBUTE = "recursive"; //$NON-NLS-1$
	private static final String XML_OVERWRITE_ATTRIBUTE = "overwrite"; //$NON-NLS-1$
	
    private static final int ERR_SOURCE_URI_INDEX = 2;
    private static final int ERR_TARGET_URI_INDEX = 3;
    
    private static final String ERR_SOURCE_URI_NAME = "sourceUri"; //$NON-NLS-1$
    private static final String ERR_TARGET_URI_NAME = "targetUri"; //$NON-NLS-1$

    private static final String RESULT_RECORD_NAME = "Result"; //$NON-NLS-1$

    private static final int RS_SOURCE_URI_INDEX = 0;
    private static final int RS_TARGET_URI_INDEX = 1;
    private static final int RS_RESULT_URI_INDEX = 2;
    private static final int RS_RESULT_INDEX = 3;
    private static final int RS_ERROR_MESSAGE_INDEX = 4;

    private static final String RS_SOURCE_URI_NAME = ERR_SOURCE_URI_NAME;
    private static final String RS_TARGET_URI_NAME = ERR_TARGET_URI_NAME;
    private static final String RS_RESULT_URI_NAME = "resultUri"; //$NON-NLS-1$
    private static final String RS_RESULT_NAME = ERR_RESULT_NAME;
    private static final String RS_ERROR_MESSAGE_NAME = ERR_ERROR_MESSAGE_NAME;

    private static final String INPUT_PARAMETERS_RECORD_NAME = "Attributes"; //$NON-NLS-1$

    private static final int IP_SOURCE_INDEX = 0;
    private static final int IP_TARGET_INDEX = 1;
    private static final int IP_RECURSIVE_INDEX = 2;
    private static final int IP_UPDATE_INDEX = 3;
    private static final int IP_NO_OVERWRITE_INDEX = 4;

    private static final String IP_SOURCE_NAME = XML_SOURCE_ATTRIBUTE;
    private static final String IP_TARGET_NAME = XML_TARGET_ATTRIBUTE;
    private static final String IP_RECURSIVE_NAME = XML_RECURSIVE_ATTRIBUTE;
    private static final String IP_UPDATE_NAME = "update"; //$NON-NLS-1$
    private static final String IP_NO_OVERWRITE_NAME = "noOverwrite"; //$NON-NLS-1$

	private String source;
	private String target;
	private Boolean recursive;
	private OverwriteMode overwrite;
	
	private String defaultSource;
	private String defaultTarget;
	private Boolean defaultRecursive;
	private OverwriteMode defaultOverwrite;
	
	public CopyFiles(String id, TransformationGraph graph) {
		super(id, graph);
	}
	
	private void setSource(String source) {
		this.defaultSource = source;
	}

	private void setTarget(String target) {
		this.defaultTarget = target;
	}

	private void setRecursive(Boolean recursive) {
		this.defaultRecursive = recursive;
	}

	private void setOverwrite(OverwriteMode overwrite) {
		this.defaultOverwrite = overwrite;
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		if ((inputPort == null) && StringUtils.isEmpty(inputMappingCode)) {
			if (StringUtils.isEmpty(defaultSource)) {
	            status.add(FileOperationComponentMessages.getString("AbstractFileOperation.source_URI_required"), Severity.ERROR, this, Priority.NORMAL, XML_SOURCE_ATTRIBUTE); //$NON-NLS-1$
			}
			if (StringUtils.isEmpty(defaultTarget)) {
	            status.add(FileOperationComponentMessages.getString("AbstractFileOperation.target_URI_required"), Severity.ERROR, this, Priority.NORMAL, XML_TARGET_ATTRIBUTE); //$NON-NLS-1$
			}
		}
		
		return status;
	}

	@Override
	protected void processInputParamsRecord() {
		//define source URI
		CharSequence runtimeSource = (CharSequence) inputParamsRecord.getField(IP_SOURCE_INDEX).getValue();
		source = runtimeSource != null ? resolver.resolveRef(runtimeSource.toString(), RefResFlag.SPEC_CHARACTERS_OFF) : null;

		//define target URI
		CharSequence runtimeTarget = (CharSequence) inputParamsRecord.getField(IP_TARGET_INDEX).getValue();
		target = runtimeTarget != null ? resolver.resolveRef(runtimeTarget.toString(), RefResFlag.SPEC_CHARACTERS_OFF) : null;

		//set recursive mode
		recursive = (Boolean) inputParamsRecord.getField(IP_RECURSIVE_INDEX).getValue();

		//set overwrite mode
		if (Boolean.TRUE.equals(inputParamsRecord.getField(IP_NO_OVERWRITE_INDEX).getValue())) {
			overwrite = OverwriteMode.NEVER;
		} else if (Boolean.TRUE.equals(inputParamsRecord.getField(IP_UPDATE_INDEX).getValue())) {
			overwrite = OverwriteMode.UPDATE;
		} else {
			overwrite = null;
		}
	}
	
	@Override
	protected void setDefaultParameters() {
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_SOURCE_NAME, defaultSource);
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_TARGET_NAME, defaultTarget);
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_RECURSIVE_NAME, defaultRecursive);
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_UPDATE_NAME, defaultOverwrite == OverwriteMode.UPDATE);
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_NO_OVERWRITE_NAME, defaultOverwrite == OverwriteMode.NEVER);
	}

	@Override
	protected void populateInputParamsRecord() {
		//store source URI
		inputParamsRecord.getField(IP_SOURCE_INDEX).setValue(source);
		//store target URI
		inputParamsRecord.getField(IP_TARGET_INDEX).setValue(target);
		//store recursive mode
		inputParamsRecord.getField(IP_RECURSIVE_INDEX).setValue(recursive);
		//store update mode
		inputParamsRecord.getField(IP_UPDATE_INDEX).setValue(OverwriteMode.UPDATE.equals(overwrite));
		inputParamsRecord.getField(IP_NO_OVERWRITE_INDEX).setValue(OverwriteMode.NEVER.equals(overwrite));
	}
	
	

	@Override
	protected void logSuccess() {
		SingleCloverURI sourceUri = result.getSource(index);
		SingleCloverURI resultUri = result.getResult(index);
		String message = MessageFormat.format(FileOperationComponentMessages.getString("CopyFiles.copy_success"), sourceUri.getPath(), resultUri.getPath());  //$NON-NLS-1$
		tokenTracker.logMessage(inputRecord, Level.INFO, message, null);
	}

	@Override
	protected void logError() {
		String message = null;
		Exception ex = result.getException();
		if (ex != null) {
			message = ex.getMessage();
		} else {
			String error = result.getError(index);
			SingleCloverURI sourceUri = result.getSource(index);
			message = MessageFormat.format(FileOperationComponentMessages.getString("CopyFiles.copy_failed"), sourceUri.getPath(), error); //$NON-NLS-1$
		}
		 
		tokenTracker.logMessage(inputRecord, Level.INFO, message, null);
	}
	
	@Override
	protected void fail() throws JetelRuntimeException {
		Exception ex = result.getException();
		if (ex != null) {
			throw new JetelRuntimeException(MessageFormat.format("Failed to copy {0} to {1}", source, target), ex);
		} else {
			throw new JetelRuntimeException(MessageFormat.format("Failed to copy {0} to {1}: {2}", result.getSource(index).getPath(), result.getTarget(index).getPath(), result.getError(index)));
		}
	}

	@Override
	protected CopyResult executeOperation() throws InterruptedException {
		String message = MessageFormat.format(FileOperationComponentMessages.getString("CopyFiles.copying"), source, target); //$NON-NLS-1$
		tokenTracker.logMessage(inputRecord, Level.INFO, message, null);
		
		CopyParameters params = new CopyParameters();
		if (recursive != null) {
			params.setRecursive(recursive);
		}
		if (overwrite != null) {
			params.setOverwriteMode(overwrite);
		}
		return manager.copy(source, target, params);
	}
	
	@Override
	protected void populateResultRecord() {
		Exception ex = result.getException();
		if (ex != null) {
			resultRecord.getField(RS_RESULT_INDEX).setValue(false);
			resultRecord.getField(RS_ERROR_MESSAGE_INDEX).setValue(ex.getMessage());
			resultRecord.getField(RS_SOURCE_URI_INDEX).setValue(source);
			resultRecord.getField(RS_TARGET_URI_INDEX).setValue(target);
		} else {
			boolean success = result.success(index);
			resultRecord.getField(RS_RESULT_INDEX).setValue(success);
			if (success) {
				resultRecord.getField(RS_RESULT_URI_INDEX).setValue(result.getResult(index).getPath());
			} else {
				resultRecord.getField(RS_ERROR_MESSAGE_INDEX).setValue(result.getError(index));
			}
			resultRecord.getField(RS_SOURCE_URI_INDEX).setValue(result.getSource(index).getPath());
			resultRecord.getField(RS_TARGET_URI_INDEX).setValue(result.getTarget(index).getPath());
		}
	}
	
	@Override
	protected void populateErrorRecord() {
		errorRecord.getField(ERR_RESULT_INDEX).setValue(false);
		Exception ex = result.getException();
		if (ex != null) {
			errorRecord.getField(ERR_ERROR_MESSAGE_INDEX).setValue(ex.getMessage());
			errorRecord.getField(ERR_SOURCE_URI_INDEX).setValue(source);
			errorRecord.getField(ERR_TARGET_URI_INDEX).setValue(target);
		} else {
			errorRecord.getField(ERR_ERROR_MESSAGE_INDEX).setValue(result.getError(index));
			errorRecord.getField(ERR_SOURCE_URI_INDEX).setValue(result.getSource(index).getPath());
			errorRecord.getField(ERR_TARGET_URI_INDEX).setValue(result.getTarget(index).getPath());
		}
	}

	public static DataRecordMetadata staticCreateInputParamsMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(INPUT_PARAMETERS_RECORD_NAME);
		
		metadata.addField(IP_SOURCE_INDEX, new DataFieldMetadata(IP_SOURCE_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_TARGET_INDEX, new DataFieldMetadata(IP_TARGET_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_RECURSIVE_INDEX, new DataFieldMetadata(IP_RECURSIVE_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(IP_UPDATE_INDEX, new DataFieldMetadata(IP_UPDATE_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(IP_NO_OVERWRITE_INDEX, new DataFieldMetadata(IP_NO_OVERWRITE_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		
		return metadata;
	}
	
	public static DataRecordMetadata staticCreateResultMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(RESULT_RECORD_NAME);
		
		metadata.addField(RS_SOURCE_URI_INDEX, new DataFieldMetadata(RS_SOURCE_URI_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(RS_TARGET_URI_INDEX, new DataFieldMetadata(RS_TARGET_URI_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(RS_RESULT_URI_INDEX, new DataFieldMetadata(RS_RESULT_URI_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(RS_RESULT_INDEX, new DataFieldMetadata(RS_RESULT_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(RS_ERROR_MESSAGE_INDEX, new DataFieldMetadata(RS_ERROR_MESSAGE_NAME, DataFieldType.STRING, DUMMY_DELIMITER));

		return metadata;
	}

	public static DataRecordMetadata staticCreateErrorMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(ERROR_RECORD_NAME);
		
		metadata.addField(ERR_RESULT_INDEX, new DataFieldMetadata(ERR_RESULT_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(ERR_ERROR_MESSAGE_INDEX, new DataFieldMetadata(ERR_ERROR_MESSAGE_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(ERR_SOURCE_URI_INDEX, new DataFieldMetadata(ERR_SOURCE_URI_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(ERR_TARGET_URI_INDEX, new DataFieldMetadata(ERR_TARGET_URI_NAME, DataFieldType.STRING, DUMMY_DELIMITER));

		return metadata;
	}

	@Override
	protected DataRecordMetadata createInputParamsMetadata() {
		return staticCreateInputParamsMetadata();
	}

	@Override
	protected DataRecordMetadata createResultMetadata() {
		return staticCreateResultMetadata();
	}

	@Override
	protected DataRecordMetadata createErrorMetadata() {
		return staticCreateErrorMetadata();
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

    public static Node fromXML(TransformationGraph transformationGraph, Element xmlElement) throws XMLConfigurationException {
        CopyFiles copyFiles = null;

        ComponentXMLAttributes componentAttributes = new ComponentXMLAttributes(xmlElement, transformationGraph);

        try {
            if (!componentAttributes.getString(XML_TYPE_ATTRIBUTE).equalsIgnoreCase(COMPONENT_TYPE)) {
                throw new XMLConfigurationException(MessageFormat.format(FileOperationComponentMessages.getString("AbstractFileOperation.invalid_attribute_value"), StringUtils.quote(XML_TYPE_ATTRIBUTE))); //$NON-NLS-1$
            }

            copyFiles = new CopyFiles(componentAttributes.getString(XML_ID_ATTRIBUTE), transformationGraph);

            copyFiles.setSource(componentAttributes.getStringEx(XML_SOURCE_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
            copyFiles.setTarget(componentAttributes.getStringEx(XML_TARGET_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
            if (componentAttributes.exists(XML_RECURSIVE_ATTRIBUTE)) {
            	copyFiles.setRecursive(componentAttributes.getBoolean(XML_RECURSIVE_ATTRIBUTE, false));
            }
            if (componentAttributes.exists(XML_OVERWRITE_ATTRIBUTE)) {
            	String overwrite = componentAttributes.getString(XML_OVERWRITE_ATTRIBUTE, null);
            	if (overwrite != null) {
            		copyFiles.setOverwrite(OverwriteMode.fromStringIgnoreCase(overwrite));
            	}
            }
            AbstractFileOperation.fromXML(copyFiles, componentAttributes);
        } catch (AttributeNotFoundException exception) {
            throw new XMLConfigurationException(FileOperationComponentMessages.getString("AbstractFileOperation.missing_attribute"), exception); //$NON-NLS-1$
        } catch (Exception exception) {
            throw new XMLConfigurationException(FileOperationComponentMessages.getString("AbstractFileOperation.error_creating_component"), exception); //$NON-NLS-1$
        }

        return copyFiles;
    }

}
