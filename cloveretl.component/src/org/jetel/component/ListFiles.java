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
import java.util.List;

import org.apache.log4j.Level;
import org.jetel.component.fileoperation.Info;
import org.jetel.component.fileoperation.SimpleParameters.ListParameters;
import org.jetel.component.fileoperation.SingleCloverURI;
import org.jetel.component.fileoperation.result.ListResult;
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
 * @created 3.4.2012
 */
public class ListFiles extends AbstractFileOperation<ListResult> {

	/** the type of the component */
    private static final String COMPONENT_TYPE = "LIST_FILES"; //$NON-NLS-1$

	// XML attribute names
	private static final String XML_TARGET_ATTRIBUTE = "target"; //$NON-NLS-1$
	private static final String XML_RECURSIVE_ATTRIBUTE = "recursive"; //$NON-NLS-1$

	private String target;
	private Boolean recursive;
	
	private String defaultTarget;
	private Boolean defaultRecursive;

    private static final int ERR_LIST_TARGET_INDEX = 2;
    private static final String ERR_LIST_TARGET_NAME = "listTarget"; //$NON-NLS-1$

    private static final String RESULT_RECORD_NAME = "Result"; //$NON-NLS-1$

    private static final int RS_URI_INDEX = 0;
    private static final int RS_NAME_INDEX = 1;
    private static final int RS_CAN_READ_INDEX = 2;
    private static final int RS_CAN_WRITE_INDEX = 3;
    private static final int RS_CAN_EXECUTE_INDEX = 4;
    private static final int RS_IS_DIRECTORY_INDEX = 5;
    private static final int RS_IS_FILE_INDEX = 6;
    private static final int RS_IS_HIDDEN_INDEX = 7;
    private static final int RS_LAST_MODIFIED_INDEX = 8;
    private static final int RS_SIZE_INDEX = 9;
    private static final int RS_RESULT_INDEX = 10;
    private static final int RS_ERROR_MESSAGE_INDEX = 11;
    private static final int RS_LIST_TARGET_INDEX = 12;
    
    private static final String RS_URI_NAME = "uri"; //$NON-NLS-1$
    private static final String RS_NAME_NAME = "name"; //$NON-NLS-1$
    private static final String RS_CAN_READ_NAME = "canRead"; //$NON-NLS-1$
    private static final String RS_CAN_WRITE_NAME = "canWrite"; //$NON-NLS-1$
    private static final String RS_CAN_EXECUTE_NAME = "canExecute"; //$NON-NLS-1$
    private static final String RS_IS_DIRECTORY_NAME = "isDirectory"; //$NON-NLS-1$
    private static final String RS_IS_FILE_NAME = "isFile"; //$NON-NLS-1$
    private static final String RS_IS_HIDDEN_NAME = "isHidden"; //$NON-NLS-1$
    private static final String RS_LAST_MODIFIED_NAME = "lastModified"; //$NON-NLS-1$
    private static final String RS_SIZE_NAME = "size"; //$NON-NLS-1$
    private static final String RS_RESULT_NAME = ERR_RESULT_NAME;
    private static final String RS_ERROR_MESSAGE_NAME = ERR_ERROR_MESSAGE_NAME;
    private static final String RS_LIST_TARGET_NAME = ERR_LIST_TARGET_NAME;

    
    private static final String INPUT_PARAMETERS_RECORD_NAME = "Attributes"; //$NON-NLS-1$

    private static final int IP_TARGET_INDEX = 0;
    private static final int IP_RECURSIVE_INDEX = 1;

    private static final String IP_TARGET_NAME = XML_TARGET_ATTRIBUTE;
    private static final String IP_RECURSIVE_NAME = XML_RECURSIVE_ATTRIBUTE;
    
    private Info info;

	public ListFiles(String id, TransformationGraph graph) {
		super(id, graph);
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	private void setTarget(String target) {
		this.defaultTarget = target;
	}

	private void setRecursive(Boolean recursive) {
		this.defaultRecursive = recursive;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		if ((inputPort == null) && StringUtils.isEmpty(inputMappingCode)) {
			if (StringUtils.isEmpty(defaultTarget)) {
	            status.add(FileOperationComponentMessages.getString("AbstractFileOperation.target_URI_required"), Severity.ERROR, this, Priority.NORMAL, XML_TARGET_ATTRIBUTE); //$NON-NLS-1$
			}
		}
		
		return status;
	}

	@Override
	protected void processInputParamsRecord() {
		//define target URI
		CharSequence runtimeTarget = (CharSequence) inputParamsRecord.getField(IP_TARGET_INDEX).getValue();
		target = runtimeTarget != null ? resolver.resolveRef(runtimeTarget.toString(), RefResFlag.SPEC_CHARACTERS_OFF) : null;

		//define recursive mode
		recursive = (Boolean) inputParamsRecord.getField(IP_RECURSIVE_INDEX).getValue();
	}

	@Override
	protected void setDefaultParameters() {
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_TARGET_NAME, defaultTarget);
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_RECURSIVE_NAME, defaultRecursive);
	}

	@Override
	protected void populateInputParamsRecord() {
		//store target URI
		inputParamsRecord.getField(IP_TARGET_INDEX).setValue(target);
		//store recursive mode
		inputParamsRecord.getField(IP_RECURSIVE_INDEX).setValue(recursive);
	}

	@Override
	protected void populateResultRecord() {
		Exception ex = result.getException();
		if (ex != null) {
			resultRecord.getField(RS_RESULT_INDEX).setValue(false);
			resultRecord.getField(RS_LIST_TARGET_INDEX).setValue(target);
			resultRecord.getField(RS_ERROR_MESSAGE_INDEX).setValue(ex.getMessage());
		} else {
			boolean success = result.success(index);
			resultRecord.getField(RS_RESULT_INDEX).setValue(success);
			resultRecord.getField(RS_LIST_TARGET_INDEX).setValue(result.getURI(index).getPath());
			if (success) {
				resultRecord.getField(RS_URI_INDEX).setValue(info.getURI().toString());
				resultRecord.getField(RS_NAME_INDEX).setValue(info.getName());
				resultRecord.getField(RS_CAN_READ_INDEX).setValue(info.canRead());
				resultRecord.getField(RS_CAN_WRITE_INDEX).setValue(info.canWrite());
				resultRecord.getField(RS_CAN_EXECUTE_INDEX).setValue(info.canExecute());
				resultRecord.getField(RS_IS_DIRECTORY_INDEX).setValue(info.isDirectory());
				resultRecord.getField(RS_IS_FILE_INDEX).setValue(info.isFile());
				resultRecord.getField(RS_IS_HIDDEN_INDEX).setValue(info.isHidden());
				resultRecord.getField(RS_LAST_MODIFIED_INDEX).setValue(info.getLastModified());
				resultRecord.getField(RS_SIZE_INDEX).setValue(info.getSize());
			} else {
				resultRecord.getField(RS_ERROR_MESSAGE_INDEX).setValue(result.getError(index));
			}
		}
	}

	@Override
	protected void populateErrorRecord() {
		errorRecord.getField(ERR_RESULT_INDEX).setValue(false);
		Exception ex = result.getException();
		if (ex != null) {
			errorRecord.getField(ERR_LIST_TARGET_INDEX).setValue(target);
			errorRecord.getField(ERR_ERROR_MESSAGE_INDEX).setValue(ex.getMessage());
		} else {
			errorRecord.getField(ERR_LIST_TARGET_INDEX).setValue(result.getURI(index).getPath());
			errorRecord.getField(ERR_ERROR_MESSAGE_INDEX).setValue(result.getError(index));
		}
	}
	
	@Override
	protected void logSuccess() {
	}

	@Override
	protected void logError() {
		String message = null;
		Exception ex = result.getException();
		if (ex != null) {
			message = ex.getMessage();
		} else {
			String error = result.getError(index);
			SingleCloverURI uri = result.getURI(index);
			message = MessageFormat.format(FileOperationComponentMessages.getString("ListFiles.listing_failed"), uri.getPath(), error);  //$NON-NLS-1$
		}
		
		tokenTracker.logMessage(inputRecord, Level.INFO, message, null);
	}

	@Override
	protected void fail() throws JetelRuntimeException {
		Exception ex = result.getException();
		if (ex != null) {
			throw new JetelRuntimeException(MessageFormat.format("Failed to list {0}", target), ex);
		} else {
			throw new JetelRuntimeException(MessageFormat.format("Failed to list {0}: {1}", result.getURI(index).getPath(), result.getError(index)));
		}
	}

	protected void processResult() throws InterruptedException {
		if (result.getException() != null) {
			processError();
		} else {
			for (index = 0; index < result.totalCount(); index++) {
				if (result.success(index)) {
					List<Info> infos = result.getResult(index);
					for (Info currentInfo: infos) {
						info = currentInfo;
						processSuccess();
					}
				} else {
					processError();
				}
			}
		}
	}

	@Override
	protected ListResult executeOperation() throws InterruptedException {
		String message = MessageFormat.format(FileOperationComponentMessages.getString("ListFiles.listing"), target); //$NON-NLS-1$
		tokenTracker.logMessage(inputRecord, Level.INFO, message, null);
		
		ListParameters params = new ListParameters();
		if (recursive != null) {
			params.setRecursive(recursive);
		}
		return manager.list(target, params);
	}

	public static DataRecordMetadata staticCreateInputParamsMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(INPUT_PARAMETERS_RECORD_NAME);
		
		metadata.addField(IP_TARGET_INDEX, new DataFieldMetadata(IP_TARGET_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_RECURSIVE_INDEX, new DataFieldMetadata(IP_RECURSIVE_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		
		return metadata;
	}
	
	public static DataRecordMetadata staticCreateResultMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(RESULT_RECORD_NAME);
		
		metadata.addField(RS_URI_INDEX, new DataFieldMetadata(RS_URI_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(RS_NAME_INDEX, new DataFieldMetadata(RS_NAME_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(RS_CAN_READ_INDEX, new DataFieldMetadata(RS_CAN_READ_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(RS_CAN_WRITE_INDEX, new DataFieldMetadata(RS_CAN_WRITE_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(RS_CAN_EXECUTE_INDEX, new DataFieldMetadata(RS_CAN_EXECUTE_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(RS_IS_DIRECTORY_INDEX, new DataFieldMetadata(RS_IS_DIRECTORY_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(RS_IS_FILE_INDEX, new DataFieldMetadata(RS_IS_FILE_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(RS_IS_HIDDEN_INDEX, new DataFieldMetadata(RS_IS_HIDDEN_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(RS_LAST_MODIFIED_INDEX, new DataFieldMetadata(RS_LAST_MODIFIED_NAME, DataFieldType.DATE, DUMMY_DELIMITER));
		metadata.addField(RS_SIZE_INDEX, new DataFieldMetadata(RS_SIZE_NAME, DataFieldType.LONG, DUMMY_DELIMITER));
		metadata.addField(RS_RESULT_INDEX, new DataFieldMetadata(RS_RESULT_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(RS_ERROR_MESSAGE_INDEX, new DataFieldMetadata(RS_ERROR_MESSAGE_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(RS_LIST_TARGET_INDEX, new DataFieldMetadata(RS_LIST_TARGET_NAME, DataFieldType.STRING, DUMMY_DELIMITER));

		return metadata;
	}

	public static DataRecordMetadata staticCreateErrorMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(ERROR_RECORD_NAME);
		
		metadata.addField(ERR_RESULT_INDEX, new DataFieldMetadata(ERR_RESULT_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(ERR_ERROR_MESSAGE_INDEX, new DataFieldMetadata(ERR_ERROR_MESSAGE_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(ERR_LIST_TARGET_INDEX, new DataFieldMetadata(ERR_LIST_TARGET_NAME, DataFieldType.STRING, DUMMY_DELIMITER));

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

    public static Node fromXML(TransformationGraph transformationGraph, Element xmlElement) throws XMLConfigurationException {
        ListFiles listFiles = null;

        ComponentXMLAttributes componentAttributes = new ComponentXMLAttributes(xmlElement, transformationGraph);

        try {
            if (!componentAttributes.getString(XML_TYPE_ATTRIBUTE).equalsIgnoreCase(COMPONENT_TYPE)) {
                throw new XMLConfigurationException(MessageFormat.format(FileOperationComponentMessages.getString("AbstractFileOperation.invalid_attribute_value"), StringUtils.quote(XML_TYPE_ATTRIBUTE))); //$NON-NLS-1$
            }

            listFiles = new ListFiles(componentAttributes.getString(XML_ID_ATTRIBUTE), transformationGraph);

            listFiles.setTarget(componentAttributes.getStringEx(XML_TARGET_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
            if (componentAttributes.exists(XML_RECURSIVE_ATTRIBUTE)) {
            	listFiles.setRecursive(componentAttributes.getBoolean(XML_RECURSIVE_ATTRIBUTE, false));
            }
            AbstractFileOperation.fromXML(listFiles, componentAttributes);
        } catch (AttributeNotFoundException exception) {
            throw new XMLConfigurationException(FileOperationComponentMessages.getString("AbstractFileOperation.missing_attribute"), exception); //$NON-NLS-1$
        } catch (Exception exception) {
            throw new XMLConfigurationException(FileOperationComponentMessages.getString("AbstractFileOperation.error_creating_component"), exception); //$NON-NLS-1$
        }

        return listFiles;
    }

}
