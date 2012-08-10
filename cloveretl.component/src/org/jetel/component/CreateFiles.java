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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Level;
import org.jetel.component.fileoperation.SimpleParameters.CreateParameters;
import org.jetel.component.fileoperation.result.CreateResult;
import org.jetel.data.Defaults;
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
public class CreateFiles extends AbstractFileOperation<CreateResult> {

	/** the type of the component */
    private static final String COMPONENT_TYPE = "CREATE_FILES"; //$NON-NLS-1$

	// XML attribute names
	private static final String XML_TARGET_ATTRIBUTE = "fileURL"; //$NON-NLS-1$
	private static final String XML_DIRECTORY_ATTRIBUTE = "directory"; //$NON-NLS-1$
	private static final String XML_MAKE_PARENT_DIRS_ATTRIBUTE = "makeParentDirs"; //$NON-NLS-1$
	private static final String XML_MODIFIED_DATE_ATTRIBUTE = "modifiedDate"; //$NON-NLS-1$

    private static final String INPUT_PARAMETERS_RECORD_NAME = "Attributes"; //$NON-NLS-1$

    private static final int IP_TARGET_INDEX = 0;
    private static final int IP_DIRECTORY_INDEX = 1;
    private static final int IP_MAKE_PARENTS_INDEX = 2;
    private static final int IP_MODIFIED_DATE_INDEX = 3;

    private static final String IP_TARGET_NAME = XML_TARGET_ATTRIBUTE;
    private static final String IP_DIRECTORY_NAME = XML_DIRECTORY_ATTRIBUTE;
    private static final String IP_MAKE_PARENTS_NAME = XML_MAKE_PARENT_DIRS_ATTRIBUTE;
    private static final String IP_MODIFIED_DATE_NAME = XML_MODIFIED_DATE_ATTRIBUTE;

    private static final int ERR_TARGET_URL_INDEX = 2;
    
    private static final String ERR_TARGET_URL_NAME = XML_TARGET_ATTRIBUTE;

    private static final String RESULT_RECORD_NAME = "Result"; //$NON-NLS-1$

    private static final int RS_TARGET_URL_INDEX = 0;
    private static final int RS_RESULT_INDEX = 1;
    private static final int RS_ERROR_MESSAGE_INDEX = 2;

    private static final String RS_TARGET_URL_NAME = XML_TARGET_ATTRIBUTE;
    private static final String RS_RESULT_NAME = ERR_RESULT_NAME;
    private static final String RS_ERROR_MESSAGE_NAME = ERR_ERROR_MESSAGE_NAME;

    private String target;
	private Boolean directory;
	private Boolean makeParents;
	private Date modifiedDate;
	
    private String defaultTarget;
	private Boolean defaultDirectory;
	private Boolean defaultMakeParents;
	private Date defaultModifiedDate;
	
	public CreateFiles(String id, TransformationGraph graph) {
		super(id, graph);
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

		//set directory mode
		directory = (Boolean) inputParamsRecord.getField(IP_DIRECTORY_INDEX).getValue();
		//set make parents
		makeParents = (Boolean) inputParamsRecord.getField(IP_MAKE_PARENTS_INDEX).getValue();
		//set modified date
		modifiedDate = (Date) inputParamsRecord.getField(IP_MODIFIED_DATE_INDEX).getValue();
	}

	@Override
	protected void setDefaultParameters() {
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_TARGET_NAME, defaultTarget);
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_DIRECTORY_NAME, defaultDirectory);
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_MAKE_PARENTS_NAME, defaultMakeParents);
		inputMapping.setDefaultOutputValue(PARAMS_RECORD_ID, IP_MODIFIED_DATE_NAME, defaultModifiedDate);
	}

	@Override
	protected void logSuccess() {
		String message = MessageFormat.format(FileOperationComponentMessages.getString("CreateFiles.create_success"), getTargetPath());  //$NON-NLS-1$
		tokenTracker.logMessage(inputRecord, Level.INFO, message, null);
	}

	@Override
	protected void logError() {
		String message = null;
		Exception ex = result.getException();
		if (ex != null) {
			message = ex.getMessage();
		} else {
			message = MessageFormat.format(FileOperationComponentMessages.getString("CreateFiles.create_failed"), getTargetPath(), getError());  //$NON-NLS-1$
		}
		
		tokenTracker.logMessage(inputRecord, Level.INFO, message, null);
	}

	@Override
	protected void fail() throws JetelRuntimeException {
		Exception ex = result.getException();
		if (ex != null) {
			throw new JetelRuntimeException(MessageFormat.format("Failed to create {0}", target), ex);
		} else {
			throw new JetelRuntimeException(MessageFormat.format("Failed to create {0}: {1}", getTargetPath(), getError()));
		}
	}

	@Override
	protected CreateResult executeOperation() throws InterruptedException {
		String message = MessageFormat.format(FileOperationComponentMessages.getString("CreateFiles.creating"), target); //$NON-NLS-1$
		tokenTracker.logMessage(inputRecord, Level.INFO, message, null);
		
		CreateParameters params = new CreateParameters();
		if (directory != null) {
			params.setDirectory(directory);
		}
		if (makeParents != null) {
			params.setMakeParents(makeParents);
		}
		if (modifiedDate != null) {
			params.setLastModified(modifiedDate);
		}
		return manager.create(target, params);
	}

	@Override
	protected CreateResult createSkippedResult() {
		return new CreateResult().setException(new RuntimeException("Skipped because one of the previous operations failed. Disable the 'Stop on fail' attribute to continue processing."));
	}

	private String getTargetPath() {
		if ((result.getException() == null) && verboseOutput || (result.totalCount() == 1)) {
			return result.getURI(index).getPath();
		}

		return target;
	}

	@Override
	protected void populateResultRecord() {
		if ((verboseOutput || (result.totalCount() == 1)) && (result.getException() == null)) {
			boolean success = result.success(index);
			resultRecord.getField(RS_RESULT_INDEX).setValue(success);
			if (!success) {
				resultRecord.getField(RS_ERROR_MESSAGE_INDEX).setValue(result.getError(index));
			}
			resultRecord.getField(RS_TARGET_URL_INDEX).setValue(result.getURI(index).getPath());
		} else {
			resultRecord.getField(RS_RESULT_INDEX).setValue(result.success());
			resultRecord.getField(RS_ERROR_MESSAGE_INDEX).setValue(result.getFirstErrorMessage());
			resultRecord.getField(RS_TARGET_URL_INDEX).setValue(target);
		}
	}
	
	@Override
	protected void populateErrorRecord() {
		errorRecord.getField(ERR_RESULT_INDEX).setValue(false);
		if ((verboseOutput || (result.totalCount() == 1)) && (result.getException() == null)) {
			errorRecord.getField(ERR_ERROR_MESSAGE_INDEX).setValue(result.getError(index));
			errorRecord.getField(ERR_TARGET_URL_INDEX).setValue(result.getURI(index).getPath());
		} else {
			errorRecord.getField(ERR_ERROR_MESSAGE_INDEX).setValue(result.getFirstErrorMessage());
			errorRecord.getField(ERR_TARGET_URL_INDEX).setValue(target);
		}
	}

	public static DataRecordMetadata staticCreateInputParamsMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(INPUT_PARAMETERS_RECORD_NAME);
		
		metadata.addField(IP_TARGET_INDEX, new DataFieldMetadata(IP_TARGET_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(IP_DIRECTORY_INDEX, new DataFieldMetadata(IP_DIRECTORY_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(IP_MAKE_PARENTS_INDEX, new DataFieldMetadata(IP_MAKE_PARENTS_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(IP_MODIFIED_DATE_INDEX, new DataFieldMetadata(IP_MODIFIED_DATE_NAME, DataFieldType.DATE, DUMMY_DELIMITER));
		
		return metadata;
	}
	
	public static DataRecordMetadata staticCreateResultMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(RESULT_RECORD_NAME);
		
		metadata.addField(RS_TARGET_URL_INDEX, new DataFieldMetadata(RS_TARGET_URL_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(RS_RESULT_INDEX, new DataFieldMetadata(RS_RESULT_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(RS_ERROR_MESSAGE_INDEX, new DataFieldMetadata(RS_ERROR_MESSAGE_NAME, DataFieldType.STRING, DUMMY_DELIMITER));

		return metadata;
	}

	public static DataRecordMetadata staticCreateErrorMetadata() {
		DataRecordMetadata metadata = new DataRecordMetadata(ERROR_RECORD_NAME);
		
		metadata.addField(ERR_RESULT_INDEX, new DataFieldMetadata(ERR_RESULT_NAME, DataFieldType.BOOLEAN, DUMMY_DELIMITER));
		metadata.addField(ERR_ERROR_MESSAGE_INDEX, new DataFieldMetadata(ERR_ERROR_MESSAGE_NAME, DataFieldType.STRING, DUMMY_DELIMITER));
		metadata.addField(ERR_TARGET_URL_INDEX, new DataFieldMetadata(ERR_TARGET_URL_NAME, DataFieldType.STRING, DUMMY_DELIMITER));

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

	private void setTarget(String target) {
		this.defaultTarget = target;
	}

	private void setDirectory(Boolean directory) {
		this.defaultDirectory = directory;
	}

	private void setMakeParents(Boolean makeParents) {
		this.defaultMakeParents = makeParents;
	}
	
	private void setModifiedDate(Date modifiedDate) {
		this.defaultModifiedDate = modifiedDate;
	}
	
	public static Node fromXML(TransformationGraph transformationGraph, Element xmlElement) throws XMLConfigurationException {
        CreateFiles createFiles = null;

        ComponentXMLAttributes componentAttributes = new ComponentXMLAttributes(xmlElement, transformationGraph);

        try {
            if (!componentAttributes.getString(XML_TYPE_ATTRIBUTE).equalsIgnoreCase(COMPONENT_TYPE)) {
                throw new XMLConfigurationException(MessageFormat.format(FileOperationComponentMessages.getString("AbstractFileOperation.invalid_attribute_value"), StringUtils.quote(XML_TYPE_ATTRIBUTE)));  //$NON-NLS-1$
            }

            createFiles = new CreateFiles(componentAttributes.getString(XML_ID_ATTRIBUTE), transformationGraph);

            createFiles.setTarget(componentAttributes.getStringEx(XML_TARGET_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF));
            if (componentAttributes.exists(XML_DIRECTORY_ATTRIBUTE)) {
            	createFiles.setDirectory(componentAttributes.getBoolean(XML_DIRECTORY_ATTRIBUTE, false));
            }
            if (componentAttributes.exists(XML_MAKE_PARENT_DIRS_ATTRIBUTE)) {
            	createFiles.setMakeParents(componentAttributes.getBoolean(XML_MAKE_PARENT_DIRS_ATTRIBUTE, false));
            }
            if (componentAttributes.exists(XML_MODIFIED_DATE_ATTRIBUTE)) {
            	SimpleDateFormat format = new SimpleDateFormat(Defaults.DEFAULT_DATETIME_FORMAT);
            	String dateString = componentAttributes.getString(XML_MODIFIED_DATE_ATTRIBUTE, null);
            	if (!StringUtils.isEmpty(dateString)) {
            		try {
            			createFiles.setModifiedDate(format.parse(dateString));
            		} catch (ParseException ex) {
                        throw new XMLConfigurationException(MessageFormat.format(FileOperationComponentMessages.getString("AbstractFileOperation.invalid_attribute_value"), StringUtils.quote(XML_MODIFIED_DATE_ATTRIBUTE)));  //$NON-NLS-1$
            		}
            	}
            }
            AbstractFileOperation.fromXML(createFiles, componentAttributes);
        } catch (AttributeNotFoundException exception) {
            throw new XMLConfigurationException(FileOperationComponentMessages.getString("AbstractFileOperation.missing_attribute"), exception); //$NON-NLS-1$
        } catch (Exception exception) {
            throw new XMLConfigurationException(FileOperationComponentMessages.getString("AbstractFileOperation.error_creating_component"), exception); //$NON-NLS-1$
        }

        return createFiles;
    }
	
}
