package com.cloveretl.components;

import java.io.ByteArrayOutputStream;

import org.jetel.component.denormalize.CTLRecordDenormalize;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.StringDataField;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;
import org.jetel.metadata.DataRecordParsingType;
import org.jetel.util.string.CloverString;

public class DataRecordMetadataDenormalize extends CTLRecordDenormalize {

	private DataRecordMetadata metadata;
	private String recordName;
	private String fileName;
	
	private enum FieldProperty {
		FILE_NAME("fileName"),
		RECORD_NAME("recordName"),
		NAME("name"),
		LABEL("label"),
		SIZE("size"),
		TYPE("type"),
		CONTAINER_TYPE("containerType"),
		DELIMITER("delimiter"),
		FORMAT("format"),
		LOCALE("locale"),
		TIMEZONE("timezone"),
		NULL_VALUE("nullValue"),
		DEFAULT("default"),
		DESCRIPTION("description"),
		;
		
		private final String propertyName;
		
		FieldProperty(String propertyName) {
			this.propertyName = propertyName;
		}
		
		public String getPropertyName() {
			return propertyName;
		}
	}
	
	private int[] indicies;
	private DataRecordMetadata inputMeta;

	@Override
	protected Integer appendDelegate() throws ComponentNotReadyException,
			TransformException {
		if (fileName == null || recordName == null) {
			fileName = getString(FieldProperty.FILE_NAME);
			recordName = getString(FieldProperty.RECORD_NAME);
		}
		
		String name = getString(FieldProperty.NAME);
		String label = getString(FieldProperty.LABEL);
		String type = getString(FieldProperty.TYPE);
		IntegerDataField sizeField = (IntegerDataField) getInputField(FieldProperty.SIZE);
		StringDataField delimiterField = getStringField(FieldProperty.DELIMITER);
		
		DataFieldMetadata field;
		if (!delimiterField.isNull()) {
			String delimiter = delimiterField.toString();
			delimiter = resolve(delimiter);
			
			field = new DataFieldMetadata(name, delimiter);
		}
		else if (!sizeField.isNull()) {
			int size = sizeField.getInt();
			field = new DataFieldMetadata(name, size);
		}
		else {
			field = new DataFieldMetadata(name, null);
		}
		
		field.setLabel(label);
		field.setDataType(DataFieldType.fromName(type));
		field.setLocaleStr(getString(FieldProperty.LOCALE));
		field.setFormatStr(getString(FieldProperty.FORMAT));
		field.setTimeZoneStr(getString(FieldProperty.TIMEZONE));
		field.setNullValue(getString(FieldProperty.NULL_VALUE));
		field.setDescription(getString(FieldProperty.DESCRIPTION));
		field.setDefaultValueStr(getString(FieldProperty.DEFAULT));
		metadata.addField(field);
		
		return OK;
	}

	private String resolve(String delimiter) {
		return getGraph().getPropertyRefResolver().resolveRef(delimiter);
	}

	@Override
	protected Integer transformDelegate() throws ComponentNotReadyException,
			TransformException {
		
		if (recordName != null && !recordName.isEmpty()) {
			metadata.setName(recordName);
		}
		else {
			recordName = metadata.getName();
		}
		
		if (fileName == null) {
			fileName = recordName;
		}
		outputRecord.getField(0).setValue(fileName);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataRecordMetadataXMLReaderWriter.write(metadata, bos);
		ByteDataField outputField = ((ByteDataField) outputRecord.getField(1));
		outputField.setValue(bos.toByteArray());
		
		return OK;
	}
	
	@Override
	public void globalScopeInit() throws ComponentNotReadyException {
	}
	
	@Override
	public void clean() {
		super.clean();
		
		fileName = null;
		recordName = null;
		metadata.delAllFields();
	}
	
	@Override
	protected Boolean initDelegate() throws ComponentNotReadyException {
		inputMeta = getNode().getInMetadata().get(0);
		if (inputMeta == null) {
			throw new ComponentNotReadyException("No metadata for input port 0");
		}
		indicies = new int[FieldProperty.values().length];
		for (FieldProperty property : FieldProperty.values()) {
			setIndex(property);
		}
		
		String metadataName = getGraph().getGraphParameters().getGraphParameter("METADATA_NAME").getValue();
		metadata = new DataRecordMetadata(metadataName);
		
		String metadataType = getGraph().getGraphParameters().getGraphParameter("METADATA_TYPE").getValue();
		
		if (metadataType == null || metadataType.isEmpty()) {
			metadataType = "delimited";
		}
		
		if ("delimited".equalsIgnoreCase(metadataType)) {
			metadata.setParsingType(DataRecordParsingType.DELIMITED);
		}
		else if ("fixed".equalsIgnoreCase(metadataType)) {
			metadata.setParsingType(DataRecordParsingType.FIXEDLEN);
		}
		else if ("mixed".equalsIgnoreCase(metadataType)) {
			metadata.setParsingType(DataRecordParsingType.MIXED);
		}
		else {
			throw new ComponentNotReadyException("Unknown metadata type '" + metadataType + "'");
		}
		
		String recordDelimiter = getGraph().getGraphParameters().getGraphParameter("METADATA_RECORD_DELIMITER").getValue();
		String fieldDelimiter = getGraph().getGraphParameters().getGraphParameter("METADATA_FIELD_DELIMITER").getValue();
		metadata.setRecordDelimiter(resolve(recordDelimiter));
		metadata.setFieldDelimiter(resolve(fieldDelimiter));
		
		String metadataSizeParam = getGraph().getGraphParameters().getGraphParameter("METADATA_SIZE").getValue();
		if (metadataSizeParam != null && !metadataSizeParam.isEmpty()) {
			int size = Integer.parseInt(metadataSizeParam);
			metadata.setRecordSize(size);
		}
		
		return super.initDelegate();
	}

	private void setIndex(FieldProperty property) throws ComponentNotReadyException {
		String propertyName = property.getPropertyName();
		int position = inputMeta.getFieldPosition(propertyName);
		if (position < 0) {
			throw new ComponentNotReadyException("Cannot find field " + propertyName);
		}
		indicies[property.ordinal()] = position;
	}
	
	private String getString(FieldProperty property) {
		CloverString value = getStringField(property).getValue();
		return value != null ? value.toString() : null;
	}
	
	private StringDataField getStringField(FieldProperty property) {
		return (StringDataField) getInputField(property);
	}
	
	private DataField getInputField(FieldProperty property) {
		return inputRecord.getField(getIndex(property));
	}
	
	private int getIndex(FieldProperty property) {
		return indicies[property.ordinal()];
	}
	
}
