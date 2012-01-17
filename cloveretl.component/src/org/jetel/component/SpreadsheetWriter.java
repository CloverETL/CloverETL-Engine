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
import java.io.InputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.SpreadsheetFormatter;
import org.jetel.data.formatter.provider.SpreadsheetFormatterProvider;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.parser.XLSMapping;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldFormatType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.SpreadsheetUtils.SpreadsheetAttitude;
import org.jetel.util.SpreadsheetUtils.SpreadsheetExistingSheetsActions;
import org.jetel.util.SpreadsheetUtils.SpreadsheetFormat;
import org.jetel.util.SpreadsheetUtils.SpreadsheetWriteMode;
import org.jetel.util.bytes.SystemOutByteChannel;
import org.jetel.util.bytes.WritableByteChannelIterator;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * @author lkrejci & psimecek (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 5 Sep 2011
 */
public class SpreadsheetWriter extends Node {

	public static final String COMPONENT_TYPE = "SPREADSHEET_WRITER";

	public static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
	public static final String XML_TEMPLATE_FILE_URL_ATTRIBUTE = "templateFileURL";
	public static final String XML_FORMATTER_TYPE_ATTRIBUTE = "formatterType";
	public static final String XML_WRITE_MODE_ATTRIBUTE = "writeMode";
	public static final String XML_MK_DIRS_ATTRIBUTE = "makeDirs";
	public static final String XML_MAPPING_ATTRIBUTE = "mapping";
	public static final String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
	public static final String XML_SHEET_ATTRIBUTE = "sheet";
	public static final String XML_EXISTING_SHEETS_ACTIONS_ATTRIBUTE = "existingSheetsActions";
	public static final String XML_RECORD_SKIP_ATTRIBUTE = "skipRecords";
	public static final String XML_RECORD_COUNT_ATTRIBUTE = "numRecords";
	public static final String XML_RECORDS_PER_FILE = "numFileRecords";
	public static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	public static final String XML_PARTITION_ATTRIBUTE = "partition";
	public static final String XML_PARTITION_OUTFIELDS_ATTRIBUTE = "partitionOutFields";
	public static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";
	public static final String XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE = "partitionUnassignedFileName";

	private static Log LOGGER = LogFactory.getLog(SpreadsheetWriter.class);
	private static final int READ_FROM_PORT = 0;
	private static final int OUTPUT_PORT = 0;
	
	private static final HashSet<Character> supportedTypes;
	
	static {
		supportedTypes = new HashSet<Character>(9);
		supportedTypes.add(DataFieldMetadata.DATE_FIELD);
		supportedTypes.add(DataFieldMetadata.DATETIME_FIELD);
		supportedTypes.add(DataFieldMetadata.BYTE_FIELD);
		supportedTypes.add(DataFieldMetadata.STRING_FIELD);
		supportedTypes.add(DataFieldMetadata.DECIMAL_FIELD);
		supportedTypes.add(DataFieldMetadata.INTEGER_FIELD);
		supportedTypes.add(DataFieldMetadata.LONG_FIELD);
		supportedTypes.add(DataFieldMetadata.NUMERIC_FIELD);
		supportedTypes.add(DataFieldMetadata.BOOLEAN_FIELD);
	}

	public static Node fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		SpreadsheetWriter spreadsheetWriter;

		try {
			spreadsheetWriter = new SpreadsheetWriter(xattribs.getString(XML_ID_ATTRIBUTE));
			spreadsheetWriter.setFileURL(xattribs.getStringEx(XML_FILE_URL_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));
			if (xattribs.exists(XML_MK_DIRS_ATTRIBUTE)) {
				spreadsheetWriter.setMkDirs(xattribs.getBoolean(XML_MK_DIRS_ATTRIBUTE));
			}

			if (xattribs.exists(XML_FORMATTER_TYPE_ATTRIBUTE)) {
				spreadsheetWriter.setFormatterType(SpreadsheetFormat.valueOfIgnoreCase(xattribs.getString(XML_FORMATTER_TYPE_ATTRIBUTE)));
			}
			if (xattribs.exists(XML_WRITE_MODE_ATTRIBUTE)) {
				spreadsheetWriter.setWriteMode(SpreadsheetWriteMode.valueOfIgnoreCase(xattribs.getString(XML_WRITE_MODE_ATTRIBUTE)));
			}

			String mappingURL = xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF);
			if (xattribs.exists(XML_TEMPLATE_FILE_URL_ATTRIBUTE)) {
				spreadsheetWriter.setTemplateFileURL(xattribs.getString(XML_TEMPLATE_FILE_URL_ATTRIBUTE));
			}
			String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE, null);
			if (mappingURL != null) {
				spreadsheetWriter.setMappingURL(mappingURL);
			} else if (mapping != null) {
				spreadsheetWriter.setMapping(mapping);
			}

			if (xattribs.exists(XML_SHEET_ATTRIBUTE)) {
				spreadsheetWriter.setSheet(xattribs.getString(XML_SHEET_ATTRIBUTE));
			}
			if (xattribs.exists(XML_EXISTING_SHEETS_ACTIONS_ATTRIBUTE)) {
				spreadsheetWriter.setExistingSheetsActions(SpreadsheetExistingSheetsActions.valueOfIgnoreCase(xattribs.getString(XML_EXISTING_SHEETS_ACTIONS_ATTRIBUTE)));
			}

			if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)) {
				spreadsheetWriter.setRecordSkip(xattribs.getInteger(XML_RECORD_SKIP_ATTRIBUTE));
			}
			if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)) {
				spreadsheetWriter.setRecordCount(xattribs.getInteger(XML_RECORD_COUNT_ATTRIBUTE));
			}
			if (xattribs.exists(XML_RECORDS_PER_FILE)) {
				spreadsheetWriter.setRecordsPerFile(xattribs.getInteger(XML_RECORDS_PER_FILE));
			}

			if (xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE)) {
				spreadsheetWriter.setPartitionKey(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE));
			}
			if (xattribs.exists(XML_PARTITION_ATTRIBUTE)) {
				spreadsheetWriter.setPartition(xattribs.getString(XML_PARTITION_ATTRIBUTE));
			}
			if (xattribs.exists(XML_PARTITION_FILETAG_ATTRIBUTE)) {
				spreadsheetWriter.setPartitionFileTagType(xattribs.getString(XML_PARTITION_FILETAG_ATTRIBUTE));
			}
			if (xattribs.exists(XML_PARTITION_OUTFIELDS_ATTRIBUTE)) {
				spreadsheetWriter.setPartitionOutFields(xattribs.getString(XML_PARTITION_OUTFIELDS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE)) {
				spreadsheetWriter.setPartitionUnassignedFileName(xattribs.getString(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE));
			}

			return spreadsheetWriter;
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}
	}

	private String fileURL;
	private String templateFileURL;

	private boolean mkDirs;

	private SpreadsheetWriteMode writeMode = SpreadsheetWriteMode.OVERWRITE_SHEET_IN_MEMORY;
	private SpreadsheetFormat formatterType = SpreadsheetFormat.AUTO;

	private String mapping;
	private String mappingURL;

	private String sheet;
	private SpreadsheetExistingSheetsActions existingSheetsActions = SpreadsheetExistingSheetsActions.DO_NOTHING;

	private int recordSkip;
	private int recordCount;
	private int recordsPerFile;

	private String partitionKey;
	private String partition;
	private PartitionFileTagType partitionFileTagType = PartitionFileTagType.NUMBER_FILE_TAG;
	private String partitionOutFields;
	private String partitionUnassignedFileName;
	private LookupTable lookupTable = null;

	private SpreadsheetFormatterProvider formatterProvider;
	private MultiFileWriter writer;

	public SpreadsheetWriter(String id) {
		super(id);
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	public void setWriteMode(SpreadsheetWriteMode writeMode) {
		this.writeMode = writeMode;
	}

	public void setFormatterType(SpreadsheetFormat formatterType) {
		this.formatterType = formatterType;
	}

	public void setFileURL(String fileURL) {
		this.fileURL = fileURL;
	}
	
	public void setTemplateFileURL(String templateFileURL) {
		this.templateFileURL = templateFileURL;
	}

	public void setMappingURL(String mappingURL) {
		this.mappingURL = mappingURL;
	}

	public void setMapping(String mapping) {
		this.mapping = mapping;
	}

	public void setSheet(String sheet) {
		this.sheet = sheet;
	}

	public void setExistingSheetsActions(SpreadsheetExistingSheetsActions existingSheetsActions) {
		this.existingSheetsActions = existingSheetsActions;
	}

	public void setMkDirs(boolean mkDirs) {
		this.mkDirs = mkDirs;
	}

	public void setRecordSkip(int recordSkip) {
		this.recordSkip = recordSkip;
	}

	public void setRecordCount(int recordCount) {
		this.recordCount = recordCount;
	}

	public void setRecordsPerFile(int recordsPerFile) {
		this.recordsPerFile = recordsPerFile;
	}

	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}

	public void setPartitionFileTagType(String partitionFileTagType) {
		this.partitionFileTagType = PartitionFileTagType.valueOfIgnoreCase(partitionFileTagType);
	}

	public void setPartitionOutFields(String partitionOutFields) {
		this.partitionOutFields = partitionOutFields;
	}

	public void setPartitionUnassignedFileName(String partitionUnassignedFileName) {
		this.partitionUnassignedFileName = partitionUnassignedFileName;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 1, 1) || !checkOutputPorts(status, 0, 1)) {
			return status;
		}
		
		Map<String, Character> fieldsOfUnsupportedType = checkInputDataFieldsTypes(getInputPort(0).getMetadata());
		if (!fieldsOfUnsupportedType.isEmpty()) {
			StringBuilder errorMessage = new StringBuilder("Input port metadata contain the following unsupported types: ");
			for (String fieldName : fieldsOfUnsupportedType.keySet()) {
				errorMessage.append("\n" + fieldName + " - " + DataFieldMetadata.type2Str(fieldsOfUnsupportedType.get(fieldName)));
			}
			status.add(new ConfigurationProblem(errorMessage.toString(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
		}
		
		Map<String, DataFieldFormatType> fieldsWithUnsupportedFormats = checkInputDataFieldsFormats(getInputPort(0).getMetadata());
		if (!fieldsWithUnsupportedFormats.isEmpty()) {
			StringBuilder errorMessage = new StringBuilder("Input port metadata contain the following fields with unsupported formatting strings (try to prepend \"" + DataFieldFormatType.EXCEL.getFormatPrefixWithDelimiter() + "\" to threir formatting strings):");
			for (String fieldName : fieldsWithUnsupportedFormats.keySet()) {
				errorMessage.append("\n" + fieldName + " - " + fieldsWithUnsupportedFormats.get(fieldName).getLongName());
			}
			status.add(new ConfigurationProblem(errorMessage.toString(), ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
		}

		try {
			FileUtils.canWrite(getGraph() != null ? getGraph().getRuntimeContext().getContextURL() : null, fileURL, mkDirs);
			XLSMapping mapping = prepareMapping();
			if (mapping != null) {
				if (mapping.getOrientation() != XLSMapping.HEADER_ON_TOP && writeMode.isStreamed()) {
					status.add(new ConfigurationProblem("Horizontal orientation is not supported with streaming!",
							ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
				}
				mapping.checkConfig(status);
			}
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this,
					ConfigurationStatus.Priority.NORMAL);

			if (!StringUtils.isEmpty(e.getAttributeName())) {
				problem.setAttributeName(e.getAttributeName());
			}

			status.add(problem);
		}
		
		if (writeMode.isStreamed()) {
			if (templateFileURL != null) { // requires implementing workbook/sheet/row copying
				status.add(new ConfigurationProblem("Write using template is not supported with streaming mode!",
						ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
			}
		}
		
		if (templateFileURL!=null && fileURL!=null) {
			if (resolveFormat(formatterType, fileURL)!=resolveTemplateFormat(formatterType, templateFileURL)) {
				status.add(new ConfigurationProblem("Formats of template and an output file must match!",
						ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
			}
		}
		
		return status;
	}
	
	private Map<String, Character> checkInputDataFieldsTypes(DataRecordMetadata metadata) {
		HashMap<String, Character> result = new HashMap<String, Character>();
		char fieldType;
		for (DataFieldMetadata field : metadata.getFields()) {
			fieldType = field.getType();
			if (!supportedTypes.contains(fieldType)) {
				result.put(field.getName(), fieldType);
			}
		}
		
		return result;
	}

	private Map<String, DataFieldFormatType> checkInputDataFieldsFormats(DataRecordMetadata metadata) {
		HashMap<String, DataFieldFormatType> result = new HashMap<String, DataFieldFormatType>();
		for (DataFieldMetadata field : metadata.getFields()) {
			if (field.hasFormat()) {
				DataFieldFormatType formatType = field.getFormatType();
				if (formatType!=DataFieldFormatType.EXCEL) {
					//unsupported data field format type
					result.put(field.getName(), formatType);
				}
			}
		}
		
		return result;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}
		super.init();

		formatterProvider = new SpreadsheetFormatterProvider();
		formatterProvider.setAttitude(writeMode.isStreamed() ? SpreadsheetAttitude.STREAM : SpreadsheetAttitude.IN_MEMORY);
		formatterProvider.setFormatterType(resolveFormat(formatterType, fileURL));
		formatterProvider.setMapping(prepareMapping());
		formatterProvider.setSheet(sheet);
		formatterProvider.setAppend(writeMode.isAppend());
		formatterProvider.setInsert(writeMode.isInsert());
		formatterProvider.setCreateFile(writeMode.isCreatingNewFile());
		formatterProvider.setRemoveSheets(existingSheetsActions.isRemovingAllSheets());
		formatterProvider.setRemoveRows(existingSheetsActions.isRemovingAllRows());
		if (templateFileURL!=null) {
			formatterProvider.setTemplateFile(getGraph().getRuntimeContext().getContextURL(), templateFileURL);
		}
		prepareWriter();
	}

	public static SpreadsheetFormat resolveFormat(SpreadsheetFormat format, String fileURL) {
		if ((format == SpreadsheetFormat.AUTO && fileURL.matches(SpreadsheetFormatter.XLSX_FILE_PATTERN)) || format == SpreadsheetFormat.XLSX) {
			return SpreadsheetFormat.XLSX;
		} else {
			return SpreadsheetFormat.XLS;
		}
	}
	
	public static SpreadsheetFormat resolveTemplateFormat(SpreadsheetFormat format, String templateFileURL) {
		if ((format == SpreadsheetFormat.AUTO && templateFileURL.matches(SpreadsheetFormatter.XLTX_FILE_PATTERN)) || format == SpreadsheetFormat.XLSX) {
			return SpreadsheetFormat.XLSX;
		} else {
			return SpreadsheetFormat.XLS;
		}
	}

	private XLSMapping prepareMapping() throws ComponentNotReadyException {
		DataRecordMetadata metadata = getInputPort(READ_FROM_PORT).getMetadata();

		XLSMapping parsedMapping = null;
		if (mappingURL != null) {
			TransformationGraph graph = getGraph();
			try {
				InputStream stream = FileUtils.getInputStream(graph.getRuntimeContext().getContextURL(), mappingURL);
				parsedMapping = XLSMapping.parse(stream, metadata);
			} catch (IOException e) {
				LOGGER.error("cannot instantiate node from XML", e);
				throw new ComponentNotReadyException(e.getMessage(), e);
			}
		} else if (mapping != null) {
			parsedMapping = XLSMapping.parse(mapping, metadata);
		}

		return parsedMapping;
	}

	private void prepareWriter() throws ComponentNotReadyException {
		if (fileURL != null) {
			writer = new MultiFileWriter(formatterProvider, getGraph().getRuntimeContext().getContextURL(), fileURL);
		} else {
			WritableByteChannelIterator channelIterator = new WritableByteChannelIterator(new SystemOutByteChannel());
			writer = new MultiFileWriter(formatterProvider, channelIterator);
		}
		writer.setLogger(LOGGER);
		writer.setDictionary(getGraph().getDictionary());
		writer.setOutputPort(getOutputPort(OUTPUT_PORT));
		writer.setMkDir(mkDirs);
		writer.setAppendData(true);//in order to allow input stream opening
		writer.setUseChannel(false);

		writer.setSkip(recordSkip);
		writer.setNumRecords(recordCount);
		writer.setRecordsPerFile(recordsPerFile);

		if (partition != null) {
			lookupTable = getGraph().getLookupTable(partition);
			if (lookupTable == null) {
				throw new ComponentNotReadyException("Lookup table \"" + partition + "\" not found.");
			}
		} else {
			lookupTable = null;
		}
		
		if (partitionKey != null) {
			writer.setLookupTable(lookupTable);
			writer.setPartitionKeyNames(partitionKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			writer.setPartitionFileTag(partitionFileTagType);
			writer.setPartitionUnassignedFileName(partitionUnassignedFileName);

			if (partitionOutFields != null) {
				writer.setPartitionOutFields(partitionOutFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			}
		}
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		if (firstRun()) { // a phase-dependent part of initialization
			writer.init(getInputPort(READ_FROM_PORT).getMetadata());
		} else {
			writer.reset();
		}
	}

	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();

		while (record != null && runIt) {
			record = inPort.readRecord(record);
			if (record != null) {
				writer.write(record);
			}
		}

		writer.finish();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();

		try {
			writer.close();
		} catch (IOException e) {
			throw new ComponentNotReadyException(COMPONENT_TYPE + ": " + e.getMessage(), e);
		}
	}

	@Override
	public synchronized void free() {
		super.free();
		if (writer != null) {
			try {
				writer.close();
			} catch (Throwable t) {
				LOGGER.warn("Resource releasing failed for '" + getId() + "'. " + t.getMessage(), t);
			}
		}
	}
}
