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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;

import javax.xml.stream.XMLStreamException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.xml.writer.DataIterator;
import org.jetel.component.xml.writer.MappingCompiler;
import org.jetel.component.xml.writer.MappingError;
import org.jetel.component.xml.writer.MappingTagger;
import org.jetel.component.xml.writer.MappingValidator;
import org.jetel.component.xml.writer.PortData;
import org.jetel.component.xml.writer.XmlFormatterProvider;
import org.jetel.component.xml.writer.mapping.MappingProperty;
import org.jetel.component.xml.writer.mapping.AbstractElement;
import org.jetel.component.xml.writer.mapping.XmlMapping;
import org.jetel.component.xml.writer.model.WritableMapping;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.provider.FormatterProvider;
import org.jetel.data.lookup.LookupTable;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * @author lkrejci
 * @created Dec 03, 2010
 */
public class ExtXmlWriter extends Node {

	public final static String COMPONENT_TYPE = "EXT_XML_WRITER";
	
	public static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
	public static final String XML_CHARSET_ATTRIBUTE = "charset";
	public static final String XML_MK_DIRS_ATTRIBUTE = "makeDirs";
	public static final String XML_MAPPING_ATTRIBUTE = "mapping";
	public static final String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
	public static final String XML_OMIT_NEW_LINES = "omitNewLines";
	public static final String XML_TEMPORARY_DIR = "tmpDir";
	public static final String XML_CACHE_SIZE = "cacheSize";
	public static final String XML_SORTED_INPUT_ATTRIBUTE = "sortedInput";
	public static final String XML_SORTKEYS_ATTRIBUTE = "sortKeys";
	public static final String XML_RECORDS_PER_FILE = "recordsPerFile";
	public static final String XML_RECORD_COUNT_ATTRIBUTE = "recordCount";
	public static final String XML_PARTITIONKEY_ATTRIBUTE = "partitionKey";
	public static final String XML_PARTITION_ATTRIBUTE = "partition";
	public static final String XML_PARTITION_OUTFIELDS_ATTRIBUTE = "partitionOutFields";
	public static final String XML_PARTITION_FILETAG_ATTRIBUTE = "partitionFileTag";
	public static final String XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE = "partitionUnassignedFileName";
	
	private static final long DEFAULT_CACHE_SIZE = 1024 * 1024;
	
	private static final int MAX_ERRORS_OR_WARNINGS = 20;

	private String fileUrl;
	private String charset;
	private boolean mkDir;
	private MultiFileWriter writer;

	private String mappingString;
	private String mappingURL;
	private boolean omitNewLines;
	private String tmpDir;
	private long cacheSize = DEFAULT_CACHE_SIZE;
	
	private boolean sortedInput;
	private String sortHintsString;
	private WritableMapping engineMapping;
	private Map<Integer, PortData> portDataMap;
	
	private int recordsPerFile;
	private int recordsCount;
	private String attrPartitionKey;
	private String partition;
	private String partitionOutFields;
	private PartitionFileTagType partitionFileTag = PartitionFileTagType.NUMBER_FILE_TAG;
	private String partitionUnassignedFileName;
	private LookupTable lookupTable;
	
	private static Log logger = LogFactory.getLog(ExtXmlWriter.class);
	

	public ExtXmlWriter(String id) {
		super(id);
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		if (checkPorts(status)) {
			return status;
		}
		try {
			validateMapping(status);
		} catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), Severity.ERROR,
                    this, ConfigurationStatus.Priority.NORMAL);
            if (!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
		}
		
		if (sortedInput && sortHintsString == null) {
			ConfigurationProblem problem = new ConfigurationProblem("Sort keys is not set",
					Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
			problem.setAttributeName("sortKeys");
			status.add(problem);
		}
		
		return status;
	}
	
	private void validateMapping(ConfigurationStatus status) throws ComponentNotReadyException  {
		XmlMapping mapping = initMapping();
		Map<Integer, DataRecordMetadata> connectedPorts = prepareConnectedData();
		MappingValidator validator = new MappingValidator(connectedPorts);
		validator.setMapping(mapping);
		validator.setMaxErrors(MAX_ERRORS_OR_WARNINGS);
		validator.setMaxWarnings(MAX_ERRORS_OR_WARNINGS);
		validator.validate();
		
		if (!validator.getErrorsMap().isEmpty()) {
			List<MappingError> errors = new ArrayList<MappingError>(MAX_ERRORS_OR_WARNINGS);
			List<MappingError> warnings = new ArrayList<MappingError>(MAX_ERRORS_OR_WARNINGS);
			
			for (Entry<AbstractElement, Map<MappingProperty, SortedSet<MappingError>>> entry : validator.getErrorsMap().entrySet()) {
				for (Entry<MappingProperty, SortedSet<MappingError>> elementEntry : entry.getValue().entrySet()) {
					for (MappingError error : elementEntry.getValue()) {
						if (error.getSeverity() == Severity.ERROR) {
							errors.add(error);
						} else {
							warnings.add(error);
						}
					}
				}
			}
			
			String atLeast = validator.isValidationComplete() ? "" : "At least ";
			
			for (MappingError error : errors) {
				status.add(new ConfigurationProblem("Invalid mapping (" + error.getMessage() + ")", Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
			}
			if (errors.size() < validator.getErrorsCount()) {
				int n = validator.getErrorsCount() - errors.size();
				status.add(new ConfigurationProblem(atLeast + n + " more error" + (n > 1 ? "s" : "") + " in the mapping",
						Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
			}
			
			int count = 0;
			for (MappingError warning : warnings) {
				if (errors.size() + count >= MAX_ERRORS_OR_WARNINGS) break;
				status.add(new ConfigurationProblem("Invalid mapping (" + warning.getMessage() + ")", Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
				count++;
			}
			if (count < validator.getWarningsCount()) {
				int n = validator.getWarningsCount() - count;
				status.add(new ConfigurationProblem(atLeast + n + (count > 0 ? " more" : " ") + " warning" + (n > 1 ? "s" : "") + " in the mapping",
						Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
			}
			
		} else {
			MappingTagger tagger = new MappingTagger(connectedPorts, sortedInput ? sortHintsString : null);
			
			tagger.setMapping(mapping);
			boolean partition = attrPartitionKey != null || recordsPerFile > 0 || recordsCount > 0; 
			tagger.setResolvePartition(partition);
			tagger.tag();
			
			Set<Integer> usedPorts = tagger.getUsedPorts();
			if (usedPorts.size() < inPorts.size()) {
				StringBuilder sb = new StringBuilder();
				int counter = 0;
				for (Integer portIndex : inPorts.keySet()) {
					if (!usedPorts.contains(portIndex)) {
						sb.append(portIndex);
						sb.append(", ");
						counter++;
					}
				}
				if (usedPorts.size() == 0) {
					status.add(new ConfigurationProblem("None of the connected input ports is used in mapping.",
							ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
				} else if (counter == 1) {
					status.add(new ConfigurationProblem("Input port " + sb.substring(0, sb.length() - 2) + " is connected, but isn't used in mapping.",
							ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
				} else if (counter > 1) {
					status.add(new ConfigurationProblem("Input ports " + sb.substring(0, sb.length() - 2) + " are connected, but aren't used in mapping.",
							ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
				}
			}
		}
	}
	
	private Map<Integer, DataRecordMetadata> prepareConnectedData() {
		Map<Integer, DataRecordMetadata> connectedData = new HashMap<Integer, DataRecordMetadata>();
		for (Entry<Integer, InputPort> entry : inPorts.entrySet()) {
			connectedData.put(entry.getKey(), entry.getValue().getMetadata());
		}
		return connectedData;
	}
	
	private boolean checkPorts(ConfigurationStatus status) {
        return !checkInputPorts(status, 1, Integer.MAX_VALUE, false) || !checkOutputPorts(status, 0, 1);
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}
		super.init();
		if (charset == null) {
			charset = Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER;
		}
		compileMapping(initMapping());
		initWriter();
	}
	
	private XmlMapping initMapping() throws ComponentNotReadyException {
		
		InputStream stream;
		if (this.mappingURL != null) {
			TransformationGraph graph = getGraph();
			try {
				stream = FileUtils.getInputStream(graph != null ? graph.getRuntimeContext().getContextURL() : null, mappingURL);
			} catch (IOException e) {
				logger.error("cannot instantiate node from XML", e);
				throw new ComponentNotReadyException(e.getMessage(), e);
			}
		} else {
			stream = new ByteArrayInputStream(this.mappingString.getBytes());
		}
		XmlMapping mapping;
		try {
			mapping = XmlMapping.fromXml(stream);
		} catch (XMLStreamException e) {
			throw new ComponentNotReadyException(e);
		}
		return mapping;		
	}
	
	private void compileMapping(XmlMapping mapping) throws ComponentNotReadyException {
		MappingValidator validator = new MappingValidator(prepareConnectedData());
		validator.setMapping(mapping);
		validator.validate();
		
		if (validator.containsErrors()) {
			throw new ComponentNotReadyException("There are errors in mapping");
		}
		
		MappingCompiler compiler = new MappingCompiler(prepareConnectedData(), sortedInput ? sortHintsString : null);
		compiler.setGraph(getGraph());
		compiler.setComponentId(getType());
		compiler.setLogger(logger);
		compiler.setMapping(mapping);
		
		boolean partition = attrPartitionKey != null || recordsPerFile > 0 || recordsCount > 0; 
		this.engineMapping = compiler.compile(inPorts, tmpDir, cacheSize, partition);
		
		portDataMap = compiler.getPortDataMap();		
		for (PortData portData : portDataMap.values()) {
			portData.init();
		}
	}
	
	private void initWriter() throws ComponentNotReadyException {
		TransformationGraph graph = getGraph();
		FormatterProvider provider = new XmlFormatterProvider(engineMapping, omitNewLines, charset);
		writer = new MultiFileWriter(provider, graph != null ? graph.getRuntimeContext().getContextURL() : null, this.fileUrl);
		writer.setLogger(logger);
		writer.setAppendData(false);
		writer.setUseChannel(true);
		writer.setOutputPort(getOutputPort(OUTPUT_PORT));
		writer.setDictionary(graph.getDictionary());
		writer.setMkDir(mkDir);
		writer.setCharset(charset);
		
		writer.setRecordsPerFile(recordsPerFile);
		writer.setNumRecords(recordsCount);
		
		if (attrPartitionKey != null) {
			initLookupTable(partition);
			writer.setLookupTable(lookupTable);
			writer.setPartitionKeyNames(attrPartitionKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			writer.setPartitionFileTag(partitionFileTag);
			if (partitionOutFields != null) {
				writer.setPartitionOutFields(partitionOutFields.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
			}
			writer.setPartitionUnassignedFileName(partitionUnassignedFileName);
		}
	}
	
	private void initLookupTable(String table) throws ComponentNotReadyException {
		if (table == null) {
			return;
		}
		lookupTable = getGraph().getLookupTable(table);
		if (lookupTable == null) {
			throw new ComponentNotReadyException("Lookup table \"" + table + "\" not found.");
		}
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		for (PortData portData : portDataMap.values()) {
			portData.preExecute();
		}
	}

	@Override
	public Result execute() throws Exception {
		loadDataToCache();
		
		// preExecute code must be placed into execute, as header might contain data from records
		if (firstRun()) {
			if (engineMapping.getPartitionElement() != null) {
				writer.init(inPorts.get(engineMapping.getPartitionElement().getPortIndex()).getMetadata());
			} else {
				//Dummy, but valid metadata
				writer.init(inPorts.firstEntry().getValue().getMetadata());
			}
		} else {
			writer.reset();
		}
		
		if (engineMapping.getPartitionElement() != null) {
			DataIterator iterator = engineMapping.getPartitionElement().getPortData().iterator(null, null, null, null);
			engineMapping.getPartitionElement().setIterator(iterator);
			while (iterator.hasNext()) {
				writer.write(iterator.next());
			}
			// postExecute code must be placed into execute, as footer might contain data from records
			writer.finish();
		} else {
			writer.write(null);
		}

		readRemainingData();
		
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	private void loadDataToCache() {
		List<InputReader> readers = new ArrayList<InputReader>();
		for (PortData portData : portDataMap.values()) {
			if (portData.readInputPort()) {
				readers.add(new InputReader(portData));
			}
		}
		
		manageReaders(readers);
	}
	
	private void readRemainingData() {
		List<InputReader> readers = new ArrayList<InputReader>();
		for (InputPort inPort : inPorts.values()) {
			if (!inPort.isEOF()) {
				readers.add(new InputReader(inPort));
			}
		}
		
		manageReaders(readers);
	}
	
	private void manageReaders(List<InputReader> readers) {
		for (InputReader reader : readers) {
			reader.start();
		}
		boolean killIt = false;
		for (Iterator<InputReader> iterator = readers.iterator(); iterator.hasNext();) {
			InputReader inputReader = iterator.next();
			while (inputReader.getState() != Thread.State.TERMINATED) {
				if (killIt) {
					inputReader.interrupt();
					break;
				}
				killIt = !runIt;
				try {
					inputReader.join(1000);
				} catch (InterruptedException e) {
					logger.warn(getId() + "thread interrupted, it will interrupt child threads", e);
					killIt = true;
				}
			}
		}
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		for (PortData portData : portDataMap.values()) {
			portData.postExecute();
		}
		try {
			writer.close();
		} catch (IOException e) {
			throw new ComponentNotReadyException(COMPONENT_TYPE + ": " + e.getMessage(), e);
		}
	}

	@Override
	public synchronized void free() {
		super.free();
		if (portDataMap != null) {
			for (PortData portData : portDataMap.values()) {
				portData.free();
			}
		}
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ExtXmlWriter writer = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		try {
			writer = new ExtXmlWriter(xattribs.getString(XML_ID_ATTRIBUTE));
			
			writer.setFileUrl(xattribs.getString(XML_FILE_URL_ATTRIBUTE));
			writer.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
			if (xattribs.exists(XML_MK_DIRS_ATTRIBUTE)) {
				writer.setMkDir(xattribs.getBoolean(XML_MK_DIRS_ATTRIBUTE));
			}
			String mappingURL = xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF);
			String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE, null);
			if (mappingURL != null)
				writer.setMappingURL(mappingURL);
			else if (mapping != null)
				writer.setMappingString(mapping);
			else {
				// throw configuration exception
				xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF);
			}
			writer.setOmitNewLines(xattribs.getBoolean(XML_OMIT_NEW_LINES, false));            
            if (xattribs.exists(XML_TEMPORARY_DIR)){
                writer.setTmpDir(xattribs.getStringEx(XML_TEMPORARY_DIR,RefResFlag.SPEC_CHARACTERS_OFF));
            }
            if (xattribs.exists(XML_CACHE_SIZE)){
                writer.setCacheSize(parseMemory(xattribs.getString(XML_CACHE_SIZE)));
            }
            if (xattribs.exists(XML_SORTED_INPUT_ATTRIBUTE)) {
            	writer.setSortedInput(xattribs.getBoolean(XML_SORTED_INPUT_ATTRIBUTE, false));
            }
            if (xattribs.exists(XML_SORTKEYS_ATTRIBUTE)) {
            	writer.setSortHintsString(xattribs.getString(XML_SORTKEYS_ATTRIBUTE));
            }
			if(xattribs.exists(XML_RECORDS_PER_FILE)) {
                writer.setRecordsPerFile(xattribs.getInteger(XML_RECORDS_PER_FILE));
            }
			if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)){
				writer.setRecordsCount(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
			}
			if(xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE)) {
				writer.setPartitionKey(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE));
            }
			if(xattribs.exists(XML_PARTITION_ATTRIBUTE)) {
				writer.setPartition(xattribs.getString(XML_PARTITION_ATTRIBUTE));
            }
			if(xattribs.exists(XML_PARTITION_FILETAG_ATTRIBUTE)) {
				writer.setPartitionFileTag(xattribs.getString(XML_PARTITION_FILETAG_ATTRIBUTE));
            }
			if(xattribs.exists(XML_PARTITION_OUTFIELDS_ATTRIBUTE)) {
				writer.setPartitionOutFields(xattribs.getString(XML_PARTITION_OUTFIELDS_ATTRIBUTE));
            }
			if(xattribs.exists(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE)) {
				writer.setPartitionUnassignedFileName(xattribs.getString(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE));
            }
		} catch (AttributeNotFoundException ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}

		return writer;
	}

	public void setFileUrl(String fileUrl) {
		this.fileUrl = fileUrl;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setMkDir(boolean mkDir) {
		this.mkDir = mkDir;
	}

	public void setMappingString(String mappingString) {
		this.mappingString = mappingString;
	}

	public void setMappingURL(String mappingURL) {
		this.mappingURL = mappingURL;
	}

	public void setOmitNewLines(boolean omitNewLines) {
		this.omitNewLines = omitNewLines;
	}

	public void setTmpDir(String tmpDir) {
		this.tmpDir = tmpDir;
	}
	
	public void setCacheSize(long cacheSize) {
		this.cacheSize = cacheSize;
	}
	
	public void setSortedInput(boolean sortedInput) {
		this.sortedInput = sortedInput;
	}

	public void setSortHintsString(String sortHintsString) {
		this.sortHintsString = sortHintsString;
	}

	public void setRecordsPerFile(int recordsPerFile) {
		this.recordsPerFile = recordsPerFile;
	}

	public void setRecordsCount(int recordsCount) {
		this.recordsCount = recordsCount;
	}

	public void setPartitionKey(String attrPartitionKey) {
		this.attrPartitionKey = attrPartitionKey;
	}

	public void setPartition(String partition) {
		this.partition = partition;
	}

	public void setPartitionOutFields(String partitionOutFields) {
		this.partitionOutFields = partitionOutFields;
	}

	public void setPartitionFileTag(String partitionFileTagType) {
		this.partitionFileTag = PartitionFileTagType.valueOfIgnoreCase(partitionFileTagType);
	}

	public void setPartitionUnassignedFileName(String partitionUnassignedFileName) {
		this.partitionUnassignedFileName = partitionUnassignedFileName;
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}
	//FastSort method:
	/**
	 * Parses a memory string in following formats and returns size in bytes
	 * Examples: "32m", "32mb", "2 g", "2gb", "128k", "128kb" "8192"
	 * @param s
	 * @return value in bytes
	 */
	public static long parseMemory(String s) { 
		if (s == null) {
			return -1;
		}
		s = s.trim().toUpperCase();
		try {
			if (s.endsWith("K")) {
				return Long.valueOf(s.substring(0, s.length()-1).trim()).longValue() * 1024;
			} else if (s.endsWith("KB")) {
				return Long.valueOf(s.substring(0, s.length()-2).trim()).longValue() * 1024;
			} else if (s.endsWith("M")) {
				return Long.valueOf(s.substring(0, s.length()-1).trim()).longValue() * 1024 * 1024;
			} else if (s.endsWith("MB")) {
				return Long.valueOf(s.substring(0, s.length()-2).trim()).longValue() * 1024 * 1024;
			} else if (s.endsWith("G")) {
				return Long.valueOf(s.substring(0, s.length()-1).trim()).longValue() * 1024 * 1024 * 1024;
			} else if (s.endsWith("GB")) {
				return Long.valueOf(s.substring(0, s.length()-2).trim()).longValue() * 1024 * 1024 * 1024;
			}
		} catch (NumberFormatException e) {
		}
		return -1;
	}

	protected class InputReader extends Thread {

		private final InputPort inPort;
		private final PortData portData;

		public InputReader(PortData portData) {
			super(Thread.currentThread().getName() + ".InputThread#" + portData.getInPort().getInputPortNumber());
			this.portData = portData;
			this.inPort = portData.getInPort();
		}
		
		public InputReader(InputPort inPort) {
			super(Thread.currentThread().getName() + ".InputThread#" + inPort.getInputPortNumber());
			this.portData = null;
			this.inPort = inPort;
		}

		public void run() {
			while (runIt) {
				try {
					DataRecord record = new DataRecord(inPort.getMetadata());
					record.init();
					if (inPort.readRecord(record) == null) {
						return;
					}
					if (portData != null) {
						portData.put(record);
					}

				} catch (InterruptedException e) {
					logger.error(getId() + ": thread forcibly aborted", e);
					return;
				} catch (Exception e) {
					logger.error(getId() + ": thread failed", e);
					return;
				}
			}
		}
	}
}
