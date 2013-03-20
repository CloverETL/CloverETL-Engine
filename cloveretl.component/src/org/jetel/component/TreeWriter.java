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
import java.io.File;
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
import org.jetel.component.tree.writer.BaseTreeFormatterProvider;
import org.jetel.component.tree.writer.model.design.AbstractNode;
import org.jetel.component.tree.writer.model.design.MappingProperty;
import org.jetel.component.tree.writer.model.design.TreeWriterMapping;
import org.jetel.component.tree.writer.model.runtime.PortBinding;
import org.jetel.component.tree.writer.model.runtime.WritableMapping;
import org.jetel.component.tree.writer.portdata.CacheRecordManager;
import org.jetel.component.tree.writer.portdata.DataIterator;
import org.jetel.component.tree.writer.portdata.PortData;
import org.jetel.component.tree.writer.util.AbstractMappingValidator;
import org.jetel.component.tree.writer.util.MappingCompiler;
import org.jetel.component.tree.writer.util.MappingError;
import org.jetel.component.tree.writer.util.MappingTagger;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.lookup.LookupTable;
import org.jetel.enums.PartitionFileTagType;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.TempFileCreationException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MultiFileWriter;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 1.11.2011
 */
public abstract class TreeWriter extends Node {

	private static Log LOGGER = LogFactory.getLog(TreeWriter.class);
	private static final int OUTPUT_PORT = 0;

	public static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
	public static final String XML_CHARSET_ATTRIBUTE = "charset";
	public static final String XML_MAPPING_ATTRIBUTE = "mapping";
	public static final String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
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

	public static Node readCommonAttributes(TreeWriter writer, ComponentXMLAttributes xattribs)
			throws XMLConfigurationException, AttributeNotFoundException {
		writer.setFileUrl(xattribs.getString(XML_FILE_URL_ATTRIBUTE));
		writer.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE, null));
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
		if (xattribs.exists(XML_CACHE_SIZE)) {
			writer.setCacheSize(StringUtils.parseMemory(xattribs.getString(XML_CACHE_SIZE)));
		}
		if (xattribs.exists(XML_SORTED_INPUT_ATTRIBUTE)) {
			writer.setSortedInput(xattribs.getBoolean(XML_SORTED_INPUT_ATTRIBUTE, false));
		}
		if (xattribs.exists(XML_SORTKEYS_ATTRIBUTE)) {
			writer.setSortHintsString(xattribs.getString(XML_SORTKEYS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_RECORDS_PER_FILE)) {
			writer.setRecordsPerFile(xattribs.getInteger(XML_RECORDS_PER_FILE));
		}
		if (xattribs.exists(XML_RECORD_COUNT_ATTRIBUTE)) {
			writer.setRecordsCount(Integer.parseInt(xattribs.getString(XML_RECORD_COUNT_ATTRIBUTE)));
		}
		if (xattribs.exists(XML_PARTITIONKEY_ATTRIBUTE)) {
			writer.setPartitionKey(xattribs.getString(XML_PARTITIONKEY_ATTRIBUTE));
		}
		if (xattribs.exists(XML_PARTITION_ATTRIBUTE)) {
			writer.setPartition(xattribs.getString(XML_PARTITION_ATTRIBUTE));
		}
		if (xattribs.exists(XML_PARTITION_FILETAG_ATTRIBUTE)) {
			writer.setPartitionFileTag(xattribs.getString(XML_PARTITION_FILETAG_ATTRIBUTE));
		}
		if (xattribs.exists(XML_PARTITION_OUTFIELDS_ATTRIBUTE)) {
			writer.setPartitionOutFields(xattribs.getString(XML_PARTITION_OUTFIELDS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE)) {
			writer.setPartitionUnassignedFileName(xattribs.getString(XML_PARTITION_UNASSIGNED_FILE_NAME_ATTRIBUTE));
		}

		return writer;
	}

	private String fileURL;
	protected String charset;
	protected MultiFileWriter writer;

	private String mappingString;
	private String mappingURL;
	private File tempDirectory;
	private long cacheSize = DEFAULT_CACHE_SIZE;

	private boolean sortedInput;
	private String sortHintsString;
	protected TreeWriterMapping designMapping;
	private WritableMapping engineMapping;
	private Map<Integer, PortData> portDataMap;
	private CacheRecordManager sharedCache;

	protected int recordsPerFile;
	protected int recordsCount;
	private String attrPartitionKey;
	private String partition;
	private String partitionOutFields;
	private PartitionFileTagType partitionFileTag = PartitionFileTagType.NUMBER_FILE_TAG;
	private String partitionUnassignedFileName;
	private LookupTable lookupTable;
	
	private Throwable throwableException = null;
	
	public TreeWriter(String id) {
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
			ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.exceptionChainToMessage(e), Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
			if (!StringUtils.isEmpty(e.getAttributeName())) {
				problem.setAttributeName(e.getAttributeName());
			}
			status.add(problem);
		}

		if (sortedInput && sortHintsString == null) {
			status.add(new ConfigurationProblem("Sort keys is not set", Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL, XML_SORTKEYS_ATTRIBUTE));
		}
		if (cacheSize > Runtime.getRuntime().maxMemory()) {
			status.add(new ConfigurationProblem("Cache size has a value of " + cacheSize + " but the JVM" + " is only configured for " + Runtime.getRuntime().maxMemory(), Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL, XML_CACHE_SIZE));
		}

		return status;
	}

	private boolean checkPorts(ConfigurationStatus status) {
		return !checkInputPorts(status, 1, Integer.MAX_VALUE, false) || !checkOutputPorts(status, 0, 1);
	}

	private void validateMapping(ConfigurationStatus status) throws ComponentNotReadyException {
		TreeWriterMapping mapping = initMapping();
		Map<Integer, DataRecordMetadata> connectedPorts = prepareConnectedData();
		AbstractMappingValidator validator = createValidator(connectedPorts);
		if (validator == null) { // No validator specified
			return;
		}
		
		validator.setMapping(mapping);
		validator.setMaxErrors(MAX_ERRORS_OR_WARNINGS);
		validator.setMaxWarnings(MAX_ERRORS_OR_WARNINGS);
		validator.validate();

		if (!validator.getErrorsMap().isEmpty()) {
			List<MappingError> errors = new ArrayList<MappingError>(MAX_ERRORS_OR_WARNINGS);
			List<MappingError> warnings = new ArrayList<MappingError>(MAX_ERRORS_OR_WARNINGS);

			for (Entry<AbstractNode, Map<MappingProperty, SortedSet<MappingError>>> entry : validator.getErrorsMap().entrySet()) {
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
				status.add(new ConfigurationProblem(atLeast + n + " more error" + (n > 1 ? "s" : "") + " in the mapping", Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
			}

			int count = 0;
			for (MappingError warning : warnings) {
				if (errors.size() + count >= MAX_ERRORS_OR_WARNINGS)
					break;
				status.add(new ConfigurationProblem("Invalid mapping (" + warning.getMessage() + ")", Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
				count++;
			}
			if (count < validator.getWarningsCount()) {
				int n = validator.getWarningsCount() - count;
				status.add(new ConfigurationProblem(atLeast + n + (count > 0 ? " more" : " ") + " warning" + (n > 1 ? "s" : "") + " in the mapping", Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
			}

		} else {
			MappingTagger tagger = new MappingTagger(connectedPorts, sortedInput ? sortHintsString : null, recordsCount == 1);

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
					status.add(new ConfigurationProblem("None of the connected input ports is used in mapping.", ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
				} else if (counter == 1) {
					status.add(new ConfigurationProblem("Input port " + sb.substring(0, sb.length() - 2) + " is connected, but isn't used in mapping.", ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
				} else if (counter > 1) {
					status.add(new ConfigurationProblem("Input ports " + sb.substring(0, sb.length() - 2) + " are connected, but aren't used in mapping.", ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL));
				}
			}
		}
	}

	protected abstract AbstractMappingValidator createValidator(Map<Integer, DataRecordMetadata> connectedPorts);
	
	private TreeWriterMapping initMapping() throws ComponentNotReadyException {

		InputStream stream;
		if (this.mappingURL != null) {
			TransformationGraph graph = getGraph();
			try {
				stream = FileUtils.getInputStream(graph != null ? graph.getRuntimeContext().getContextURL() : null, mappingURL);
			} catch (IOException e) {
				throw new ComponentNotReadyException("cannot instantiate node from XML", e);
			}
		} else {
			stream = new ByteArrayInputStream(this.mappingString.getBytes());
		}
		TreeWriterMapping mapping;
		try {
			mapping = TreeWriterMapping.fromXml(stream);
		} catch (XMLStreamException e) {
			throw new ComponentNotReadyException(e);
		}
		return mapping;
	}

	private Map<Integer, DataRecordMetadata> prepareConnectedData() {
		Map<Integer, DataRecordMetadata> connectedData = new HashMap<Integer, DataRecordMetadata>();
		for (Entry<Integer, InputPort> entry : inPorts.entrySet()) {
			connectedData.put(entry.getKey(), entry.getValue().getMetadata());
		}
		return connectedData;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}
		super.init();
		if (charset == null) {
			charset = getDefaultCharset();
		}
		designMapping = initMapping();
		compileMapping(designMapping);

		configureWriter();
	}

	protected String getDefaultCharset() {
		return Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER; 
	}
	
	private void compileMapping(TreeWriterMapping mapping) throws ComponentNotReadyException {
		AbstractMappingValidator validator = createValidator(prepareConnectedData());
		if (validator != null) {
			validator.setMapping(mapping);
			validator.validate();

			if (validator.containsErrors()) {
				throw new ComponentNotReadyException("There are errors in mapping");
			}
		}

		MappingCompiler compiler = new MappingCompiler(prepareConnectedData(), sortedInput ? sortHintsString : null, recordsCount == 1);
		compiler.setGraph(getGraph());
		compiler.setComponentId(getType());
		compiler.setLogger(LOGGER);
		compiler.setMapping(mapping);

		boolean partition = attrPartitionKey != null || recordsPerFile > 0 || recordsCount > 0;

		engineMapping = compiler.compile(inPorts, partition);

		portDataMap = compiler.getPortDataMap();
		for (PortData portData : portDataMap.values()) {
			portData.init();
		}
	}

	protected void configureWriter() throws ComponentNotReadyException {
		int maxPortIndex = 0;
		for (InputPort inPort : getInPorts()) {
			if (inPort.getInputPortNumber() > maxPortIndex) {
				maxPortIndex = inPort.getInputPortNumber(); 
			}
		}
		BaseTreeFormatterProvider provider = createFormatterProvider(engineMapping, maxPortIndex);

		TransformationGraph graph = getGraph();
		writer = new MultiFileWriter(provider, graph.getRuntimeContext().getContextURL(), fileURL);

		writer.setLogger(LOGGER);
		writer.setAppendData(false);
		writer.setUseChannel(true);
		writer.setOutputPort(getOutputPort(OUTPUT_PORT));
		writer.setDictionary(graph.getDictionary());
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

	protected abstract BaseTreeFormatterProvider createFormatterProvider(WritableMapping engineMapping, int maxPortIndex) throws ComponentNotReadyException;

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

		try {
			// Init new cache record manager
			tempDirectory = getGraph().getAuthorityProxy().newTempDir("tree-writer-cache-", -1);
			File file = new File(tempDirectory, "jdbm-cache");
			sharedCache = CacheRecordManager.createInstance(file.getAbsolutePath(), cacheSize);
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
		} catch (TempFileCreationException e) {
			throw new ComponentNotReadyException(e);
		}

		for (PortData portData : portDataMap.values()) {
			portData.setSharedCache(sharedCache);
			portData.preExecute();
		}
	}

	@Override
	public Result execute() throws Exception {
		loadDataToCache();

		// preExecute code must be placed into execute, as header might contain data from records
		PortBinding portBinding = engineMapping.getPartitionElement().getPortBinding();
		if (firstRun()) {
			if (engineMapping.getPartitionElement() != null) {
				writer.init(inPorts.get(portBinding.getPortIndex()).getMetadata());
			} else {
				// Dummy, but valid metadata
				writer.init(inPorts.firstEntry().getValue().getMetadata());
			}
		} else {
			writer.reset();
		}

		if (engineMapping.getPartitionElement() != null) {
			DataIterator iterator = portBinding.getPortData().iterator(null, null, null, null);
			portBinding.setIterator(iterator);
			while (iterator.hasNext()) {
				writer.write(iterator.next());
			}
		}

		// postExecute code must be placed into execute, as footer might contain data from records
		writer.finish();
		readRemainingData();

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	private void loadDataToCache() throws Exception {
		List<InputReader> readers = new ArrayList<InputReader>();
		for (PortData portData : portDataMap.values()) {
			if (portData.readInputPort()) {
				readers.add(new InputReader(portData));
			}
		}

		manageReaders(readers);
	}

	private void readRemainingData() throws Exception {
		List<InputReader> readers = new ArrayList<InputReader>();
		for (InputPort inPort : inPorts.values()) {
			if (!inPort.isEOF()) {
				readers.add(new InputReader(inPort));
			}
		}

		manageReaders(readers);
	}

	private void manageReaders(List<InputReader> readers) throws Exception {
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
				
				if (throwableException != null) {
					//inputReader.interrupt();
					throw new Exception(throwableException);
				}
				
				killIt = !runIt;
				try {
					inputReader.join(1000);
				} catch (InterruptedException e) {
					LOGGER.debug(getId() + " thread interrupted, it will interrupt child threads", e);
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
			sharedCache.close();
			writer.close();
			if (tempDirectory != null) {
				FileUtils.deleteRecursively(tempDirectory);
			}
		} catch (IOException e) {
			throw new ComponentNotReadyException(e);
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

	public void setFileUrl(String fileURL) {
		this.fileURL = fileURL;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setMappingString(String mappingString) {
		this.mappingString = mappingString;
	}

	public void setMappingURL(String mappingURL) {
		this.mappingURL = mappingURL;
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

		@Override
		public void run() {
			while (runIt) {
				try {
					DataRecord record = DataRecordFactory.newRecord(inPort.getMetadata());
					record.init();
					if (inPort.readRecord(record) == null) {
						return;
					}
					if (portData != null) {
						portData.put(record);
					}

				} catch (InterruptedException e) {
					LOGGER.debug(getId() + ": thread forcibly aborted", e);
					return;
				} catch (Exception e) {
					LOGGER.error(getId() + ": thread failed", e);
					return;
				} catch (Throwable e) {
					throwableException = e;
					return;
				}
			}
		}
	}
}
