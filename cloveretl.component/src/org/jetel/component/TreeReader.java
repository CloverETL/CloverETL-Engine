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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.tree.reader.AbortParsingException;
import org.jetel.component.tree.reader.DataRecordProvider;
import org.jetel.component.tree.reader.DataRecordReceiver;
import org.jetel.component.tree.reader.FieldFillingException;
import org.jetel.component.tree.reader.InputAdapter;
import org.jetel.component.tree.reader.TreeReaderParserProvider;
import org.jetel.component.tree.reader.TreeStreamParser;
import org.jetel.component.tree.reader.TreeXMLReaderAdaptor;
import org.jetel.component.tree.reader.TreeXmlContentHandlerAdapter;
import org.jetel.component.tree.reader.XPathEvaluator;
import org.jetel.component.tree.reader.XPathPushParser;
import org.jetel.component.tree.reader.XPathSequenceProvider;
import org.jetel.component.tree.reader.mappping.FieldMapping;
import org.jetel.component.tree.reader.mappping.ImplicitMappingAddingVisitor;
import org.jetel.component.tree.reader.mappping.MalformedMappingException;
import org.jetel.component.tree.reader.mappping.MappingContext;
import org.jetel.component.tree.reader.mappping.MappingElementFactory;
import org.jetel.component.tree.reader.mappping.MappingVisitor;
import org.jetel.component.tree.reader.xml.XmlXPathEvaluator;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.sequence.PrimitiveSequence;
import org.jetel.util.AutoFilling;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.SourceIterator;
import org.jetel.util.XmlUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;

/**
 * @author lkrejci (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created 19 Jan 2012
 */
public abstract class TreeReader extends Node implements DataRecordProvider, DataRecordReceiver, XPathSequenceProvider {

	/**
	 * TreeReader works in several processing modes. Chosen processing mode depends on provided
	 * {@link TreeReaderParserProvider} and mapping complexity. The priority of modes:
	 * <ol>
	 * <li>{@link ProcessingMode#STREAM}</li>
	 * <li>{@link ProcessingMode#XPATH_DIRECT}</li>
	 * <li>{@link ProcessingMode#XPATH_CONVERT_STREAM}</li>
	 * </ol>
	 * 
	 * @author krejcil (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
	 */
	private static enum ProcessingMode {
		/**
		 * #NOT IMPLEMENTED# {@link TreeReaderParserProvider} provides {@link TreeStreamParser} and mapping is simple
		 * enough input is processed in SAX-like manner
		 */
		STREAM,
		/**
		 * {@link TreeReaderParserProvider} provides {@link XPathEvaluator} xpath expressions are evaluated directly on
		 * input
		 */
		XPATH_DIRECT,
		/**
		 * {@link TreeReaderParserProvider} provides {@link TreeReaderParserProvider} -- input is converted to xml and
		 * xml xpath evaluator is used to resolve xpath expressions on converted input
		 */
		XPATH_CONVERT_STREAM
	}

	private static final Log LOG = LogFactory.getLog(TreeReader.class);

	private static final int INPUT_PORT_INDEX = 0;

	// this attribute is not used at runtime right now
	public static final String XML_SCHEMA_ATTRIBUTE = "schema";
	protected final static String XML_FILE_URL_ATTRIBUTE = "fileURL";
	public final static String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
	public final static String XML_MAPPING_ATTRIBUTE = "mapping";
	public final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	public static final String XML_CHARSET_ATTRIBUTE = "charset";
	public static final String XML_IMPLICIT_MAPPING_ATTRIBUTE = "implicitMapping";

	protected static void readCommonAttributes(TreeReader treeReader, ComponentXMLAttributes xattribs)
			throws XMLConfigurationException, AttributeNotFoundException {
		treeReader.setFileURL(xattribs.getStringEx(XML_FILE_URL_ATTRIBUTE, RefResFlag.URL));
		if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
			treeReader.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
		}
		treeReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));

		String mappingURL = xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, null, RefResFlag.URL);
		String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE, null);
		if (mappingURL != null) {
			treeReader.setMappingURL(mappingURL);
		} else if (mapping != null) {
			treeReader.setMappingString(mapping);
		} else {
			// throw configuration exception
			xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, RefResFlag.URL);
		}

		treeReader.setImplicitMapping(xattribs.getBoolean(XML_IMPLICIT_MAPPING_ATTRIBUTE, false));
	}

	// DataRecordProvider, DataRecordReceiver, XPathSequenceProvider properties
	private DataRecord outputRecords[];
	private OutputPort outputPorts[];
	private boolean recordReadWithException[];
	private int sequenceId;
	private Map<MappingContext, Sequence> sequences = new HashMap<MappingContext, Sequence>();

	protected String fileURL;
	protected String charset;
	private SourceIterator sourceIterator;

	private PolicyType policyType;

	private String mappingString;
	private String mappingURL;
	private boolean implicitMapping;

	private TreeReaderParserProvider parserProvider;
	private TreeProcessor treeProcessor;

	private AutoFilling autoFilling = new AutoFilling();
	private int[] sourcePortRecordCounters; // counters of records written to particular ports per source

	private boolean errorPortLogging;
	private DataRecord errorLogRecord;
	private int maxErrors = -1;
	private int errorsCount;
	
	public TreeReader(String id) {
		super(id);
	}

	protected abstract TreeReaderParserProvider getTreeReaderParserProvider();

	protected ConfigurationStatus disallowEmptyCharsetOnDictionaryAndPort(ConfigurationStatus status) {
		ConfigurationStatus configStatus = super.checkConfig(status);
		
		for (String fileUrlEntry : this.getFileUrl().split(";")) {
			if ((fileUrlEntry.startsWith("dict:") || fileUrlEntry.startsWith("port:")) && charset == null) {
				status.add(new ConfigurationProblem("Charset cannot be auto-detected for input from a port or dictionary. Define it in the \"Charset\" attribute explicitly.", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
			}
		}
		
		return configStatus;
	}

	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		if (!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}

		if (charset != null && !Charset.isSupported(charset)) {
			status.add(new ConfigurationProblem("Charset " + charset + " not supported!", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
		}

		/*
		 * TODO validate mapping model in checkConfig with respect to port metadata i.e. that there are all fields
		 * available and all mentioned ports are connected
		 */

		return status;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}
		super.init();
		
		this.parserProvider = getTreeReaderParserProvider();

		recordProviderReceiverInit();
		for (OutputPort outPort : outPortsArray) {
			autoFilling.addAutoFillingFields(outPort.getMetadata());
		}
		sourcePortRecordCounters = new int[outPortsSize];

		sourceIterator = createSourceIterator();

		// FIXME: mapping should not be initialized here in init if is passed via external file, since it is possible
		// that mapping file does not exist at this moment, or its content can change
		MappingContext rootContext = createMapping();
		
		if (implicitMapping) {
			MappingVisitor implicitMappingVisitor = new ImplicitMappingAddingVisitor(getOutMetadata());
			rootContext.acceptVisitor(implicitMappingVisitor);
		}
		
		XPathPushParser pushParser;
		ProcessingMode processingMode = resolveProcessingMode();
		switch (processingMode) {
		case XPATH_CONVERT_STREAM:
			pushParser = new XPathPushParser(this, this, new XmlXPathEvaluator(), parserProvider.getValueHandler(), this);
			treeProcessor = new StreamConvertingXPathProcessor(parserProvider, pushParser, rootContext, charset);
			break;
		case XPATH_DIRECT:
			pushParser = new XPathPushParser(this, this, parserProvider.getXPathEvaluator(), parserProvider.getValueHandler(), this);
			InputAdapter inputAdapter = parserProvider.getInputAdapter();
			treeProcessor = new XPathProcessor(pushParser, rootContext, inputAdapter);
			break;
		default:
			throw new UnsupportedOperationException("Processing mode " + processingMode + " is not supported");
		}
		
		errorPortLogging = isErrorPortLogging(rootContext);
		if (errorPortLogging) {
			LOG.info("Using port " + getErrorPortIndex() + " as error logging port");
			errorLogRecord = DataRecordFactory.newRecord(getOutputPort(getErrorPortIndex()).getMetadata());
			errorLogRecord.init();
		}
	}

	private void recordProviderReceiverInit() {
		int portCount = getOutPorts().size();
		outputRecords = new DataRecord[portCount];
		outputPorts = new OutputPort[portCount];
		recordReadWithException = new boolean[portCount];
		for (int i = 0; i < portCount; ++i) {
			OutputPort port = getOutputPort(i);
			outputPorts[i] = port;

			DataRecord record = DataRecordFactory.newRecord(port.getMetadata());
			record.init();
			record.reset();
			outputRecords[i] = record;
		}
	}

	private ProcessingMode resolveProcessingMode() {
		if (parserProvider.providesXPathEvaluator()) {
			return ProcessingMode.XPATH_DIRECT;
		} else if (parserProvider.providesTreeStreamParser()) {
			return ProcessingMode.XPATH_CONVERT_STREAM;
		} else {
			throw new IllegalStateException("Invalid parser provider configuration");
		}
	}

	protected MappingContext createMapping() throws ComponentNotReadyException {
		Document mappingDocument;

		try {
			if (mappingURL != null) {
				TransformationGraph graph = getGraph();
				URL contextURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;
				ReadableByteChannel ch = FileUtils.getReadableChannel(contextURL, mappingURL);
				mappingDocument = XmlUtils.createDocumentFromChannel(ch);
			} else {
				mappingDocument = XmlUtils.createDocumentFromString(mappingString);
			}
		} catch (IOException e) {
			throw new ComponentNotReadyException("Mapping parameter parse error occured.", e);
		} catch (JetelException e) {
			throw new ComponentNotReadyException("Mapping parameter parse error occured.", e);
		}

		try {
			return new MappingElementFactory().readMapping(mappingDocument);
		} catch (MalformedMappingException e) {
			throw new ComponentNotReadyException("Input mapping is not valid.", e);
		}
	}

	protected SourceIterator createSourceIterator() {
		TransformationGraph graph = getGraph();
		URL projectURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;

		SourceIterator iterator = new SourceIterator(getInputPort(INPUT_PORT_INDEX), projectURL, fileURL);
		iterator.setCharset(charset);
		iterator.setPropertyRefResolver(graph != null ? new PropertyRefResolver(graph.getGraphProperties()) : null);
		iterator.setDictionary(graph.getDictionary());
		return iterator;
	}

	/**
	 * @return true iff the last out port is not used in the mapping and it has prescribed metadata
	 */
	private boolean isErrorPortLogging(MappingContext rootContext) {
		OutputPort errorPortCandidate = outPortsArray[getErrorPortIndex()];
		if (isPortUsed(errorPortCandidate.getOutputPortNumber(), rootContext)) {
			return false;
		} else {
			if (hasErrorLoggingMetadata(errorPortCandidate)) {
				return true;
			} else {
				LOG.warn("If the last output port is intended for error logging, metadata should be: "
						+ "integer (out port number), integer (record number per source and port), integer (field number), "
						+ "string (field name), string (value which caused the error), string (error message), string (source name - optional field)");
				return false;
			}
		}
    }
	
	private int getErrorPortIndex() {
		return outPortsSize - 1;
	}

	private boolean hasErrorLoggingMetadata(OutputPort errorPort) {
		DataRecordMetadata metadata = errorPort.getMetadata();
        int errorNumFields = metadata.getNumFields();
        return (errorNumFields == 6 || errorNumFields  == 7)
        		&& metadata.getField(0).getDataType() == DataFieldType.INTEGER	// port number
        		&& metadata.getField(1).getDataType() == DataFieldType.INTEGER	// port record number per source
        		&& metadata.getField(2).getDataType() == DataFieldType.INTEGER	// field number
        		&& isStringOrByte(metadata.getField(3))							// field name
        		&& isStringOrByte(metadata.getField(4))							// offending value
        		&& isStringOrByte(metadata.getField(5))							// error message
        		&& (errorNumFields == 6 || isStringOrByte(metadata.getField(6)));	// optional source name
	}

	private boolean isStringOrByte(DataFieldMetadata field) {
		return field.getDataType() == DataFieldType.STRING || field.getDataType() == DataFieldType.BYTE || field.getDataType() == DataFieldType.CBYTE;
	}
	
	private static boolean isPortUsed(int portIndex, MappingContext rootContext) {
		PortUsageMappingVisitor portUsageMappingVisitor = new PortUsageMappingVisitor();
		rootContext.acceptVisitor(portUsageMappingVisitor);
		return portUsageMappingVisitor.isPortUsed(portIndex);
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();

		// FIXME: init of source iterator is not implemented well right now, so it has to be called here in preExecute!
		sourceIterator.init();
		sourceIterator.preExecute();
	}

	@Override
	public Result execute() throws Exception {
		Object inputData = getNextSource();
		while (inputData != null) {
			try {
				treeProcessor.processInput(inputData);
			} catch (AbortParsingException e) {
				if (!runIt) {
					return Result.ABORTED;
				} else if (e.getCause() instanceof MaxErrorsCountExceededException) {
					return Result.ERROR;
				} else if (e.getCause() instanceof Exception) { // TODO BadDataFormatException / Exception ?
					throw (Exception) e.getCause();
				}
			} finally {
				if (inputData instanceof Closeable) {
					try {
						((Closeable) inputData).close();
					} catch (Exception ex) {
						LOG.error("Failed to close input");
					}
				}
			}
			inputData = getNextSource();
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		sequences.clear();
	}

	private Object getNextSource() throws JetelException {
		Object input = null;
		while (sourceIterator.hasNext()) {
			input = sourceIterator.next();
			if (input == null) {
				continue; // if record no record found
			}

			autoFilling.resetSourceCounter();
			autoFilling.resetGlobalSourceCounter();
			autoFilling.setFilename(sourceIterator.getCurrentFileName());
			if (!sourceIterator.isGraphDependentSource()) {
				long fileSize = 0;
				Date fileTimestamp = null;
				if (autoFilling.getFilename() != null && 
						FileUtils.isLocalFile(null, autoFilling.getFilename()) && 
						!sourceIterator.isGraphDependentSource()) {
					File tmpFile = new File(autoFilling.getFilename());
					long timestamp = tmpFile.lastModified();
					fileTimestamp = timestamp == 0 ? null : new Date(timestamp);
					fileSize = tmpFile.length();
				}
				autoFilling.setFileSize(fileSize);
				autoFilling.setFileTimestamp(fileTimestamp);
			}
			Arrays.fill(sourcePortRecordCounters, 0);

			return input;
		}

		sourceIterator.blankRead();
		return input;
	}

	public String getFileUrl() {
		return fileURL;
	}
	
	public void setFileURL(String fileURL) {
		this.fileURL = fileURL;
	}

	private void setCharset(String charset) {
		this.charset = charset;
	}

	public void setPolicyType(String strPolicyType) {
		policyType = PolicyType.valueOfIgnoreCase(strPolicyType);
	}

	public void setMappingString(String mappingString) {
		this.mappingString = mappingString;
	}

	public void setMappingURL(String mappingURL) {
		this.mappingURL = mappingURL;
	}
	
	public void setImplicitMapping(boolean implicitMapping) {
		this.implicitMapping = implicitMapping;
	}

	@Override
	public void receive(DataRecord record, int port) throws AbortParsingException {
		if (runIt) {
			try {
				sourcePortRecordCounters[port]++;
				autoFilling.incGlobalCounter();
				autoFilling.incSourceCounter();
				// FIXME: some autofilling fields should be filled sooner - so that it can be used with combination of
				// generated key pointing at autofilled field
				autoFilling.setAutoFillingFields(record);
				outputPorts[port].writeRecord(record);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new AbortParsingException();
		}
	}

	@Override
	public void exceptionOccurred(FieldFillingException e) throws AbortParsingException {
		if (policyType == PolicyType.STRICT) {
			LOG.error("Could not assign data field \"" + e.getFieldMetadata().getName() + "\" ("
					+ e.getFieldMetadata().getDataType().getName() + ") on port " + e.getPortIndex(), e.getCause());
			throw new AbortParsingException(e);
		} else if (policyType == PolicyType.CONTROLLED) {
			if (!recordReadWithException[e.getPortIndex()]) {
				recordReadWithException[e.getPortIndex()] = true;
				sourcePortRecordCounters[e.getPortIndex()]++;
			}
			if (errorPortLogging) {
				writeErrorLogRecord(e);
			} else {
				BadDataFormatException bdfe = e.getCause();
				bdfe.setRecordNumber(sourcePortRecordCounters[e.getPortIndex()]);
				bdfe.setFieldNumber(e.getFieldMetadata().getNumber());
				bdfe.setFieldName(e.getFieldMetadata().getName());
				bdfe.setRecordName(e.getFieldMetadata().getDataRecordMetadata().getName());
				String errorMsg = ExceptionUtils.getMessage(bdfe) + "; output port: " + e.getPortIndex();
				if (!sourceIterator.isSingleSource()) {
					errorMsg += "; input source: " + sourceIterator.getCurrentFileName();
				}
				LOG.error(errorMsg);
			}
			if (maxErrors != -1 && ++errorsCount > maxErrors) {
				LOG.error("Max errors count exceeded.", e);
				throw new AbortParsingException(new MaxErrorsCountExceededException());
			}
		}
	}

	private void writeErrorLogRecord(FieldFillingException e) {
		int i = 0;
		errorLogRecord.getField(i++).setValue(e.getPortIndex());
		errorLogRecord.getField(i++).setValue(sourcePortRecordCounters[e.getPortIndex()]);
		errorLogRecord.getField(i++).setValue(e.getFieldMetadata().getNumber() + 1);
		setCharSequenceToField(e.getFieldMetadata().getName(), errorLogRecord.getField(i++));
		setCharSequenceToField(e.getCause().getOffendingValue(), errorLogRecord.getField(i++));
		setCharSequenceToField(ExceptionUtils.getMessage(e.getCause()), errorLogRecord.getField(i++));
		if (errorLogRecord.getNumFields() > i) {
			setCharSequenceToField(sourceIterator.getCurrentFileName(), errorLogRecord.getField(i++));
		}

		try {
			outputPorts[getErrorPortIndex()].writeRecord(errorLogRecord);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}
	
	private void setCharSequenceToField(CharSequence charSeq, DataField field) {
		if (charSeq == null) {
			field.setNull(true);
		} else {
			field.setNull(false);
			
			if (field.getMetadata().getDataType() == DataFieldType.STRING) {
				field.setValue(charSeq);
			} else if (field.getMetadata().getDataType() == DataFieldType.BYTE || field.getMetadata().getDataType() == DataFieldType.CBYTE) {
				String cs;
				if (charset != null) {
					cs = charset;
				} else {
					cs = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
				}
				try {
					field.setValue(charSeq.toString().getBytes(cs));
				} catch (UnsupportedEncodingException e) {
					LOG.error(getId() + ": failed to write log record", e);
				}
			} else {
				throw new IllegalArgumentException("Type of field \""+ field.getMetadata().getName() +"\" has to be string, byte or cbyte");
			}
		}
	}
	
	@Override
	public DataRecord getDataRecord(int port) throws AbortParsingException {
		if (runIt) {
			recordReadWithException[port] = false;
			/*
			 * answer copy of record instead of re-usage because
			 * parser could ask for new record without previous record
			 * having been written
			 */
			return outputRecords[port].duplicate();
		} else {
			throw new AbortParsingException();
		}
	}

	@Override
	public Sequence getSequence(MappingContext context) {

		Sequence sequence = sequences.get(context);
		if (sequence == null) {
			if (context.getSequenceId() != null) {
				sequence = getGraph().getSequence(context.getSequenceId());
			}
			if (sequence == null) {
				
				String id = getType() + "Seq_" + sequenceId++;
				sequence = SequenceFactory.createSequence(getGraph(), PrimitiveSequence.SEQUENCE_TYPE, new Object[] { id, getGraph(), context.getSequenceField() }, new Class[] { String.class, TransformationGraph.class, String.class });
			}
			sequences.put(context, sequence);
		}
		return sequence;
	}
	
	/**
	 * Interface for classes encapsulating the functionality of one {@link ProcessingMode} of TreeReader
	 * 
	 * @author krejcil (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
	 * 
	 * @created 10.3.2012
	 */
	private interface TreeProcessor {
		void processInput(Object input) throws Exception;
	}


	/**
	 * TreeProcessor implementing {@link ProcessingMode#XPATH_DIRECT} mode
	 * 
	 * @author krejcil (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
	 * 
	 * @created 10.3.2012
	 */
	private static class XPathProcessor implements TreeProcessor {

		private XPathPushParser pushParser;
		private MappingContext rootContext;
		private InputAdapter inputAdapter;

		private XPathProcessor(XPathPushParser pushParser, MappingContext rootContext, InputAdapter inputAdapter) {
			this.pushParser = pushParser;
			this.rootContext = rootContext;
			this.inputAdapter = inputAdapter;
		}

		@Override
		public void processInput(Object input) throws AbortParsingException {
			pushParser.parse(rootContext, inputAdapter.adapt(input));
		}
	}

	/**
	 * TreeProcessor implementing {@link ProcessingMode#XPATH_CONVERT_STREAM} mode.
	 * 
	 * @author krejcil (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
	 * 
	 * @created 14.3.2012
	 */
	private class StreamConvertingXPathProcessor implements TreeProcessor {

		private PipeTransformer pipeTransformer;
		private PipeParser pipeParser;
		boolean killIt = false;
		TreeReaderParserProvider parserProvider;
		XPathPushParser pushParser;
		MappingContext rootContext;

		private String charset;
		/*
		 * Pointer to exception thrown in different thread
		 */
		volatile Throwable failure;

		public StreamConvertingXPathProcessor(TreeReaderParserProvider parserProvider, XPathPushParser pushParser,
				MappingContext rootContext, String charset) {
			this.charset = charset;

			this.parserProvider = parserProvider;
			this.pushParser = pushParser;
			this.rootContext = rootContext;
			
		}

		@Override
		public void processInput(Object input) throws Exception {
			if (input instanceof ReadableByteChannel) {
				/*
				 * Convert input stream to XML
				 */
				InputSource source = new InputSource(Channels.newInputStream((ReadableByteChannel) input));
				if (charset != null) {
					source.setEncoding(charset);
				}

				Pipe pipe = Pipe.open();
				
				try {
					TreeStreamParser treeStreamParser = parserProvider.getTreeStreamParser();
					treeStreamParser.setTreeContentHandler(new TreeXmlContentHandlerAdapter());
					XMLReader treeXmlReader = new TreeXMLReaderAdaptor(treeStreamParser);
					pipeTransformer = new PipeTransformer(treeXmlReader);

					pipeTransformer.setInputOutput(Channels.newWriter(pipe.sink(), "UTF-8"), source);
					pipeParser = new PipeParser(pushParser, rootContext);
					pipeParser.setInput(Channels.newReader(pipe.source(), "UTF-8"));
				} catch (TransformerFactoryConfigurationError e) {
					throw new JetelRuntimeException("Failed to instantiate transformer", e);
				}

				pipeTransformer.start();
				pipeParser.start();

				manageThread(pipeTransformer);
				manageThread(pipeParser);
			} else {
				throw new JetelRuntimeException("Could not read input " + input);
			}

		}

		private void manageThread(Thread thread) throws Exception {
			while (thread.getState() != Thread.State.TERMINATED) {
				if (killIt) {
					thread.interrupt();
					break;
				}
				
				checkThrownException();

				killIt = !runIt;
				try {
					thread.join(1000);
				} catch (InterruptedException e) {
					LOG.debug(getId() + " thread interrupted, it will interrupt child threads", e);
					killIt = true;
				}
			}
			
			checkThrownException();
		}
		
		private void checkThrownException() throws Exception {
			try {
				if (failure != null) {
					if (failure instanceof AbortParsingException) {
						throw (AbortParsingException) failure;
					} else {
						throw new Exception(failure);
					}
				}
			} finally {
				// clear exception for case this instance would be re-used 
				failure = null;
			}
		}

		private class PipeTransformer extends Thread {

			private XMLReader treeXmlReader;
			private Transformer transformer;
			private Writer pipedWriter;
			private InputSource source;

			public PipeTransformer(XMLReader treeXmlReader) {
				try {
					this.transformer = TransformerFactory.newInstance().newTransformer();
					this.treeXmlReader = treeXmlReader;
				} catch (TransformerConfigurationException e) {
					throw new JetelRuntimeException("Failed to instantiate transformer", e);
				} catch (TransformerFactoryConfigurationError e) {
					throw new JetelRuntimeException("Failed to instantiate transformer", e);
				}
			}

			@Override
			public void run() {
				javax.xml.transform.Result result = new StreamResult(pipedWriter);
				try {
					transformer.transform(new SAXSource(treeXmlReader, source), result);
					pipedWriter.close();
				} catch (Throwable t) {
					StreamConvertingXPathProcessor.this.failure = t;
				}
			}

			public void setInputOutput(Writer pipedWriter, InputSource source) {
				this.pipedWriter = pipedWriter;
				this.source = source;
			}
		}
		
		private class PipeParser extends Thread {

			private XPathPushParser pushParser;
			private MappingContext rootContext;

			private Reader pipedReader;

			public PipeParser(XPathPushParser pushParser, MappingContext rootContext) {
				this.pushParser = pushParser;
				this.rootContext = rootContext;
			}

			@Override
			public void run() {
				try {
					pushParser.parse(rootContext, new SAXSource(new InputSource(pipedReader)));
				} catch (Throwable t) {
					StreamConvertingXPathProcessor.this.failure = t;
				}
			}

			private void setInput(Reader pipedReader) {
				this.pipedReader = pipedReader;
			}
		}
	}

	
	private static class MaxErrorsCountExceededException extends RuntimeException {
		private static final long serialVersionUID = -3499614028763254366L;
	}
	
	private static class PortUsageMappingVisitor implements MappingVisitor {
		
		private Set<Integer> usedPortIndexes = new HashSet<Integer>();

		@Override
		public void visitBegin(MappingContext context) {
			Integer port = context.getOutputPort();
			if (port != null) {
				usedPortIndexes.add(port);
			}
		}

		@Override
		public void visitEnd(MappingContext context) { }

		@Override
		public void visit(FieldMapping mapping) { }
		
		public boolean isPortUsed(int portIndex) {
			return usedPortIndexes.contains(portIndex);
		}
		
	}

}