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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
import org.jetel.component.tree.reader.InputAdapter;
import org.jetel.component.tree.reader.TreeReaderParserProvider;
import org.jetel.component.tree.reader.TreeStreamParser;
import org.jetel.component.tree.reader.TreeXMLReaderAdaptor;
import org.jetel.component.tree.reader.TreeXmlContentHandlerAdapter;
import org.jetel.component.tree.reader.XPathEvaluator;
import org.jetel.component.tree.reader.XPathPushParser;
import org.jetel.component.tree.reader.XPathSequenceProvider;
import org.jetel.component.tree.reader.mappping.MalformedMappingException;
import org.jetel.component.tree.reader.mappping.MappingContext;
import org.jetel.component.tree.reader.mappping.MappingElementFactory;
import org.jetel.component.tree.reader.xml.XmlXPathEvaluator;
import org.jetel.data.DataRecord;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
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
import org.jetel.sequence.PrimitiveSequence;
import org.jetel.util.AutoFilling;
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

	protected static void readCommonAttributes(TreeReader treeReader, ComponentXMLAttributes xattribs)
			throws XMLConfigurationException {
		try {
			treeReader.setFileURL(xattribs.getStringEx(XML_FILE_URL_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF));
			if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
				treeReader.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
			}
			treeReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null));

			String mappingURL = xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, null, RefResFlag.SPEC_CHARACTERS_OFF);
			String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE, null);
			if (mappingURL != null) {
				treeReader.setMappingURL(mappingURL);
			} else if (mapping != null) {
				treeReader.setMappingString(mapping);
			} else {
				// throw configuration exception
				xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, RefResFlag.SPEC_CHARACTERS_OFF);
			}
		} catch (Exception ex) {
			throw new XMLConfigurationException(treeReader.getType() + ":" + xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}
	}

	// DataRecordProvider, DataRecordReceiver, XPathSequenceProvider properties
	private DataRecord outputRecords[];
	private OutputPort outputPorts[];
	private int sequenceId;
	private Map<MappingContext, Sequence> sequences = new HashMap<MappingContext, Sequence>();

	protected String fileURL;
	protected String charset;
	private SourceIterator sourceIterator;

	private PolicyType policyType;

	private String mappingString;
	private String mappingURL;

	private TreeReaderParserProvider parserProvider;
	private TreeProcessor treeProcessor;

	private AutoFilling autoFilling = new AutoFilling();
	private volatile Throwable throwableException;

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

		sourceIterator = createSourceIterator();

		// FIXME: mapping should not be initialized here in init if is passed via external file, since it is possible
		// that mapping file does not exist at this moment, or its content can change
		MappingContext rootContext = createMapping();
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
	}

	private void recordProviderReceiverInit() {
		int portCount = getOutPorts().size();
		outputRecords = new DataRecord[portCount];
		outputPorts = new OutputPort[portCount];
		for (int i = 0; i < portCount; ++i) {
			OutputPort port = getOutputPort(i);
			outputPorts[i] = port;

			DataRecord record = new DataRecord(port.getMetadata());
			record.init();
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
		iterator.setPropertyRefResolver(new PropertyRefResolver(graph.getGraphProperties()));
		iterator.setDictionary(graph.getDictionary());
		return iterator;
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
				} else if (e.getCause() instanceof BadDataFormatException) {
					throw (BadDataFormatException) e.getCause();
				}

				throw new IllegalStateException("Unexpected exception", e);
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
			File tmpFile = new File(autoFilling.getFilename());
			long timestamp = tmpFile.lastModified();
			autoFilling.setFileSize(tmpFile.length());
			autoFilling.setFileTimestamp(timestamp == 0 ? null : new Date(timestamp));

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

	@Override
	public void receive(DataRecord record, int port) throws AbortParsingException {
		if (runIt) {
			try {
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
	public void exceptionOccurred(BadDataFormatException e) throws AbortParsingException {
		if (policyType == PolicyType.STRICT) {
			LOG.error("Could not assign data field.", e);
			throw new AbortParsingException(e);
		}
		// TODO: review this part of handling autofilling
		autoFilling.incGlobalCounter();
		autoFilling.incSourceCounter();
	}

	@Override
	public DataRecord getDataRecord(int port) throws AbortParsingException {
		if (runIt) {
			DataRecord record = outputRecords[port];
			record.reset();
			return record;
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
					LOG.warn(getId() + " thread interrupted, it will interrupt child threads", e);
					killIt = true;
				}
			}
			
			checkThrownException();
		}
		
		private void checkThrownException() throws Exception {
			if (throwableException != null) {
				if (throwableException instanceof AbortParsingException) {
					throw (AbortParsingException) throwableException;
				} else {
					throw new Exception(throwableException);
				}
			}
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
				throwableException = t;
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
				throwableException = t;
			}
		}

		private void setInput(Reader pipedReader) {
			this.pipedReader = pipedReader;
		}

	}

}
