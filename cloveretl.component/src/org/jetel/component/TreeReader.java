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
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.tree.TreeReaderParserProvider;
import org.jetel.component.xpathparser.AbortParsingException;
import org.jetel.component.xpathparser.DataRecordProvider;
import org.jetel.component.xpathparser.DataRecordReceiver;
import org.jetel.component.xpathparser.XPathPushParser;
import org.jetel.component.xpathparser.XPathSequenceProvider;
import org.jetel.component.xpathparser.mappping.MalformedMappingException;
import org.jetel.component.xpathparser.mappping.MappingContext;
import org.jetel.component.xpathparser.mappping.MappingElementFactory;
import org.jetel.component.xpathparser.xml.XmlXPathEvaluator;
import org.jetel.data.DataRecord;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.data.tree.parser.TreeStreamParser;
import org.jetel.data.tree.parser.TreeXMLReaderAdaptor;
import org.jetel.data.tree.parser.TreeXmlContentHandlerAdapter;
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

	private static enum ProcessingMode {
		STREAM, XPATH_DIRECT, XPATH_CONVERT_DIRECT, XPATH_CONVERT_STREAM
	}

	private static final Log LOG = LogFactory.getLog(TreeReader.class);

	private static final int INPUT_PORT_INDEX = 0;

	// this attribute is not used at runtime right now
	public static final String XML_SCHEMA_ATTRIBUTE = "schema";
	public final static String XML_FILE_URL_ATTRIBUTE = "fileURL";
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

	private DataRecord outputRecords[];
	private OutputPort outputPorts[];

	private int sequenceId;
	private Map<MappingContext, Sequence> sequences = new HashMap<MappingContext, Sequence>();

	private String fileURL;
	private String charset;
	private SourceIterator sourceIterator;

	private PolicyType policyType;

	private String mappingString;
	private String mappingURL;

	
	private TreeReaderParserProvider parserProvider;
	
	private ProcessingMode processingMode;

	private MappingContext rootContext;
	private XPathPushParser parser;
	
	public TreeReader(String id) {
		super(id);
		this.parserProvider = getTreeReaderParserProvider();
	}
	
	protected abstract TreeReaderParserProvider getTreeReaderParserProvider();

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

		initOutPort();
		rootContext = createMapping();
		sourceIterator = createSourceIterator();
		sourceIterator.init();

		processingMode = resolveProcessingMode();
		switch (processingMode) {
		case XPATH_CONVERT_STREAM:
			parser = new XPathPushParser(this, this, new XmlXPathEvaluator(), parserProvider.getValueHandler(), this);
			break;
		default:
			throw new UnsupportedOperationException("Processing mode" + processingMode + "is not supported");
		}
	}

	private void initOutPort() {
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
		return ProcessingMode.XPATH_CONVERT_STREAM;
	}

	private MappingContext createMapping() throws ComponentNotReadyException {
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
		sourceIterator.preExecute();

		if (!firstRun()) {
			sequences.clear();
		}
	}

	@Override
	public Result execute() throws Exception {
		switch (processingMode) {
		case XPATH_CONVERT_STREAM:
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			TreeStreamParser treeStreamParser = parserProvider.getTreeStreamParser();
			treeStreamParser.setTreeContentHandler(new TreeXmlContentHandlerAdapter());
			XMLReader treeXmlReader = new TreeXMLReaderAdaptor(treeStreamParser);

			Object inputData = getNextSource();
			while (inputData != null) {
				if (inputData instanceof ReadableByteChannel) {
					InputStream inputStream = Channels.newInputStream((ReadableByteChannel) inputData);
					InputSource source = new InputSource(inputStream);
					StringWriter writer = new StringWriter();
					javax.xml.transform.Result result = new StreamResult(writer);
					transformer.transform(new SAXSource(treeXmlReader, source), result);
					
					// TODO: There is definitely better way how to do this... I'm just right now running out of time :-/
					parser.parse(rootContext, new SAXSource(new InputSource(new StringReader(writer.toString()))));
				} else {
					throw new JetelRuntimeException("Could not read input " + inputData);
				}

				inputData = getNextSource();
			}
			break;
		default:
			throw new JetelRuntimeException();
		}
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	private Object getNextSource() throws JetelException {
		Object input = null;
		while (sourceIterator.hasNext()) {
			input = sourceIterator.next();
			if (input == null) {
				continue; // if record no record found
			}
			
			return input;
		}

		sourceIterator.blankRead();
		return input;
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
			throw new AbortParsingException();
		}
	}

	@Override
	public DataRecord getDataRecord(int port) throws AbortParsingException {
		if (runIt) {
			return outputRecords[port];
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
				String id = "XPathBeanReaderSeq_" + sequenceId++;
				sequence = SequenceFactory.createSequence(getGraph(), PrimitiveSequence.SEQUENCE_TYPE, new Object[] { id, getGraph(), context.getSequenceField() }, new Class[] { String.class, TransformationGraph.class, String.class });
			}
			sequences.put(context, sequence);
		}
		return sequence;
	}

}
