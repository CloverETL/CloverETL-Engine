/*
 * CloverETL Engine - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com).  Use is subject to license terms.
 *
 * www.cloveretl.com
 */
package org.jetel.data.parser;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.parser.AbstractXmlSaxParser;
import org.jetel.data.sequence.Sequence;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.TransformationGraph;
import org.jetel.sequence.PrimitiveSequence;
import org.jetel.util.AutoFilling;
import org.jetel.util.ReadableChannelIterator;
import org.jetel.util.file.FileUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * @author mlaska (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Jan 9, 2012
 */
public class XMLEXmlSaxParser extends AbstractXmlSaxParser {

	private static final Log logger = LogFactory.getLog(XMLEXmlSaxParser.class);

	private static final String FEATURES_DELIMETER = ";";
	private static final String FEATURES_ASSIGN = ":=";

	/**
	 * @param graph
	 * @param parentComponent
	 * @param mapping
	 */
	public XMLEXmlSaxParser(TransformationGraph graph, Node parentComponent, String mapping) {
		super(graph, parentComponent, mapping);
	}

	@Override
	public void init() throws ComponentNotReadyException {
		augmentNamespaceURIs();

		URL projectURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;

		// prepare mapping
		NodeList mappingNodes = null;
		if (mappingURL != null) {
			try {
				ReadableByteChannel ch = FileUtils.getReadableChannel(projectURL, mappingURL);
				Document doc = createDocumentFromChannel(ch);
				Element rootElement = doc.getDocumentElement();
				mappingNodes = rootElement.getChildNodes();
			} catch (Exception e) {
				throw new ComponentNotReadyException(e);
			}
		} else if (mapping != null) {
			Document doc;
			try {
				doc = createDocumentFromString(mapping);
			} catch (XMLConfigurationException e) {
				throw new ComponentNotReadyException(e);
			}
			Element rootElement = doc.getDocumentElement();
			mappingNodes = rootElement.getChildNodes();
		}
		// iterate over 'Mapping' elements
		declaredTemplates.clear();
		String errorPrefix = parentComponent.getId() + ": Mapping error - ";
		for (int i = 0; i < mappingNodes.getLength(); i++) {
			org.w3c.dom.Node node = mappingNodes.item(i);
			List<String> errors = processMappings(graph, null, node);
			for (String error : errors) {
				logger.warn(errorPrefix + error);
			}
		}

		// test that we have at least one input port and one output
		if (parentComponent.getOutPorts().size() < 1) {
			throw new ComponentNotReadyException(parentComponent.getId() + ": At least one output port has to be defined!");
		}

		if (m_elementPortMap.size() < 1) {
			throw new ComponentNotReadyException(parentComponent.getId() + ": At least one mapping has to be defined.  <Mapping element=\"elementToMatch\" outPort=\"123\" [parentKey=\"key in parent\" generatedKey=\"new foreign key in target\"]/>");
		}
	}

	@Override
	protected OutputPort getOutputPort(int outPortIndex) {
		return parentComponent.getOutputPort(outPortIndex);
	}

	@Override
	protected Sequence createPrimitiveSequence(String id, TransformationGraph graph, String name) {
		return new PrimitiveSequence(id, graph, name);
	}

	/**
	 * Parses the inputSource. The SAXHandler defined in this class will handle the rest of the events. Returns false if
	 * there was an exception encountered during processing.
	 */
	public boolean parse(boolean validate, String xmlFeatures, String charset,
			ReadableChannelIterator readableChannelIterator, AutoFilling autoFilling, InputSource m_inputSource)
			throws JetelException {
		// create new sax factory
		SAXParserFactory factory = SAXParserFactory.newInstance();
		factory.setValidating(validate);
		factory.setNamespaceAware(true);
		initXmlFeatures(factory, xmlFeatures);
		SAXParser parser;
		Set<String> xmlAttributes = getXMLMappingValues(charset);

		try {
			// create new sax parser
			parser = factory.newSAXParser();
		} catch (Exception ex) {
			throw new JetelException(ex.getMessage(), ex);
		}

		try {
			// prepare next source
			if (readableChannelIterator.isGraphDependentSource()) {
				try {
					if (!nextSource(readableChannelIterator, autoFilling, m_inputSource))
						return true;
				} catch (JetelException e) {
					/*
					 * "FileURL attribute (" + inputFile + ") doesn't contain valid file url."
					 */
					throw new ComponentNotReadyException(e.getMessage(), e);
				}
			}
			do {
				if (m_inputSource != null) {
					// parse the input source
					parser.parse(m_inputSource, createNewSAXHandler(xmlAttributes));
				}

				// get a next source
			} while (nextSource(readableChannelIterator, autoFilling, m_inputSource));

		} catch (SAXException ex) {
			// process error
			if (!parentComponent.runIt()) {
				return true; // we were stopped by a stop signal... probably
			}
			logger.error("XML Extract: " + parentComponent.getId() + " Parse Exception" + ex.getMessage(), ex);
			throw new JetelException("XML Extract: " + parentComponent.getId() + " Parse Exception", ex);
		} catch (Exception ex) {
			logger.error("XML Extract: " + parentComponent.getId() + " Unexpected Exception", ex);
			throw new JetelException("XML Extract: " + parentComponent.getId() + " Unexpected Exception", ex);
		}
		return true;
	}

	/**
	 * Xml features initialization.
	 * 
	 * @throws JetelException
	 */
	private void initXmlFeatures(SAXParserFactory factory, String xmlFeatures) throws JetelException {
		if (xmlFeatures == null)
			return;
		String[] aXmlFeatures = xmlFeatures.split(FEATURES_DELIMETER);
		String[] aOneFeature;
		try {
			for (String oneFeature : aXmlFeatures) {
				aOneFeature = oneFeature.split(FEATURES_ASSIGN);
				if (aOneFeature.length != 2)
					throw new JetelException("The xml feature '" + oneFeature + "' has wrong format");
				factory.setFeature(aOneFeature[0], Boolean.parseBoolean(aOneFeature[1]));
			}
		} catch (Exception e) {
			throw new JetelException(e.getMessage(), e);
		}
	}

	private Set<String> getXMLMappingValues(String charset) {
		try {
			SAXParser saxParser = SAXParserFactory.newInstance().newSAXParser();
			DefaultHandler handler = new MyHandler();
			InputStream is = null;
			if (this.mappingURL != null) {
				String filePath = FileUtils.getFile(graph.getRuntimeContext().getContextURL(), mappingURL);
				is = new FileInputStream(new File(filePath));
			} else if (this.mapping != null) {
				is = new ByteArrayInputStream(mapping.getBytes(charset));
			}
			if (is != null) {
				saxParser.parse(is, handler);
				return ((MyHandler) handler).getCloverAttributes();
			}
		} catch (Exception e) {
			return new HashSet<String>();
		}
		return new HashSet<String>();
	}

	/**
	 * Switch to the next source file.
	 * 
	 * @return
	 * @throws JetelException
	 */
	private boolean nextSource(ReadableChannelIterator readableChannelIterator, AutoFilling autoFilling,
			InputSource m_inputSource) throws JetelException {
		ReadableByteChannel stream = null;
		while (readableChannelIterator.hasNext()) {
			autoFilling.resetSourceCounter();
			autoFilling.resetGlobalSourceCounter();
			stream = readableChannelIterator.nextChannel();
			if (stream == null) {
				continue; // if record no record found
			}
			autoFilling.setFilename(readableChannelIterator.getCurrentFileName());
			File tmpFile = new File(autoFilling.getFilename());
			long timestamp = tmpFile.lastModified();
			autoFilling.setFileSize(tmpFile.length());
			autoFilling.setFileTimestamp(timestamp == 0 ? null : new Date(timestamp));
			m_inputSource = new InputSource(Channels.newInputStream(stream));
			return true;
		}
		readableChannelIterator.blankRead();
		return false;
	}
}
