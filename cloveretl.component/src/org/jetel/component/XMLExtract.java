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
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.io.StringReader;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.parser.Parser.DataSourceType;
import org.jetel.data.parser.XmlSaxParser;
import org.jetel.data.parser.XmlSaxParser.MyHandler;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.AutoFilling;
import org.jetel.util.ReadableChannelIterator;
import org.jetel.util.XmlUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.file.stream.Input;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * <h3>XMLExtract Component</h3>
 * 
 * <!-- Provides the logic to parse a xml file and filter to different ports based on a matching element. The element
 * and all children will be turned into a Data record -->
 * 
 * <table border="1">
 * <th>Component:</th>
 * <tr>
 * <td>
 * <h4><i>Name:</i></h4></td>
 * <td>XMLExtract</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Category:</i></h4></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Description:</i></h4></td>
 * <td>Provides the logic to parse a xml file and filter to different ports based on a matching element. The element and
 * all children will be turned into a Data record.</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Inputs:</i></h4></td>
 * <td>0</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Outputs:</i></h4></td>
 * <td>Output port[0] defined/connected. Depends on mapping definition.</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Comment:</i></h4></td>
 * <td></td>
 * </tr>
 * </table>
 * <br>
 * <table border="1">
 * <th>XML attributes:</th>
 * <tr>
 * <td><b>type</b></td>
 * <td>"XML_EXTRACT"</td>
 * </tr>
 * <tr>
 * <td><b>id</b></td>
 * <td>component identification</td>
 * <tr>
 * <td><b>sourceUri</b></td>
 * <td>location of source XML data to process</td>
 * <tr>
 * <td><b>useNestedNodes</b></td>
 * <td><b>true</b> if nested unmapped XML elements will be used as data source; <b>false</b> if will be ignored</td>
 * <tr>
 * <td><b>mapping</b></td>
 * <td>&lt;mapping&gt;</td>
 * </tr>
 * </table>
 * 
 * Provides the logic to parse a xml file and filter to different ports based on a matching element. The element and all
 * children will be turned into a Data record.<br>
 * Mapping attribute contains mapping hierarchy in XML form. DTD of mapping:<br>
 * <code>
 * &lt;!ELEMENT Mappings (Mapping*)&gt;<br>
 * 
 * &lt;!ELEMENT Mapping (Mapping*)&gt;<br>
 * &lt;!ATTLIST Mapping<br>
 * &nbsp;element NMTOKEN #REQUIRED<br>      
 * &nbsp;&nbsp;//name of binded XML element<br>  
 * &nbsp;outPort NMTOKEN #IMPLIED<br>      
 * &nbsp;&nbsp;//name of output port for this mapped XML element<br>
 * &nbsp;parentKey NMTOKEN #IMPLIED<br>     
 * &nbsp;&nbsp;//field name of parent record, which is copied into field of the current record<br>
 * &nbsp;&nbsp;//passed in generatedKey attribute<br> 
 * &nbsp;generatedKey NMTOKEN #IMPLIED<br>  
 * &nbsp;&nbsp;//see parentKey comment<br>
 * &nbsp;sequenceField NMTOKEN #IMPLIED<br> 
 * &nbsp;&nbsp;//field name, which will be filled by value from sequence<br>
 * &nbsp;&nbsp;//(can be used to generate new key field for relative records)<br> 
 * &nbsp;sequenceId NMTOKEN #IMPLIED<br>    
 * &nbsp;&nbsp;//we can supply sequence id used to fill a field defined in a sequenceField attribute<br>
 * &nbsp;&nbsp;//(if this attribute is omitted, non-persistent PrimitiveSequence will be used)<br>
 * &nbsp;xmlFields NMTOKEN #IMPLIED<br>     
 * &nbsp;&nbsp;//comma separated xml element names, which will be mapped on appropriate record fields<br>
 * &nbsp;&nbsp;//defined in cloverFields attribute<br>
 * &nbsp;cloverFields NMTOKEN #IMPLIED<br>  
 * &nbsp;&nbsp;//see xmlFields comment<br>
 * &gt;<br>
 * </code> All nested XML elements will be recognized as record fields and mapped by name (except elements serviced by
 * other nested Mapping elements), if you prefer other mapping xml fields and clover fields than 'by name', use
 * xmlFields and cloveFields attributes to setup custom fields mapping. 'useNestedNodes' component attribute defines if
 * also child of nested xml elements will be mapped on the current clover record. Record from nested Mapping element
 * could be connected via key fields with parent record produced by parent Mapping element (see parentKey and
 * generatedKey attribute notes). In case that fields are unsuitable for key composing, extractor could fill one or more
 * fields with values coming from sequence (see sequenceField and sequenceId attribute).
 * 
 * For example: given a xml file:<br>
 * <code>
 * &lt;myXML&gt; <br>
 * &nbsp;&lt;phrase&gt; <br>
 * &nbsp;&nbsp;&lt;text&gt;hello&lt;/text&gt; <br>
 * &nbsp;&nbsp;&lt;localization&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;chinese&gt;how allo yee dew ying&lt;/chinese&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;german&gt;wie gehts&lt;/german&gt; <br>
 * &nbsp;&nbsp;&lt;/localization&gt; <br>
 * &nbsp;&lt;/phrase&gt; <br>
 * &nbsp;&lt;locations&gt; <br>
 * &nbsp;&nbsp;&lt;location&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;name&gt;Stormwind&lt;/name&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;description&gt;Beautiful European architecture with a scenic canal system.&lt;/description&gt; <br>
 * &nbsp;&nbsp;&lt;/location&gt; <br>
 * &nbsp;&nbsp;&lt;location&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;name&gt;Ironforge&lt;/name&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;description&gt;Economic capital of the region with a high population density.&lt;/description&gt; <br>
 * &nbsp;&nbsp;&lt;/location&gt; <br>
 * &nbsp;&lt;/locations&gt; <br>
 * &nbsp;&lt;someUselessElement&gt;...&lt;/someUselessElement&gt; <br>
 * &nbsp;&lt;someOtherUselessElement/&gt; <br>
 * &nbsp;&lt;phrase&gt; <br>
 * &nbsp;&nbsp;&lt;text&gt;bye&lt;/text&gt; <br>
 * &nbsp;&nbsp;&lt;localization&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;chinese&gt;she yee lai ta&lt;/chinese&gt; <br>
 * &nbsp;&nbsp;&nbsp;&lt;german&gt;aufweidersehen&lt;/german&gt; <br>
 * &nbsp;&nbsp;&lt;/localization&gt; <br>
 * &nbsp;&lt;/phrase&gt; <br>
 * &lt;/myXML&gt; <br>
 * </code> Suppose we want to pull out "phrase" as one datarecord, "localization" as another datarecord, and "location"
 * as the final datarecord and ignore the useless elements. First we define the metadata for the records. Then create
 * the following mapping in the graph: <br>
 * <code>
 * &lt;node id="myId" type="com.lrn.etl.job.component.XMLExtract"&gt; <br>
 * &nbsp;&lt;attr name="mapping"&gt;<br>
 * &nbsp;&nbsp;&lt;Mapping element="phrase" outPort="0" sequenceField="id"&gt;<br>
 * &nbsp;&nbsp;&nbsp;&lt;Mapping element="localization" outPort="1" parentKey="id" generatedKey="parent_id"/&gt;<br>
 * &nbsp;&nbsp;&lt;/Mapping&gt; <br>
 * &nbsp;&nbsp;&lt;Mapping element="location" outPort="2"/&gt;<br>
 * &nbsp;&lt;/attr&gt;<br>
 * &lt;/node&gt;<br>
 * </code> Port 0 will get the DataRecords:<br>
 * 1) id=1, text=hello<br>
 * 2) id=2, text=bye<br>
 * Port 1 will get:<br>
 * 1) parent_id=1, chinese=how allo yee dew ying, german=wie gehts<br>
 * 2) parent_id=2, chinese=she yee lai ta, german=aufwiedersehen<br>
 * Port 2 will get:<br>
 * 1) name=Stormwind, description=Beautiful European architecture with a scenic canal system.<br>
 * 2) name=Ironforge, description=Economic capital of the region with a high population density.<br>
 * <hr>
 * Issue: Enclosing elements having values are not supported.<br>
 * i.e. <br>
 * <code>
 *   &lt;x&gt; <br>
 *     &lt;y&gt;z&lt;/y&gt;<br>
 *     xValue<br>
 *   &lt;/x&gt;<br>
 * </code> there will be no column x with value xValue.<br>
 * Issue: Namespaces are not considered.<br>
 * i.e. <br>
 * <code>
 *   &lt;ns1:x&gt;xValue&lt;/ns1:x&gt;<br>
 *   &lt;ns2:x&gt;xValue2&lt;/ns2:x&gt;<br>
 * </code> will be considered the same x.
 * 
 * @author KKou
 */
public class XMLExtract extends Node {

	public final static String COMPONENT_TYPE = "XML_EXTRACT";

	private static final Log LOGGER = LogFactory.getLog(XMLExtract.class);

	// xml attributes
	public static final String XML_SOURCEURI_ATTRIBUTE = "sourceUri";
	public static final String XML_USENESTEDNODES_ATTRIBUTE = "useNestedNodes";
	public static final String XML_MAPPING_ATTRIBUTE = "mapping"; 
	public static final String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRows";
	private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
	private static final String XML_TRIM_ATTRIBUTE = "trim";
	private static final String XML_VALIDATE_ATTRIBUTE = "validate";
	private static final String XML_XML_FEATURES_ATTRIBUTE = "xmlFeatures";
	public static final String XML_NAMESPACE_BINDINGS_ATTRIBUTE = "namespaceBindings";

	// from which input port to read
	private final static int INPUT_PORT = 0;

	// Where the XML comes from
	private InputSource m_inputSource;

	// input file
	private String inputFile;
	private ReadableChannelIterator readableChannelIterator;

	private String mapping;
	private String mappingURL;
	private String charset = null;

	private NodeList mappingNodes;

	private XmlSaxParser parser = new XmlSaxParser(null, this);

	// autofilling support
	private AutoFilling autoFilling = parser.getAutoFilling();

	/**
	 * Constructs an XML Extract node with the given id.
	 */
	public XMLExtract(String id) {
		super(id);
	}

	/**
	 * Creates an inctence of this class from a xml node.
	 * 
	 * @param graph
	 * @param xmlElement
	 * @return
	 * @throws XMLConfigurationException
	 * @throws AttributeNotFoundException
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException,
			AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		XMLExtract extract;

		// constructor
		extract = new XMLExtract(xattribs.getString(XML_ID_ATTRIBUTE));

		// set input file
		extract.setInputFile(xattribs.getStringEx(XML_SOURCEURI_ATTRIBUTE, null, RefResFlag.URL));

		extract.setUseNestedNodes(xattribs.getBoolean(XML_USENESTEDNODES_ATTRIBUTE, true));

		// set mapping
		String mappingURL = xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, null, RefResFlag.URL);
		String mapping = xattribs.getString(XML_MAPPING_ATTRIBUTE, null);
		NodeList nodes = xmlElement.getChildNodes();
		if (mappingURL != null)
			extract.setMappingURL(mappingURL);
		else if (mapping != null)
			extract.setMapping(mapping);
		else if (nodes != null && nodes.getLength() > 0) {
			// old-fashioned version of mapping definition
			// mapping xml elements are child nodes of the component
			extract.setNodes(nodes);
		}

		// set namespace bindings attribute
		if (xattribs.exists(XML_NAMESPACE_BINDINGS_ATTRIBUTE)) {
			Properties props = null;
			try {
				props = new Properties();
				final String content = xattribs.getString(XML_NAMESPACE_BINDINGS_ATTRIBUTE, null);
				if (content != null) {
					props.load(new StringReader(content));
				}
			} catch (IOException e) {
				throw new XMLConfigurationException("Unable to initialize namespace bindings", e);
			}

			final HashMap<String, String> namespaceBindings = new HashMap<String, String>();
			for (String name : props.stringPropertyNames()) {
				namespaceBindings.put(name, props.getProperty(name));
			}

			extract.setNamespaceBindings(namespaceBindings);
		}

		// set a skip row attribute
		if (xattribs.exists(XML_SKIP_ROWS_ATTRIBUTE)) {
			extract.setSkipRows(xattribs.getInteger(XML_SKIP_ROWS_ATTRIBUTE));
		}

		// set a numRecord attribute
		if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)) {
			extract.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
		}

		if (xattribs.exists(XML_XML_FEATURES_ATTRIBUTE)) {
			extract.setXmlFeatures(xattribs.getString(XML_XML_FEATURES_ATTRIBUTE));
		}
		if (xattribs.exists(XML_VALIDATE_ATTRIBUTE)) {
			extract.setValidate(xattribs.getBoolean(XML_VALIDATE_ATTRIBUTE));
		}
		if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
			extract.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
		}

		if (xattribs.exists(XML_TRIM_ATTRIBUTE)) {
			extract.setTrim(xattribs.getBoolean(XML_TRIM_ATTRIBUTE));
		}
		return extract;
	}

	/**
	 * Perform sanity checks.
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized())
			return;
		super.init();

		parser.setGraph(getGraph());
		parser.init();
	}

	@Override
	public synchronized void free() {
		try {
			ReadableChannelIterator.free(readableChannelIterator);
		} finally {
			super.free();
		}
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();

		if (firstRun()) {
			// sets input file to readableChannelIterator and sets its settings (directory, charset, input port,...)
			if (inputFile != null) {
				createReadableChannelIterator();
				this.readableChannelIterator.init();
			}
		} else {
			autoFilling.reset();
			this.readableChannelIterator.reset();
		}

		if (!readableChannelIterator.isGraphDependentSource()) {
			try {
				nextSource();
			} catch (JetelException e) {
				throw new ComponentNotReadyException(e);
			}
		}
	}

	private void createReadableChannelIterator() throws ComponentNotReadyException {
		TransformationGraph graph = getGraph();
		URL projectURL = getContextURL();

		this.readableChannelIterator = new ReadableChannelIterator(getInputPort(INPUT_PORT), projectURL, inputFile);
		this.readableChannelIterator.setCharset(charset);
		this.readableChannelIterator.setPropertyRefResolver(getPropertyRefResolver());
		this.readableChannelIterator.setDictionary(graph.getDictionary());
		this.readableChannelIterator.setPreferredDataSourceType(DataSourceType.STREAM);
	}

	@Override
	public Result execute() throws Exception {
		try {
			// prepare next source
			if (readableChannelIterator.isGraphDependentSource()) {
				nextSource();
			}
			do {
				if (m_inputSource != null) {
					parser.setInputRecord(this.readableChannelIterator.getCurrenRecord());
					parser.parse(m_inputSource);
				}
			} while (nextSource());

		} catch (SAXException ex) {
			// process error
			if (!runIt()) {
				return runIt ? Result.FINISHED_OK : Result.ABORTED; // we were stopped by a stop signal... probably
			}
			throw new JetelException("SAX parsing exception", ex);
		} finally {
			broadcastEOF();
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;

	}
	
    @Override
    public void postExecute() throws ComponentNotReadyException {
    	try {
    		super.postExecute();
    		if (m_inputSource != null) {
    			org.apache.commons.io.IOUtils.closeQuietly(m_inputSource.getByteStream());
    		}
    	} finally {
    		ReadableChannelIterator.postExecute(readableChannelIterator);
    	}
    }

	/**
	 * Switch to the next source file.
	 * 
	 * @return
	 * @throws JetelException
	 */
	private boolean nextSource() throws JetelException {
		Object input = null;
		InputStream is = null;
		while (readableChannelIterator.hasNext()) {
			autoFilling.resetSourceCounter();
			autoFilling.resetGlobalSourceCounter();
			input = readableChannelIterator.next();
			if (input == null)
				continue; // if record no record found
			if (input instanceof InputStream) {
				is = (InputStream) input;
			} else if (input instanceof ReadableByteChannel) {
				is = Channels.newInputStream((ReadableByteChannel) input);
			}
			autoFilling.setFilename(readableChannelIterator.getCurrentFileName());
			long fileSize = 0;
			Date fileTimestamp = null;
			if (autoFilling.getFilename() != null && !readableChannelIterator.isGraphDependentSource()) {
				try {
					File tmpFile = FileUtils.getJavaFile(getGraph().getRuntimeContext().getContextURL(), autoFilling.getFilename());
					long timestamp = tmpFile.lastModified();
					fileTimestamp = timestamp == 0 ? null : new Date(timestamp);
					fileSize = tmpFile.length();
				} catch (Exception e) {
					// do nothing - the url is not regular file
				}
			}
			autoFilling.setFileSize(fileSize);
			autoFilling.setFileTimestamp(fileTimestamp);

			String url = autoFilling.getFilename();
			m_inputSource = new InputSource(this.handleBOM(is, url));
			if (charset != null) {
				m_inputSource.setEncoding(charset); // CLO-6692: override encoding detection
			}
			XmlUtils.setSystemId(m_inputSource, getContextURL(), url);
			return true;
		}
		readableChannelIterator.blankRead();
		return false;
	}

	private InputStream handleBOM(InputStream is, String fileName) throws JetelException {
		if (charset == null) {
			return is;
		}
		PushbackInputStream pushbackInputStream = new PushbackInputStream(is, 4);

		try {
			byte[] bom = new byte[4];
			int read = pushbackInputStream.read(bom);
			int unread = 0;

			String warnEncoding = null;

			if (read == 4) {
				int[] unsigned = new int[read];
				for (int i = 0; i < read; i++) {
					unsigned[i] = bom[i] & 0xFF;
				}
				
				if (unsigned[0] == 0xFF && unsigned[1] == 0xFE && unsigned[2] == 0x00 && unsigned[3] == 0x00) {
					// unsigned.UTF_32_LE;
					if (!"UTF-32LE".equals(this.charset)) {
						warnEncoding = "UTF-32LE";
					}
					unread = 0;
				} else if (unsigned[0] == 0x00 && unsigned[1] == 0x00 && unsigned[2] == 0xFE && unsigned[3] == 0xFF) {
					// unsigned.UTF_32_BE;
					if (!"UTF-32BE".equals(this.charset)) {
						warnEncoding = "UTF-32BE";
					}
					unread = 0;
				} else if (unsigned[0] == 0xEF && unsigned[1] == 0xBB && unsigned[2] == 0xBF) {
					// unsigned.UTF_8;
					if (!"UTF-8".equals(this.charset)) {
						warnEncoding = "UTF-8";
					}
					unread = 1;
				} else if (unsigned[0] == 0xFF && unsigned[1] == 0xFE) {
					// unsigned.UTF_16_LE;
					if (!"UTF-16LE".equals(this.charset)) {
						warnEncoding = "UTF-16LE";
					}
					unread = 2;
				} else if (unsigned[0] == 0xFE && unsigned[1] == 0xFF) {
					// unsigned.UTF_16_BE;
					if (!"UTF-16BE".equals(this.charset)) {
						warnEncoding = "UTF-16BE";
					}
					unread = 2;
				} else {
					unread = 4;
				}
			} else if (read > 0) {
				unread = read;
			}

			if (warnEncoding != null) {
				LOGGER.warn("Byte Order Mark indicates " + warnEncoding + " but charset attribute is set to " + this.charset + (fileName != null ? " (file:" + fileName + ")" : ""));
			}

			if (unread > 0) {
				pushbackInputStream.unread(bom, bom.length - unread, unread);
			}
		} catch (IOException e) {
			throw new JetelException("Error during BOM detection.", e);
		}
		
		return pushbackInputStream;

	}

	@Override
	public String[] getUsedUrls() {
		return new String[] { inputFile };
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		if (checkPorts(status)) {
			return status;
		}

		if (charset != null && !Charset.isSupported(charset)) {
			status.addError(this, XML_CHARSET_ATTRIBUTE, "Charset " + charset + " not supported!");
		}

		TransformationGraph graph = getGraph();
		URL contextURL = getContextURL();
		// Check whether XML mapping schema is valid
		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			DefaultHandler handler = new MyHandler();
			InputSource is = null;
			Document doc = null;
			if (this.mappingURL != null) {
				InputStream inputStream = FileUtils.getInputStream(contextURL, mappingURL);
				is = new InputSource(inputStream);
				ReadableByteChannel ch = FileUtils.getReadableChannel(contextURL, mappingURL);
				doc = XmlUtils.createDocumentFromChannel(ch);
			} else if (this.mapping != null) {
				// inlined mapping
				// don't use the charset of the component's input files, but the charset of the .grf file
				is = new InputSource(new StringReader(mapping));
				doc = XmlUtils.createDocumentFromString(mapping);
			}
			if (is != null) {
				saxParser.parse(is, handler);
				Set<String> attributesNames = ((MyHandler) handler).getAttributeNames();
				for (String attributeName : attributesNames) {
					if (!isXMLAttribute(attributeName)) {
						status.addWarning(this, null, "Can't resolve XML attribute: " + attributeName);
					}
				}
			}
			if (doc != null) {
				Element rootElement = doc.getDocumentElement();
				mappingNodes = rootElement.getChildNodes();
				parser.setMappingNodes(mappingNodes);

				for (int i = 0; i < mappingNodes.getLength(); i++) {
					org.w3c.dom.Node node = mappingNodes.item(i);
					List<String> errors = parser.processMappings(graph, node);
					for (String error : errors) {
						status.addWarning(this, null, "Mapping error - " + error);
					}
				}
			}
		} catch (Exception e) {
			status.addError(this, null, "Can't parse XML mapping schema.", e);
		} finally {
			parser.reset();
		}

		// TODO Labels:
		// for (Mapping mapping: getMappings().values()) {
		// checkUniqueness(status, mapping);
		// }
		// TODO Labels end

		try {
			// check inputs
			if (inputFile != null) {
				createReadableChannelIterator();
				this.readableChannelIterator.checkConfig();

				String fName = null;
				Iterator<Input> fit = readableChannelIterator.getInputIterator();
				while (fit.hasNext()) {
					try {
						Input input = fit.next();
						fName = input.getAbsolutePath();
						try (ReadableByteChannel channel = (ReadableByteChannel) input.getPreferredInput(DataSourceType.CHANNEL)) {
							// do nothing, just close the channel
						}
					} catch (IOException e) {
						throw new ComponentNotReadyException("File is unreachable: " + fName, e);
					}
				}
			}
		} catch (ComponentNotReadyException e) {
			status.addWarning(this, null, e);
		} finally {
			free();
		}

		return status;
	}

	private boolean checkPorts(ConfigurationStatus status) {
		return !checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 0, Integer.MAX_VALUE);
	}

	private boolean isXMLAttribute(String attribute) {
		return attribute.equals(XmlSaxParser.XML_ELEMENT) || attribute.equals(XmlSaxParser.XML_OUTPORT) || attribute.equals(XmlSaxParser.XML_PARENTKEY) || attribute.equals(XmlSaxParser.XML_GENERATEDKEY) || attribute.equals(XmlSaxParser.XML_XMLFIELDS) || attribute.equals(XmlSaxParser.XML_CLOVERFIELDS) || attribute.equals(XmlSaxParser.XML_SEQUENCEFIELD) || attribute.equals(XmlSaxParser.XML_SEQUENCEID) || attribute.equals(XmlSaxParser.XML_TEMPLATE_ID) || attribute.equals(XmlSaxParser.XML_TEMPLATE_REF) || attribute.equals(XmlSaxParser.XML_TEMPLATE_DEPTH) || attribute.equals(XML_SKIP_ROWS_ATTRIBUTE) || attribute.equals(XML_NUMRECORDS_ATTRIBUTE) || attribute.equals(XML_TRIM_ATTRIBUTE) || attribute.equals(XML_VALIDATE_ATTRIBUTE) || attribute.equals(XML_XML_FEATURES_ATTRIBUTE) || attribute.equals(XmlSaxParser.XML_USE_PARENT_RECORD) || attribute.equals(XmlSaxParser.XML_IMPLICIT) || attribute.equals(XmlSaxParser.XML_INPUTFIELD) || attribute.equals(XmlSaxParser.XML_OUTPUTFIELD);
	}

	/**
	 * Set the input source containing the XML this will parse.
	 */
	public void setInputSource(InputSource inputSource) {
		this.m_inputSource = inputSource;
	}

	public void setInputFile(String inputFile) {
		this.inputFile = inputFile;
	}

	public void setUseNestedNodes(boolean useNestedNodes) {
		parser.setUseNestedNodes(useNestedNodes);
	}

	/**
	 * Sets skipRows - how many elements to skip.
	 * 
	 * @param skipRows
	 */
	public void setSkipRows(int skipRows) {
		parser.setSkipRows(skipRows);
	}

	/**
	 * Sets numRecords - how many elements to process.
	 * 
	 * @param numRecords
	 */
	public void setNumRecords(int numRecords) {
		parser.setNumRecords(numRecords);
	}

	public void setXmlFeatures(String xmlFeatures) {
		parser.setXmlFeatures(xmlFeatures);
	}

	public void setValidate(boolean validate) {
		parser.setValidate(validate);
	}

	/**
	 * Sets charset for dictionary and input port reading.
	 * 
	 * @param string
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 * Sets namespace bindings to allow processing that relate namespace prefix used in Mapping and namespace URI used
	 * in processed XML document
	 * 
	 * @param namespaceBindings
	 *            the namespaceBindings to set
	 */
	private void setNamespaceBindings(HashMap<String, String> namespaceBindings) {
		parser.setNamespaceBindings(namespaceBindings);
	}

	@Deprecated
	private void setNodes(NodeList nodes) {
		this.mappingNodes = nodes;
		parser.setMappingNodes(nodes);
	}

	public void setMappingURL(String mappingURL) {
		this.mappingURL = mappingURL;
		parser.setMappingURL(mappingURL);
	}

	public void setMapping(String mapping) {
		this.mapping = mapping;
		parser.setMapping(mapping);
	}

	/**
	 * Sets the trim indicator.
	 * 
	 * @param trim
	 */
	public void setTrim(boolean trim) {
		parser.setTrim(trim);
	}
}
