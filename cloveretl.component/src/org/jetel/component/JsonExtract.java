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
import java.io.StringReader;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.data.parser.JsonSaxParser;
import org.jetel.data.parser.XmlSaxParser;
import org.jetel.data.parser.XmlSaxParser.MyHandler;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.AutoFilling;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.ReadableChannelIterator;
import org.jetel.util.XmlUtils;
import org.jetel.util.file.FileURLParser;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

/**
 * <h3>JSONExtract Component</h3>
 * 
 * <!-- Provides the logic to parse a JSON file and filter to different ports based on a matching element. The element
 * and all children will be turned into a Data record. JSONExtract is heavily based on XMLExtract as JSON data are essentially converted to XML and
 * then processed-->
 * 
 * <table border="1">
 * <th>Component:</th>
 * <tr>
 * <td>
 * <h4><i>Name:</i></h4></td>
 * <td>JSONExtract</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Category:</i></h4></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Description:</i></h4></td>
 * <td>Provides the logic to parse a JSON file and filter to different ports based on a matching element. The element and
 * all children will be turned into a Data record. This component is heavily basedon XMLExtract component as JSON data are esentially converted
 * to XML first and then parsed.</td>
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
 * <td>"JSON_EXTRACT"</td>
 * </tr>
 * <tr>
 * <td><b>id</b></td>
 * <td>component identification</td>
 * <tr>
 * <td><b>sourceUri</b></td>
 * <td>location of source JSON data to process</td>
 * <tr>
 * <td><b>useNestedNodes</b></td>
 * <td><b>true</b> if nested unmapped JSON elements will be used as data source; <b>false</b> if will be ignored</td>
 * <tr>
 * <td><b>mapping</b></td>
 * <td>&lt;mapping&gt;</td>
 * </tr>
 * </table>
 * <br>
 * Provides the logic to parse a JSON file and filter to different ports based on a matching element. The element and all
 * children will be turned into a Data record.<br>
 * Mapping attribute contains mapping hierarchy in XML form. DTD of mapping:<br>
 * <code><pre>
 * &lt;!ELEMENT Mappings (Mapping*)&gt;<br>
 * 
 * &lt;!ELEMENT Mapping (Mapping*)&gt;<br>
 * &lt;!ATTLIST Mapping<br>
 * &nbsp;element NMTOKEN #REQUIRED<br>      
 * &nbsp;&nbsp;//name of binded JSON element<br>  
 * &nbsp;outPort NMTOKEN #IMPLIED<br>      
 * &nbsp;&nbsp;//name of output port for this mapped JSON element<br>
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
 * &nbsp;&nbsp;//comma separated JSON element names, which will be mapped on appropriate record fields<br>
 * &nbsp;&nbsp;//defined in cloverFields attribute<br>
 * &nbsp;cloverFields NMTOKEN #IMPLIED<br>  
 * &nbsp;&nbsp;//see xmlFields comment<br>
 * &gt;</pre>
 * </code><p>All nested JSON elements will be recognized as record fields and mapped by name (except elements serviced by
 * other nested Mapping elements), if you prefer other mapping JSON fields and clover fields than 'by name', use
 * xmlFields and cloveFields attributes to setup custom fields mapping. 'useNestedNodes' component attribute defines if
 * also child of nested JSON elements will be mapped on the current clover record. Record from nested Mapping element
 * could be connected via key fields with parent record produced by parent Mapping element (see parentKey and
 * generatedKey attribute notes). In case that fields are unsuitable for key composing, extractor could fill one or more
 * fields with values coming from sequence (see sequenceField and sequenceId attribute).</p>
 * 
 * For example: given a JSON file:<br>
 * <code><pre>
 * {
 *   "firstName": "John",
 *   "lastName": "Smith",
 *   "age": 25,
 *   "address": {
 *       "streetAddress": "21 2nd Street",
 *       "city": "New York",
 *       "state": "NY",
 *       "postalCode": 10021
 *   },
 *   "phoneNumbers": [
 *       {
 *           "type": "home",
 *           "number": "212 555-1234"
 *       },
 *       {
 *           "type": "fax",
 *           "number": "646 555-4567"
 *       }
 *   ]
 * }
 * </pre></code> <p>Suppose we want to pull out a person data (first name, last name, age, address) as one datarecord, "phoneNumbers" as another datarecord(s) with
 * relation to parent - the primary record carrying person's identity.<br>
 * First we define metadata for the records. Then create
 * the following mapping in the graph:</p> <br>
 * <code><pre>
 * &lt;Mappings&gt;<br>
 *	&lt;Mapping element=&quot;json_object&quot; outPort=&quot;0&quot;<br>
 *			xmlFields=&quot;{}age;{}firstName;{}lastName&quot;<br>
 *			cloverFields=&quot;age;firstName;lastName&quot;&gt;<br>
 *		&lt;Mapping element=&quot;address&quot; useParentRecord=&quot;true&quot;<br>
 *				xmlFields=&quot;{}city;{}postalCode;{}state;{}streetAddress&quot;<br>
 *				cloverFields=&quot;city;postalCode;state;streetAddress&quot;&gt;<br>
 *		&lt;/Mapping&gt;<br>
 *		&lt;Mapping element=&quot;phoneNumbers&quot; outPort=&quot;1&quot; parentKey=&quot;firstName;lastName&quot; generatedKey=&quot;firstName;lastName&quot;<br>
 * 				xmlFields=&quot;{}number;{}type&quot;<br>
 *				cloverFields=&quot;number;type&quot;&gt;<br>
 *		&lt;/Mapping&gt;<br>
 *	&lt;/Mapping&gt;<br>
 * &lt;/Mappings&gt;<br>
 *
 * </pre></code> Port 0 will get the DataRecords:<br>
 * <code><pre>John;Smith;25;21 2nd Street;New York;NY;10021</pre></code>
 * Port 1 will get:<br>
 * <code><pre>home;212 555-1234;John;Smith;
 *fax;646 555-4567;John;Smith;</pre></code>
 * <hr>
 * @author dpavlis (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 */
public class JsonExtract extends Node {

	public final static String COMPONENT_TYPE = "JSON_EXTRACT";

	private static final Log LOGGER = LogFactory.getLog(JsonExtract.class);

	// xml attributes
	public static final String XML_SOURCEURI_ATTRIBUTE = "sourceUri";
	public static final String XML_USENESTEDNODES_ATTRIBUTE = "useNestedNodes";
	private static final String XML_MAPPING_ATTRIBUTE = "mapping";
	private final static String XML_MAPPING_URL_ATTRIBUTE = "mappingURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRows";
	private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
	private static final String XML_TRIM_ATTRIBUTE = "trim";
	
	// from which input port to read
	private final static int INPUT_PORT = 0;

	// Where the XML comes from
	private InputSource m_inputSource;

	// input file
	private String inputFile;
	private ReadableChannelIterator readableChannelIterator;

	private String mapping;
	private String mappingURL;
	private String charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;

	private NodeList mappingNodes;

	private XmlSaxParser parser = new XmlSaxParser(null, this);

	// autofilling support
	private AutoFilling autoFilling = parser.getAutoFilling();

	/**
	 * Constructs an XML Extract node with the given id.
	 */
	public JsonExtract(String id) {
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
	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		JsonExtract extract;

		// constructor
		extract = new JsonExtract(xattribs.getString(XML_ID_ATTRIBUTE));

		// set input file
		extract.setInputFile(xattribs.getStringEx(XML_SOURCEURI_ATTRIBUTE, RefResFlag.URL));

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
		} else {
			xattribs.getStringEx(XML_MAPPING_URL_ATTRIBUTE, RefResFlag.URL); // throw configuration
																								// exception
		}

		// set a skip row attribute
		if (xattribs.exists(XML_SKIP_ROWS_ATTRIBUTE)) {
			extract.setSkipRows(xattribs.getInteger(XML_SKIP_ROWS_ATTRIBUTE));
		}

		// set a numRecord attribute
		if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)) {
			extract.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
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
		parser.setParser(new JsonSaxParser());
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
		URL projectURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;

		this.readableChannelIterator = new ReadableChannelIterator(getInputPort(INPUT_PORT), projectURL, inputFile);
		this.readableChannelIterator.setCharset(charset);
		this.readableChannelIterator.setPropertyRefResolver(graph != null ? new PropertyRefResolver(graph.getGraphProperties()) : null);
		this.readableChannelIterator.setDictionary(graph.getDictionary());
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
		}

		broadcastEOF();
		return runIt ? Result.FINISHED_OK : Result.ABORTED;

	}

	/**
	 * Switch to the next source file.
	 * 
	 * @return
	 * @throws JetelException
	 */
	private boolean nextSource() throws JetelException {
		ReadableByteChannel stream = null;
		while (readableChannelIterator.hasNext()) {
			autoFilling.resetSourceCounter();
			autoFilling.resetGlobalSourceCounter();
			stream = readableChannelIterator.nextChannel();
			if (stream == null)
				continue; // if record no record found
			autoFilling.setFilename(readableChannelIterator.getCurrentFileName());
			long fileSize = 0;
			Date fileTimestamp = null;
			if (autoFilling.getFilename() != null
					&& !readableChannelIterator.isGraphDependentSource()) {
				try {
					File tmpFile = FileUtils.getJavaFile(getGraph().getRuntimeContext().getContextURL(), autoFilling.getFilename());
					long timestamp = tmpFile.lastModified();
					fileTimestamp = timestamp == 0 ? null : new Date(timestamp);
					fileSize = tmpFile.length();
				} catch (Exception e) {
					//do nothing - the url is not regular file
				}
			}
			autoFilling.setFileSize(fileSize);
			autoFilling.setFileTimestamp(fileTimestamp);				
			m_inputSource = new InputSource(Channels.newReader(stream, charset));
			return true;
		}
		readableChannelIterator.blankRead();
		return false;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		if (checkPorts(status)) {
			return status;
		}

		if (charset != null && !Charset.isSupported(charset)) {
			status.add(new ConfigurationProblem("Charset " + charset + " not supported!", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
		}

		TransformationGraph graph = getGraph();
		// Check whether XML mapping schema is valid
		try {
			SAXParserFactory factory = SAXParserFactory.newInstance();
			SAXParser saxParser = factory.newSAXParser();
			DefaultHandler handler = new MyHandler();
			InputSource is = null;
			Document doc = null;
			if (this.mappingURL != null) {
				InputStream inputStream = FileUtils.getInputStream(graph != null ? graph.getRuntimeContext().getContextURL() : null, mappingURL);
				is = new InputSource(inputStream);
				ReadableByteChannel ch = FileUtils.getReadableChannel(graph != null ? graph.getRuntimeContext().getContextURL() : null, mappingURL);
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
						status.add(new ConfigurationProblem("Can't resolve XML attribute: " + attributeName, Severity.WARNING, this, Priority.NORMAL));
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
					ConfigurationProblem problem;
					for (String error : errors) {
						problem = new ConfigurationProblem("Mapping error - " + error, Severity.WARNING, this, Priority.NORMAL);
						status.add(problem);
					}
				}
			}
		} catch (Exception e) {
			status.add(new ConfigurationProblem("Can't parse JSON mapping schema. Reason: " + ExceptionUtils.getMessage(e), Severity.ERROR, this, Priority.NORMAL));
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

				URL contextURL = graph != null ? graph.getRuntimeContext().getContextURL() : null;
				String fName = null;
				Iterator<String> fit = readableChannelIterator.getFileIterator();
				while (fit.hasNext()) {
					try {
						fName = fit.next();
						if (fName.equals("-"))
							continue;
						if (fName.startsWith("dict:"))
							continue; // this test has to be here, since an involuntary warning is caused
						String mostInnerFile = FileURLParser.getMostInnerAddress(fName);
						URL url = FileUtils.getFileURL(contextURL, mostInnerFile);
						if (FileUtils.isServerURL(url)) {
							// FileUtils.checkServer(url); //this is very long operation
							continue;
						}
						if (FileURLParser.isArchiveURL(fName)) {
							// test if the archive file exists
							// getReadableChannel is too long for archives
							String path = url.getRef() != null ? url.getFile() + "#" + url.getRef() : url.getFile();
							if (new File(path).exists())
								continue;
							throw new ComponentNotReadyException("File is unreachable: " + fName);
						}
						FileUtils.getReadableChannel(contextURL, fName).close();
					} catch (IOException e) {
						throw new ComponentNotReadyException("File is unreachable: " + fName, e);
					} catch (ComponentNotReadyException e) {
						throw new ComponentNotReadyException("File is unreachable: " + fName, e);
					}
				}
			}
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL);
			if (!StringUtils.isEmpty(e.getAttributeName())) {
				problem.setAttributeName(e.getAttributeName());
			}
			status.add(problem);
		} finally {
			free();
		}

		return status;
	}

	private boolean checkPorts(ConfigurationStatus status) {
		return !checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 0, Integer.MAX_VALUE);
	}

	private boolean isXMLAttribute(String attribute) {
		return attribute.equals(XmlSaxParser.XML_ELEMENT) || attribute.equals(XmlSaxParser.XML_OUTPORT) || attribute.equals(XmlSaxParser.XML_PARENTKEY) || attribute.equals(XmlSaxParser.XML_GENERATEDKEY) || attribute.equals(XmlSaxParser.XML_XMLFIELDS) || attribute.equals(XmlSaxParser.XML_CLOVERFIELDS) || attribute.equals(XmlSaxParser.XML_SEQUENCEFIELD) || attribute.equals(XmlSaxParser.XML_SEQUENCEID) || attribute.equals(XmlSaxParser.XML_TEMPLATE_ID) || attribute.equals(XmlSaxParser.XML_TEMPLATE_REF) || attribute.equals(XmlSaxParser.XML_TEMPLATE_DEPTH) || attribute.equals(XML_SKIP_ROWS_ATTRIBUTE) || attribute.equals(XML_NUMRECORDS_ATTRIBUTE) || attribute.equals(XML_TRIM_ATTRIBUTE) || attribute.equals(XmlSaxParser.XML_USE_PARENT_RECORD) 
				|| attribute.equals(XmlSaxParser.XML_IMPLICIT)|| attribute.equals(XmlSaxParser.XML_INPUTFIELD)|| attribute.equals(XmlSaxParser.XML_OUTPUTFIELD);
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
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
