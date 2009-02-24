package org.jetel.component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.parser.MultiLevelParser;
import org.jetel.data.parser.MultiLevelSelector;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MultiFileReader;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.compile.DynamicJavaCode;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * <h3>MultiLevelReader Component</h3> <!-- This component reads data records
 * from heterogenous flat files -->
 * 
 * <table border="1">
 * <tr>
 * <th colspan="2">Component:</th>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Name:</i></h4></td>
 * <td>MultiLevelReader</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Category:</i></h4></td>
 * <td>Readers</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Description:</i></h4></td>
 * <td>This component reads data records from heterogenous flat files</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Inputs:</i></h4></td>
 * <td>none</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Outputs:</i></h4></td>
 * <td>One output port for each data type found in the flat file</td>
 * </tr>
 * <tr>
 * <td>
 * <h4><i>Comment:</i></h4></td>
 * <td>-</td>
 * </tr>
 * </table>
 * <br>
 * <table border="1">
 * <tr>
 * <th colspan="2">XML attributes:</th>
 * </tr>
 * <tr>
 * <td><b>type</b></td>
 * <td>"MULTI_LEVEL_READER"</td>
 * </tr>
 * <tr>
 * <td><b>id</b></td>
 * <td>component identification</td>
 * <tr>
 * <td><b>fileURL</b></td>
 * <td>path to input file</td>
 * <tr>
 * <td><b>charset</b></td>
 * <td>Character encoding of the input file</td>
 * <tr>
 * <td><b>dataPolicy</b></td>
 * <td>specifies how to handle misformatted or incorrect data. 'Strict' aborts
 * processing, 'Controlled' logs the entire record while processing continues,
 * and 'Lenient' attempts to set incorrect data to default values while
 * processing continues.</td>
 * <tr>
 * <td><b>skipRows</b></td>
 * <td>Number of records from beginning to skip</td>
 * <tr>
 * <td><b>numRecords</b></td>
 * <td>Maximum number of records to read</td>
 * <tr>
 * <td><b>selectorClass</b></td>
 * <td>pre-loaded full class name of a class implementing the MultiLevelSelector
 * interface</td>
 * <tr>
 * <td><b>selectorCode</b></td>
 * <td>in-line Java code of a class implementing the MultiLevelSelector
 * interface</td>
 * <tr>
 * <td><b>selectorURL</b></td>
 * <td>URL of a Java class implementing the MultiLevelSelector interface</td>
 * <tr>
 * <td><b>selectorProperties</b></td>
 * <td>Selector-specific properties passed to the plugged-in selector</td>
 * </table>
 * 
 * @author pnajvar
 * 
 */
public class MultiLevelReader extends Node {

	private static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private final static String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	private static final String XML_SKIP_ROWS_ATTRIBUTE = "skipRows";
	private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
	private static final String XML_SELECTORCLASS_ATTRIBUTE = "selectorClass";
	private static final String XML_SELECTORCODE_ATTRIBUTE = "selectorCode";
	private static final String XML_SELECTORURL_ATTRIBUTE = "selectorURL";
	private static final String XML_SELECTOR_PROPERTIES_ATTRIBUTE = "selectorProperties";

	static Log logger = LogFactory.getLog(MultiLevelReader.class);

	public final static String COMPONENT_TYPE = "MULTI_LEVEL_READER";

	public static final String DEFAULT_SELECTOR_CLASS = "org.jetel.data.parser.PrefixMultiLevelSelector";

	private String fileURL;
	private MultiLevelParser parser;
	private MultiFileReader reader;
	private PolicyType policyType;
	private int skipRows = 0; // do not skip rows by default
	private boolean skipFirstLine = false;
	private int numRecords = -1;
	private String charset;
	private String selectorCode;
	private String selectorClass;
	private String selectorURL;
	private MultiLevelSelector selector;
	private Properties selectorProperties;
	private OutputPort[] output;

	public MultiLevelReader(String id, TransformationGraph graph) {
		super(id, graph);
	}

	public MultiLevelReader(String id, String fileURL, String charset,
			String dataPolicy, int skipRows, int numRecords, String selectorCode,
			String selectorClass, String selectorURL, Properties selectorProperties) {
		super(id);
		this.fileURL = fileURL;
		this.charset = charset;
		this.policyType = PolicyType.valueOfIgnoreCase(dataPolicy);
		this.skipRows = skipRows;
		this.numRecords = numRecords;
		this.selectorCode = selectorCode;
		this.selectorClass = selectorClass;
		this.selectorURL = selectorURL;
		this.selectorProperties = selectorProperties;
	}

	@Override
	public Result execute() throws Exception {
		DataRecord record;
		try {
			while (runIt) {
				try {
					// try to get a record into the reader
					if ((record = reader.getNext(null)) == null) { // no more
																	// records
						break;
					}
					// send it to appropriate port
					output[parser.getTypeIdx()].writeRecord(record);
				} catch (BadDataFormatException bdfe) {
					if (policyType == PolicyType.STRICT) {
						throw bdfe;
					} else {
						logger.info(bdfe.getMessage());
					}
				}
				SynchronizeUtils.cloverYield();
			}
		} catch (Exception e) {
			throw e;
		} finally {
			broadcastEOF();
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.graph.Node#reset()
	 */
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		reader.reset();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.jetel.graph.GraphElement#free()
	 */
	@Override
	public synchronized void free() {
		super.free();
		if (reader != null) {
			reader.close();
		}
	}

	/**
	 * Instantiates selector class by its name
	 * 
	 * @param denormClass
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private MultiLevelSelector createSelector(String selectorClass)
			throws ComponentNotReadyException {
		MultiLevelSelector selector;
		try {
			selector = (MultiLevelSelector) Class.forName(selectorClass)
					.newInstance();
		} catch (InstantiationException ex) {
			throw new ComponentNotReadyException(
					"Can't instantiate selector class: " + ex.getMessage());
		} catch (IllegalAccessException ex) {
			throw new ComponentNotReadyException(
					"Can't instantiate selector class: " + ex.getMessage());
		} catch (ClassNotFoundException ex) {
			throw new ComponentNotReadyException(
					"Can't find specified selector class: " + selectorClass);
		}
		return selector;
	}

	/**
	 * Instantiates selector class from source code in a string
	 * 
	 * @param selectorCode
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private MultiLevelSelector createSelectorDynamic(String selectorCode)
			throws ComponentNotReadyException {
		DynamicJavaCode dynCode = new DynamicJavaCode(selectorCode, this
				.getClass().getClassLoader());
		logger.info(" (compiling dynamic source) ");
		// use DynamicJavaCode to instantiate transformation class
		Object transObject = null;
		try {
			transObject = dynCode.instantiate();
		} catch (RuntimeException ex) {
			logger.debug(dynCode.getCompilerOutput());
			logger.debug(dynCode.getSourceCode());
			throw new ComponentNotReadyException(
					"Type selector code is not compilable.\n" + "Reason: "
							+ ex.getMessage());
		}
		if (transObject instanceof MultiLevelSelector) {
			return (MultiLevelSelector) transObject;
		} else {
			throw new ComponentNotReadyException(
					"Provided type selector class doesn't implement TypeSelector.");
		}
	}

	/**
	 * 
	 * @exception ComponentNotReadyException
	 *                Description of the Exception
	 * @since April 4, 2002
	 */
	public void init() throws ComponentNotReadyException {
		if (isInitialized())
			return;
		super.init();

		if (selector == null && selectorCode != null) {
			selector = createSelectorDynamic(selectorCode);
		} else {
			selector = createSelector(selectorClass);
		}
		parser = new MultiLevelParser(charset, selector, getOutMetadata()
				.toArray(new DataRecordMetadata[0]), selectorProperties);
		parser.setExceptionHandler(ParserExceptionHandlerFactory
				.getHandler(policyType));

		// initialize multifile reader based on prepared parser
		TransformationGraph graph = getGraph();
		reader = new MultiFileReader(parser, graph != null ? graph
				.getProjectURL() : null, fileURL);
		reader.setLogger(logger);
		reader.setFileSkip(skipFirstLine ? 1 : 0);
		reader.setSkip(skipRows);
		reader.setNumRecords(numRecords);
		reader.setInputPort(getInputPort(INPUT_PORT)); // for port protocol:
														// ReadableChannelIterator
														// reads data
		reader.setCharset(charset);
		reader.setDictionary(graph.getDictionary());
		reader.init(null);
		output = (OutputPort[]) getOutPorts().toArray(new OutputPort[0]);
	}

	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE, this.fileURL);

		// if (this.parser.getCharsetName() != null) {
		// xmlElement.setAttribute(XML_CHARSET_ATTRIBUTE,
		// this.parser.getCharsetName());
		// }
		//		
		// if (this.skipRows>0){
		// xmlElement.setAttribute(XML_SKIP_ROWS_ATTRIBUTE,
		// String.valueOf(skipRows));
		// }
		if (policyType != null) {
			xmlElement.setAttribute(XML_DATAPOLICY_ATTRIBUTE, policyType
					.toString());
		}

		if (!DEFAULT_SELECTOR_CLASS.equals(this.selectorClass)) {
			xmlElement.setAttribute(XML_SELECTORCLASS_ATTRIBUTE,
					this.selectorClass);
		}

	}

	/**
	 * Description of the Method
	 * 
	 * @param nodeXML
	 *            Description of Parameter
	 * @return Description of the Returned Value
	 * @since May 21, 2002
	 */
	public static Node fromXML(TransformationGraph graph, Element xmlElement)
			throws XMLConfigurationException {
		MultiLevelReader reader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(
				xmlElement, graph);

		try {
			String selectorCode = null;
			if (! xattribs.exists(XML_SELECTORCLASS_ATTRIBUTE)) {
				if (xattribs.exists(XML_SELECTORURL_ATTRIBUTE)) {
					selectorCode = xattribs
							.resolveReferences(FileUtils.getStringFromURL(graph
									.getProjectURL(), xattribs
									.getString(XML_SELECTORURL_ATTRIBUTE), "utf-8"));
				} else if (xattribs.exists(XML_SELECTORCODE_ATTRIBUTE)) {
					selectorCode = xattribs.getString(XML_SELECTORCODE_ATTRIBUTE);
				}
			}

			Properties selectorProperties = null;
			if (xattribs.exists(XML_SELECTOR_PROPERTIES_ATTRIBUTE)) {
				try {
					selectorProperties = new Properties();
					String stringProperties = xattribs.getString(
							XML_SELECTOR_PROPERTIES_ATTRIBUTE, null);
					if (stringProperties != null) {
						selectorProperties.load(new ByteArrayInputStream(
								stringProperties.getBytes()));
					}
				} catch (IOException e) {
					logger.error(e);
				}
			}

			reader = new MultiLevelReader(
					xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_FILEURL_ATTRIBUTE, ""),
					xattribs.getString(XML_CHARSET_ATTRIBUTE, null),
					xattribs.getString(XML_DATAPOLICY_ATTRIBUTE, null),
					xattribs.getInteger(XML_SKIP_ROWS_ATTRIBUTE, 0),
					xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE, 0),
					selectorCode,
					xattribs.getString(XML_SELECTORCLASS_ATTRIBUTE, DEFAULT_SELECTOR_CLASS),
					xattribs.getString(XML_SELECTORURL_ATTRIBUTE, null),
					selectorProperties);

		} catch (Exception ex) {
			ex.printStackTrace();
			throw new XMLConfigurationException(COMPONENT_TYPE + ":"
					+ xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ")
					+ ":" + ex.getMessage(), ex);
		}

		return reader;
	}

	/**
	 * Adds BadDataFormatExceptionHandler to behave according to DataPolicy.
	 * 
	 * @param handler
	 */
	public void setExceptionHandler(IParserExceptionHandler handler) {
		parser.setExceptionHandler(handler);
	}

	/** Description of the Method */
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 0, 1)
				|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}

		try {
			init();
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(e
					.getMessage(), ConfigurationStatus.Severity.ERROR, this,
					ConfigurationStatus.Priority.NORMAL);
			if (!StringUtils.isEmpty(e.getAttributeName())) {
				problem.setAttributeName(e.getAttributeName());
			}
			status.add(problem);
		} finally {
			free();
		}

		return status;
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

}
