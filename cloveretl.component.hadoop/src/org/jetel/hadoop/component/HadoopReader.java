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
package org.jetel.hadoop.component;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.database.IConnection;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.IGraphElement;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.hadoop.connection.HadoopConnection;
import org.jetel.hadoop.connection.HadoopURLUtils;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MultiFileReader;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * @author David Pavlis (info@cloveretl.com) (c) Javlin a.s. (www.javlin.eu)
 * 
 * @since
 */

public class HadoopReader extends Node {

	private static final String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	private static final String XML_CONNECTION_ID_ATTRIBUTE = "connectionId";
	public static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "skipRows";
	private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
	private static final String XML_SKIP_SOURCE_ROWS_ATTRIBUTE = "skipSourceRows";
	private static final String XML_NUM_SOURCE_RECORDS_ATTRIBUTE = "numSourceRecords";
	private static final String XML_INCREMENTAL_FILE_ATTRIBUTE = "incrementalFile";
	private static final String XML_INCREMENTAL_KEY_ATTRIBUTE = "incrementalKey";
	private static final String XML_KEY_FIELD_NAME_ATTRIBUTE = "keyField";
	private static final String XML_VALUE_FIELD_NAME_ATTRIBUTE = "valueField";

	/** Description of the Field */
	public final static String COMPONENT_TYPE = "HADOOP_READER";

	private static Log logger = LogFactory.getLog(HadoopReader.class);

	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	private MultiFileReader reader;
	private String policyTypeStr;
	private PolicyType policyType = PolicyType.STRICT;
	private String connectionId;
	private String fileURL;
	private int skipRows = -1; // do not skip rows by default
	private int numRecords = -1;
	private int skipSourceRows = -1;
	private int numSourceRecords = -1;
	private String incrementalFile;
	private String incrementalKey;

	private String keyFieldName;
	private String valueFieldName;

	private HadoopConnection connection;
	private IHadoopSequenceFileParser parser;

	/**
	 * Constructor for the DBFDataReader object
	 * 
	 * @param id
	 *            Description of the Parameter
	 * @param fileURL
	 *            Description of the Parameter
	 */
	public HadoopReader(String id, String fileURL, String keyFieldName, String valueFieldName) {
		super(id);
		this.fileURL = fileURL;
		this.keyFieldName = keyFieldName;
		this.valueFieldName = valueFieldName;
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		reader.preExecute();
	}

	@Override
	public Result execute() throws Exception {
		// we need to create data record - take the metadata from first output port
		DataRecord record = DataRecordFactory.newRecord(getOutputPort(OUTPUT_PORT).getMetadata());

		// till it reaches end of data or it is stopped from outside
		try {
			while (record != null && runIt) {
				try {
					if ((record = reader.getNext(record)) != null) {
						// broadcast the record to all connected Edges
						writeRecordBroadcast(record);
					}
				} catch (RuntimeException bdfe) {
					if (policyType == PolicyType.STRICT) {
						throw bdfe;
					} else {
						logger.info(bdfe);
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

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		
		reader.postExecute();
	}

	@Override
	public void commit() {
		super.commit();
		// TODO: storeValues();
	}

	@Override
	public synchronized void free() {
		super.free();
		try {
			if (reader != null) {
				reader.free();
				reader = null;
			}
			if (connection != null) {
				connection.free();
				connection = null;
			}
		} catch (Throwable t) {
			logger.warn("Resource releasing failed for '" + getId() + "'.", t);
		}
	}

	/**
	 * Stores all values as incremental reading.
	 */
	private void storeValues() {
		try {
			Object dictValue = getGraph().getDictionary().getValue(Defaults.INCREMENTAL_STORE_KEY);
			if (Boolean.FALSE.equals(dictValue)) {
				return;
			}
			reader.storeIncrementalReading();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Description of the Method
	 * 
	 * @exception ComponentNotReadyException
	 *                Description of the Exception
	 * @since April 4, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			return;
		}
		super.init();

		policyType = PolicyType.valueOfIgnoreCase(policyTypeStr);

		prepareMultiFileReader();
	}

	private void prepareConnection() throws ComponentNotReadyException {
		IConnection conn = prepareGraphConnection(connectionId, fileURL, "input", this, getGraph(), logger);
		this.connection = (HadoopConnection) conn;
	}

	static IConnection prepareGraphConnection(String connectionId, String fileURL, String fileURLAdjective, Node component, TransformationGraph graph, Log log) throws ComponentNotReadyException {
		IConnection conn = null;
		
		if (HadoopURLUtils.isHDFSUrl(fileURL)) {
			URL url;
			try {
				url = FileUtils.getFileURL(fileURL);
			} catch (MalformedURLException e) {
				throw new ComponentNotReadyException("Malformed " + fileURLAdjective + " file URL", e);
			}
		
			String connIdFromUrl = url.getAuthority();
			conn = graph.getConnection(connIdFromUrl);
			// if (conn == null || !(conn instanceof HadoopConnection)), connIdFromUrl can be valid hostname of NameNode
			if (conn != null && !(conn instanceof HadoopConnection)) {
				conn = null;
			}
		}
		
		if (conn == null) {
			if (connectionId == null) {
				throw new ComponentNotReadyException(component, "Hadoop connection ID specified neither in its dedicated attribute nor using " + fileURLAdjective + " file URL with \"hdfs\" protocol");
			}
			conn = graph.getConnection(connectionId);
			if (conn == null) {
				throw new ComponentNotReadyException(component, "Can't find Hadoop connection with ID: " + connectionId);
			}
			if (!(conn instanceof HadoopConnection)) {
				throw new ComponentNotReadyException(component, "Connection with ID: " + connectionId + " is not a Hadoop connection");
			}
			
		}

		log.debug(String.format("Connecting to HDFS via [%s].", conn.getId()));

		conn.init();
		return conn;
	}

	private void prepareMultiFileReader() throws ComponentNotReadyException {
		DataRecordMetadata metadata = getOutputPort(OUTPUT_PORT).getMetadata();
		TransformationGraph graph = getGraph();

		if (connection == null)
			prepareConnection();

		try {
			parser = connection.getFileSystemServiceUnconnected().createParser(this.keyFieldName, this.valueFieldName,
					metadata, connection.getUserName(), connection.getAdditionalProperties());
		} catch (IOException e) {
			throw new ComponentNotReadyException(this, e);
		}

		parser.setGraph(graph);
		parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));

		// initialize multifile reader based on prepared parser
		reader = new MultiFileReader(parser, getContextURL(), fileURL);
		reader.setLogger(logger);
		reader.setSkip(skipRows);
		reader.setNumSourceRecords(numSourceRecords);
		// skip source rows
		if (skipSourceRows == -1) {
			for (DataRecordMetadata dataRecordMetadata : getOutMetadata()) {
				int ssr = dataRecordMetadata.getSkipSourceRows();
				if (ssr > 0) {
					skipSourceRows = ssr;
					break;
				}
			}
		}
		reader.setSkipSourceRows(skipSourceRows);
		reader.setNumRecords(numRecords);
		reader.setIncrementalFile(incrementalFile);
		reader.setIncrementalKey(incrementalKey);
		reader.setInputPort(getInputPort(INPUT_PORT)); // for port protocol:
														// ReadableChannelIterator
														// reads data
		reader.setPropertyRefResolver(getPropertyRefResolver());
		reader.setDictionary(graph.getDictionary());
		reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		HadoopReader hadoopReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		hadoopReader = new HadoopReader(xattribs.getString(XML_ID_ATTRIBUTE),
				xattribs.getStringEx(XML_FILEURL_ATTRIBUTE, null, RefResFlag.URL),
				xattribs.getString(XML_KEY_FIELD_NAME_ATTRIBUTE, null),
				xattribs.getString(XML_VALUE_FIELD_NAME_ATTRIBUTE, null));
			
			if (xattribs.exists(XML_CONNECTION_ID_ATTRIBUTE)) {
				hadoopReader.setConnectionId(xattribs.getString(XML_CONNECTION_ID_ATTRIBUTE));
			}
		if (xattribs.exists(XML_DATAPOLICY_ATTRIBUTE)) {
			hadoopReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE));
		}
		if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)) {
			hadoopReader.setSkipRows(xattribs.getInteger(XML_RECORD_SKIP_ATTRIBUTE));
		}
		if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)) {
			hadoopReader.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_INCREMENTAL_FILE_ATTRIBUTE)) {
			hadoopReader.setIncrementalFile(xattribs.getStringEx(XML_INCREMENTAL_FILE_ATTRIBUTE, RefResFlag.URL));
		}
		if (xattribs.exists(XML_INCREMENTAL_KEY_ATTRIBUTE)) {
			hadoopReader.setIncrementalKey(xattribs.getString(XML_INCREMENTAL_KEY_ATTRIBUTE));
		}
		if (xattribs.exists(XML_SKIP_SOURCE_ROWS_ATTRIBUTE)) {
			hadoopReader.setSkipSourceRows(xattribs.getInteger(XML_SKIP_SOURCE_ROWS_ATTRIBUTE));
		}
		if (xattribs.exists(XML_NUM_SOURCE_RECORDS_ATTRIBUTE)) {
			hadoopReader.setNumSourceRecords(xattribs.getInteger(XML_NUM_SOURCE_RECORDS_ATTRIBUTE));
		}

		return hadoopReader;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}

		checkMetadata(status, null, getOutPorts());

		if (!PolicyType.isPolicyType(policyTypeStr)) {
			status.addError(this, XML_DATAPOLICY_ATTRIBUTE, "Invalid data policy: " + policyTypeStr);
		} else {
			policyType = PolicyType.valueOfIgnoreCase(policyTypeStr);
		}
		
		if (fileURL == null) {
			status.addError(this, XML_FILEURL_ATTRIBUTE, "File URL not defined.");
		}
		if (keyFieldName == null) {
			status.addError(this, XML_KEY_FIELD_NAME_ATTRIBUTE, "Key field not defined.");
		}
		if (valueFieldName == null) {
			status.addError(this, XML_VALUE_FIELD_NAME_ATTRIBUTE, "Value field not defined.");
		}

		try {
			// check inputs
			prepareMultiFileReader();
			checkConnectionIDs(connectionId, connection, this, status);
			DataRecordMetadata metadata = getOutputPort(OUTPUT_PORT).getMetadata();
			if (!metadata.hasFieldWithoutAutofilling()) {
				status.addError(this, null, "No field elements without autofilling for '"
						+ getOutputPort(OUTPUT_PORT).getMetadata().getName() + "' have been found!");
			}
			reader.checkConfig(metadata);
		} catch (Exception e) {
			status.addWarning(this, null, e);
		} catch (NoClassDefFoundError e) {
			status.addError(this, null, e);
		} finally {
			free();
		}

		return status;
	}

	static void checkConnectionIDs(String hadConnId, HadoopConnection usedHadConn, IGraphElement graphElement, ConfigurationStatus status) {
		if (!StringUtils.isEmpty(hadConnId) && !hadConnId.equalsIgnoreCase(usedHadConn.getId())) { 
			status.addInfo(graphElement, XML_CONNECTION_ID_ATTRIBUTE, "Hadoop connecion with ID '" + usedHadConn.getId() + "' is specified in the 'File URL' component property, therefore connection with ID '" + hadConnId +
					"' form 'Hadoop connection' property will be ignored");
		}
	}

	/**
	 * @param skipRows
	 *            The skipRows to set.
	 */
	public void setSkipRows(int skipRows) {
		this.skipRows = skipRows;
	}

	public void setNumRecords(int numRecords) {
		this.numRecords = Math.max(numRecords, 0);
	}

	/**
	 * @param skipSourceRows
	 *            how many rows to skip for every source
	 */
	public void setSkipSourceRows(int skipSourceRows) {
		this.skipSourceRows = Math.max(skipSourceRows, 0);
	}

	/**
	 * @param numSourceRecords
	 *            how many rows to process for every source
	 */
	public void setNumSourceRecords(int numSourceRecords) {
		this.numSourceRecords = Math.max(numSourceRecords, 0);
	}

	public void setPolicyType(String strPolicyType) {
		this.policyTypeStr = strPolicyType;
	}

	public void setIncrementalFile(String incrementalFile) {
		this.incrementalFile = incrementalFile;
	}

	public void setIncrementalKey(String incrementalKey) {
		this.incrementalKey = incrementalKey;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}
}
