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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.hadoop.connection.HadoopConnection;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MultiFileReader;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * @author David Pavlis (info@cloveretl.com) (c) Javlin a.s. (www.javlin.eu)
 * 
 * @since
 * @revision $Revision: $
 */

public class HadoopReader extends Node {

	private static final String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	public static final String XML_FILEURL_ATTRIBUTE = "fileURL";
	private static final String XML_RECORD_SKIP_ATTRIBUTE = "skipRows";
	private static final String XML_NUMRECORDS_ATTRIBUTE = "numRecords";
	private static final String XML_SKIP_SOURCE_ROWS_ATTRIBUTE = "skipSourceRows";
	private static final String XML_NUM_SOURCE_RECORDS_ATTRIBUTE = "numSourceRecords";
	private static final String XML_INCREMENTAL_FILE_ATTRIBUTE = "incrementalFile";
	private static final String XML_INCREMENTAL_KEY_ATTRIBUTE = "incrementalKey";
	private static final String XML_KEY_FIELD_NAME_ATTRIBUTE = "keyField";
	private static final String XML_VALUE_FIELD_NAME_ATTRIBUTE = "valueField";
	private static final String XML_CONNECTION_ATTRIBUTE = "connection";

	/** Description of the Field */
	public final static String COMPONENT_TYPE = "HADOOP_READER";

	private static Log logger = LogFactory.getLog(HadoopReader.class);

	private final static int INPUT_PORT = 0;
	private final static int OUTPUT_PORT = 0;
	private MultiFileReader reader;
	private PolicyType policyType; // default value set in fromXML()
	private String fileURL;
	private String connectionID;
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

	IParserExceptionHandler exceptionHandler = null;

	/**
	 * Constructor for the DBFDataReader object
	 * 
	 * @param id
	 *            Description of the Parameter
	 * @param fileURL
	 *            Description of the Parameter
	 */
	public HadoopReader(String id, String fileURL, String connectionID, String keyFieldName, String valueFieldName) {
		super(id);
		this.fileURL = fileURL;
		this.connectionID = connectionID;
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
		record.init();

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

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		try {
			reader.postExecute();
		} catch (ComponentNotReadyException e) {
			throw new ComponentNotReadyException(COMPONENT_TYPE + ": " + e.getMessage(), e);
		}
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
			logger.warn("Resource releasing failed for '" + getId() + "'. " + t.getMessage(), t);
		}
	}

	/**
	 * Stores all values as incremental reading.
	 */
	private void storeValues() {
		try {
			Object dictValue = getGraph().getDictionary().getValue(Defaults.INCREMENTAL_STORE_KEY);
			if (dictValue != null && dictValue == Boolean.FALSE) {
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
		if (isInitialized())
			return;
		super.init();
		if (connection == null)
			prepareConnection();
		prepareMultiFileReader();
	}

	private void prepareConnection() throws ComponentNotReadyException {
		IConnection conn = getGraph().getConnection(connectionID);
		if (conn == null) {
			throw new ComponentNotReadyException(this, "Can't find HadoopConnection ID: " + connectionID);
		}
		if (!(conn instanceof HadoopConnection)) {
			throw new ComponentNotReadyException(this, "Connection with ID: " + connectionID
					+ " isn't instance of the HadoopConnection class - " + conn.getClass().toString());
		}

		logger.debug(String.format("Connecting to HDFS via [%s].", conn.getId()));

		conn.init();
		this.connection = (HadoopConnection) conn;
	}

	private void prepareMultiFileReader() throws ComponentNotReadyException {
		DataRecordMetadata metadata = getOutputPort(OUTPUT_PORT).getMetadata();
		TransformationGraph graph = getGraph();

		if (connection == null)
			prepareConnection();

		try {
			parser = connection.getConnection().createParser(this.keyFieldName, this.valueFieldName, metadata);
		} catch (IOException e) {
			throw new ComponentNotReadyException(this, e);
		}

		parser.setGraph(graph);
		parser.setExceptionHandler(exceptionHandler);

		// initialize multifile reader based on prepared parser
		reader = new MultiFileReader(parser, graph != null ? graph.getRuntimeContext().getContextURL() : null, fileURL);
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
		reader.setPropertyRefResolver(new PropertyRefResolver(graph.getGraphProperties()));
		reader.setDictionary(graph.getDictionary());
		reader.init(getOutputPort(OUTPUT_PORT).getMetadata());
	}

	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);

		xmlElement.setAttribute(XML_KEY_FIELD_NAME_ATTRIBUTE, this.keyFieldName);
		xmlElement.setAttribute(XML_VALUE_FIELD_NAME_ATTRIBUTE, this.valueFieldName);

		PolicyType policyType = this.parser.getPolicyType();
		if (policyType != null) {
			xmlElement.setAttribute(XML_DATAPOLICY_ATTRIBUTE, policyType.toString());
		}
		xmlElement.setAttribute(XML_FILEURL_ATTRIBUTE, this.fileURL);
		if (skipRows != 0) {
			xmlElement.setAttribute(XML_RECORD_SKIP_ATTRIBUTE, String.valueOf(skipRows));
		}
		if (numRecords != 0) {
			xmlElement.setAttribute(XML_NUMRECORDS_ATTRIBUTE, String.valueOf(numRecords));
		}
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		HadoopReader hadoopReader = null;
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

		try {

			hadoopReader = new HadoopReader(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_FILEURL_ATTRIBUTE), xattribs.getString(XML_CONNECTION_ATTRIBUTE),
					xattribs.getString(XML_KEY_FIELD_NAME_ATTRIBUTE),
					xattribs.getString(XML_VALUE_FIELD_NAME_ATTRIBUTE));

			if (xattribs.exists(XML_DATAPOLICY_ATTRIBUTE)) {
				hadoopReader.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE));
			} else {
				// default policy type
				hadoopReader.setPolicyType(PolicyType.STRICT);
			}
			if (xattribs.exists(XML_RECORD_SKIP_ATTRIBUTE)) {
				hadoopReader.setSkipRows(xattribs.getInteger(XML_RECORD_SKIP_ATTRIBUTE));
			}
			if (xattribs.exists(XML_NUMRECORDS_ATTRIBUTE)) {
				hadoopReader.setNumRecords(xattribs.getInteger(XML_NUMRECORDS_ATTRIBUTE));
			}
			if (xattribs.exists(XML_INCREMENTAL_FILE_ATTRIBUTE)) {
				hadoopReader.setIncrementalFile(xattribs.getStringEx(XML_INCREMENTAL_FILE_ATTRIBUTE,
						RefResFlag.SPEC_CHARACTERS_OFF));
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
		} catch (Exception ex) {
			throw new XMLConfigurationException(COMPONENT_TYPE + ":"
					+ xattribs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ":" + ex.getMessage(), ex);
		}

		return hadoopReader;
	}

	/**
	 * Adds BadDataFormatExceptionHandler to behave according to DataPolicy.
	 * 
	 * @param handler
	 */
	private void setExceptionHandler(IParserExceptionHandler handler) {
		exceptionHandler = handler;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (!checkInputPorts(status, 0, 1) || !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
			return status;
		}

		checkMetadata(status, getOutMetadata());

		try {
			// check inputs
			prepareMultiFileReader();
			DataRecordMetadata metadata = getOutputPort(OUTPUT_PORT).getMetadata();
			if (!metadata.hasFieldWithoutAutofilling()) {
				status.add(new ConfigurationProblem("No field elements without autofilling for '"
						+ getOutputPort(OUTPUT_PORT).getMetadata().getName() + "' have been found!",
						ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL));
			}
			reader.checkConfig(metadata);
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(),
					ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL);
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
		setPolicyType(PolicyType.valueOfIgnoreCase(strPolicyType));
	}

	public void setPolicyType(PolicyType policyType) {
		this.policyType = policyType;
		setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));
	}

	public void setIncrementalFile(String incrementalFile) {
		this.incrementalFile = incrementalFile;
	}

	public void setIncrementalKey(String incrementalKey) {
		this.incrementalKey = incrementalKey;
	}

}
