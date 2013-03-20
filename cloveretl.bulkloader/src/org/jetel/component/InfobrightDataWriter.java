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
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.infobright.BooleanType;
import org.jetel.component.infobright.CloverValueConverter;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.parser.Parser;
import org.jetel.data.parser.TextParser;
import org.jetel.data.parser.TextParserConfiguration;
import org.jetel.data.parser.TextParserFactory;
import org.jetel.database.IConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

import com.infobright.etl.model.BrighthouseRecord;
import com.infobright.etl.model.DataFormat;
import com.infobright.etl.model.ValueConverter;
import com.infobright.etl.model.datatype.AbstractColumnType;
import com.infobright.io.InfobrightNamedPipeLoader;
import com.infobright.logging.EtlLogger;

/**
 *  <h3>Infobright Data Writer Component</h3>
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Infobright data writer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>Bulk loaders</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Loads data to Infobright database</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] - input records.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - optionally one output port connected - for debugging purpose. Records as they were sent to database.<i>Comma</i> must be set as
 * delimiter for each field, {@link System.getProperty("line.separator")} must be set as record delimiter. Date fields must have
 * <i>yyyy-MM-dd</i> format for dates and <i>yyyy-MM-dd HH:mm:ss</i> format for dates with time. </tr>
 * <tr><td><h4><i><a name="comment">Comment:</a></i></h4></td>
 * <td>If the hostname is "localhost" or "127.0.0.1" then the load
will be done using a local pipe. Otherwise it will use a
remote pipe. The external IP address of the server is not
recognized as a local server.

If loading to a remote server, then you need to start the
Infobright remote load agent on the server where Infobright
is running. The command to do this is:

  java -jar infobright-core-3.0-remote.jar [-p port] [-l loglevel]
  
The output can be redirected to a log file.

The port defaults to 5555. 

The loglevel can be one of: "info", "error", "debug".<br>To run this component on Windows, infobright_jni.dll must be present in the Java library path 
 * (<a href="http://www.infobright.org/downloads/contributions/infobright-core-3.0-remote.zip">infobright-core-3.0-remote.zip</a>).</tr>
 * </tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"INFOBRIGHT_DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>table</b></td><td>table name in database for loading data. </td>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <tr><td><b>dataFormat</b><br><i>optional</i></td><td>bh_dataformat supported by Infobright. Available values:<ul><li><i>txt_variable</i>
 *  <li><i>binary</i> - faster but works with IEE only</ul> </td>
 *  <tr><td><b>charset</b><br><i>optional</i></td><td>the character set to use to encode String values for CHAR, VARCHAR column types.
 *  Default is {@link Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER}</td>
 *  <tr><td><b>logFile</b><br><i>optional</i></td><td>path to debug file. Records as they were sent to database. This attribute has higher
 *  priority then output port, i.e. if <i>logFile</i> is set and output port is connected no data is sent to output port.</td>
 *  <tr><td><b>timeout</b><br><i>optional</i></td><td>timeout for load command. Has effect only on Windows platform. </td>
 *  <tr><td><b>cloverFields</b><br><i>optional</i></td><td>delimited list of input record's fields. Only listed fields (in the order they appear 
 *  in the list) will be considered for mapping onto target table's fields. </td>
 *  <tr><td><b>checkValues</b><br><i>optional</i></td><td>Should strings and binary types be checked for size before being passed to the database?
 *  Default is <i>false</i>. (Should be set to <i>true</i> if you support a dubugging). </td>
 *  </tr>
 *  <tr><td><b>agentPort</b><br><i>optional</i></td><td>Port the remote agent is listening on (see {@link #comment}). </td>
 *  </table>
 *
 *  <h4>Example:</h4>
 * <pre>&lt;Node cloverFields="boolField;dateField;decimalField;stringField" dataFormat="txt_variable" dbConnection="JDBC1" 
 *  id="INFOBRIGHT_DATA_WRITER0" table="test1" type="INFOBRIGHT_DATA_WRITER"/&gt;</pre>
 *  
 *  <b>Remote loading:</b>
 *  <pre>&lt;Node agentPort="6666" dbConnection="JDBC2" id="INFOBRIGHT_DATA_WRITER0" table="test" type="INFOBRIGHT_DATA_WRITER"/&gt;</pre>
 * 
 * @author avackova (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 2 Nov 2009
 */
public class InfobrightDataWriter extends Node {

	private static final String XML_TABLE_ATTRIBUTE = "table";
	private static final String XML_DBCONNECTION_ATTRIBUTE = "dbConnection";
	private static final String XML_DATA_FORMAT_ATTRIBUTE = "dataFormat";
	private static final String XML_CHARSET_ATTRIBUTE = "charset";
	private static final String XML_LOG_FILE_ATTRIBUTE = "logFile";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	private static final String XML_PIPE_NAMEPREFIX_ATTRIBUTE = "pipeNamePrefix";//changing this parameter has no effect - infobright-core bug
	private static final String XML_TIMEOUT_ATTRIBUTE = "timeout";
	private static final String XML_CLOVER_FIELDS_ATTRIBUTE = "cloverFields";
	private static final String XML_CHECK_VALUES_ATTRIBUTE = "checkValues";
	private static final String XML_AGENT_PORT_ATTRIBUTE = "agentPort";

    public final static String COMPONENT_TYPE = "INFOBRIGHT_DATA_WRITER";
    
    private final static int DEFAULT_AGENT_PORT = 5555;
    private final static DataFormat DEFAULT_DATA_FORMAT = DataFormat.TXT_VARIABLE;

	static Log logger = LogFactory.getLog(CheckForeignKey.class);

	private final static int READ_FROM_PORT = 0;
	private final static int WRITE_TO_PORT = 0;

	private String table;
	private String connectionName;
	private DBConnection dbConnection;
	private Connection sqlConnection;
	private DataFormat dataFormat = DEFAULT_DATA_FORMAT;
	private String dataFormatStr;
	private String charset;
	private String logFile;
	private boolean append = false;
	private String pipeNamePrefix;
	private int timeout = -1;
	private String[] cloverFields;
	private int agentPort = DEFAULT_AGENT_PORT;
	
	private BrighthouseRecord bRecord;
	private ValueConverter converter;
	private InfobrightNamedPipeLoader loader;
	private int[] cloverFieldIndexes;
	private TextParser dataParser;
	private Charset chset;
	private CommonsLogger log;
	private boolean checkValues = false;
	
	/**
	 * @param id
	 */
	public InfobrightDataWriter(String id) {
		super(id);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#checkConfig(org.jetel.exception.ConfigurationStatus)
	 */
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		checkInputPorts(status, 1, 1);
		checkOutputPorts(status, 0, 1);
		
		try {
			chset = Charset.forName(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		} catch (Exception e) {
			status.add(ExceptionUtils.getMessage(e), Severity.ERROR, this, Priority.NORMAL, XML_CHARSET_ATTRIBUTE);
		}

		//check debug file
		try {
			if (logFile != null && !FileUtils.canWrite(getGraph().getRuntimeContext().getContextURL(), logFile)) {
				status.add(new ConfigurationProblem("Can't write to " + logFile, Severity.WARNING, this, Priority.NORMAL, XML_LOG_FILE_ATTRIBUTE));
			}
		} catch (ComponentNotReadyException e) {
			status.add(new ConfigurationProblem(e, Severity.WARNING, this, Priority.NORMAL, XML_LOG_FILE_ATTRIBUTE));
		}
		
		if (dataFormatStr != null) {
			if (!(dataFormatStr.equalsIgnoreCase(DataFormat.BINARY.getBhDataFormat())
					|| dataFormatStr.equalsIgnoreCase(DataFormat.TXT_VARIABLE.getBhDataFormat()))){
				status.add(new ConfigurationProblem("Unknown data format: " + dataFormatStr + ". " + DEFAULT_DATA_FORMAT.getBhDataFormat() + " will be used.", 
						Severity.WARNING, this, Priority.NORMAL, XML_DATA_FORMAT_ATTRIBUTE));
			}
			this.dataFormat = dataFormatStr.equalsIgnoreCase(DataFormat.BINARY.getBhDataFormat()) ?
					DataFormat.BINARY : DataFormat.TXT_VARIABLE;
		}
		
		// get dbConnection from graph
	    if (dbConnection == null){
	        IConnection conn = getGraph().getConnection(connectionName);
            if(conn == null) {
                status.add("Can't find DBConnection ID: " + connectionName, Severity.ERROR, this, Priority.NORMAL, XML_DBCONNECTION_ATTRIBUTE);
            }else
            if(!(conn instanceof DBConnection)) {
                status.add("Connection with ID: " + connectionName + " isn't instance of the DBConnection class.", Severity.ERROR, this, Priority.NORMAL, XML_DBCONNECTION_ATTRIBUTE);
            }else{
            	dbConnection = (DBConnection) conn;
            }
	    }
	    //check connection
		if (dbConnection != null && !dbConnection.isInitialized()) {
			try {
				dbConnection.init();
				sqlConnection = dbConnection.getConnection(getId(), OperationType.WRITE);
			} catch (ComponentNotReadyException e) {
				status.add(e, Severity.ERROR, this, Priority.NORMAL, XML_DBCONNECTION_ATTRIBUTE);
				return status;
			} catch (JetelException e) {
				status.add(ExceptionUtils.getMessage(e), Severity.ERROR, this, Priority.NORMAL, XML_DBCONNECTION_ATTRIBUTE);
				return status;
			}
		}        

		DataRecordMetadata metadata = getInputPort(READ_FROM_PORT).getMetadata();
		if (cloverFields != null) {
			cloverFieldIndexes = new int[cloverFields.length];
			for (int i = 0; i < cloverFieldIndexes.length; i++) {
				cloverFieldIndexes[i] = metadata.getFieldPosition(cloverFields[i]);
			}
		}else{
			cloverFieldIndexes = new int[metadata.getNumFields()];
			for (int i = 0; i < cloverFieldIndexes.length; i++) {
				cloverFieldIndexes[i] = i;
			}
		}
		//try to create loader and Brighthouse record
		log = new CommonsLogger(logger);
		try {
			loader = new InfobrightNamedPipeLoader(table, sqlConnection, log, dataFormat, chset, agentPort);
			// TODO Labels:
//			String quotedTable = "`" + table + "`";
//			loader = new InfobrightNamedPipeLoader(quotedTable, sqlConnection, log, dataFormat, chset, agentPort);
			// TODO Labels end
		} catch (Exception e) {
			status.add(new ComponentNotReadyException(e), Severity.ERROR, this, Priority.NORMAL, XML_AGENT_PORT_ATTRIBUTE);
		}

		if (sqlConnection != null && loader != null) {
			try {
				bRecord = createBrighthouseRecord(metadata, dbConnection.getJdbcSpecific(), log);
			} catch (SQLException e) {//probably table doesn't exist yet
				status.add(ExceptionUtils.getMessage(e), Severity.WARNING, this, Priority.NORMAL, XML_TABLE_ATTRIBUTE);
			}catch (Exception e) {
				status.add(ExceptionUtils.getMessage(e), Severity.ERROR, this, Priority.NORMAL, XML_CLOVER_FIELDS_ATTRIBUTE);
			}
		}
		return status;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#init()
	 */
	
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		if (loader == null) {//if not created in checkConfig() 
			// get dbConnection from graph
			if (dbConnection == null) {
				IConnection conn = getGraph().getConnection(connectionName);
				if (conn == null) {
					throw new ComponentNotReadyException(
							"Can't find DBConnection ID: " + connectionName);
				}
				if (!(conn instanceof DBConnection)) {
					throw new ComponentNotReadyException("Connection with ID: "
							+ connectionName
							+ " isn't instance of the DBConnection class.");
				}
				dbConnection = (DBConnection) conn;
			}
			if (!dbConnection.isInitialized()) {
				dbConnection.init();
			}
			try {
				sqlConnection = dbConnection.getConnection(getId(), OperationType.WRITE);
			} catch (JetelException e) {
				throw new ComponentNotReadyException(this,
						"Invalid " + XML_DBCONNECTION_ATTRIBUTE, e);
			}
			//prepare indexes of clover fields to load
			DataRecordMetadata metadata = getInputPort(READ_FROM_PORT)
					.getMetadata();
			if (cloverFields != null) {
				cloverFieldIndexes = new int[cloverFields.length];
				for (int i = 0; i < cloverFieldIndexes.length; i++) {
					cloverFieldIndexes[i] = metadata
							.getFieldPosition(cloverFields[i]);
				}
			} else {
				cloverFieldIndexes = new int[metadata.getNumFields()];
				for (int i = 0; i < cloverFieldIndexes.length; i++) {
					cloverFieldIndexes[i] = i;
				}
			}
			chset = Charset.forName(charset != null ? charset
					: Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
			if (dataFormatStr != null) {
				if (!(dataFormatStr.equalsIgnoreCase(DataFormat.BINARY.getBhDataFormat()) 
						|| dataFormatStr.equalsIgnoreCase(DataFormat.TXT_VARIABLE.getBhDataFormat()))){
					logger.info("Unknown data format: " + dataFormatStr + ". " + DEFAULT_DATA_FORMAT + " will be used.");
				}
				this.dataFormat = dataFormatStr.equalsIgnoreCase(DataFormat.BINARY.getBhDataFormat()) ?
						DataFormat.BINARY : DataFormat.TXT_VARIABLE;
			}
			//create loader and Brighthouse record
			log = new CommonsLogger(logger);
		}
		converter = new CloverValueConverter();
	}

	/**
	 * Creates brighthouse record from input metadata
	 * 
	 * @param metadata input metadata
	 * @param jdbcSpecific connection specific (should be MYSQL)
	 * @param logger
	 * @return
	 * @throws SQLException when database error occurs
	 * @throws ComponentNotReadyException for wrong number or type of the fields 
	 */
	private BrighthouseRecord createBrighthouseRecord(DataRecordMetadata metadata, JdbcSpecific jdbcSpecific, 
			EtlLogger logger) throws Exception{
	    Statement stmt = sqlConnection.createStatement();
	    ResultSet rs = stmt.executeQuery("select * from `" + table + "` limit 0");
	    ResultSetMetaData md = rs.getMetaData();
	    if (md.getColumnCount() != cloverFieldIndexes.length) {
	    	throw new ComponentNotReadyException(this, "Number of db fields (" + md.getColumnCount()+ ") is different then " +
					"number of clover fields (" + cloverFieldIndexes.length + ")." );
	    }
		List<AbstractColumnType> columns = new ArrayList<AbstractColumnType>(md.getColumnCount());
		AbstractColumnType col;
		for (int i = 0; i < cloverFieldIndexes.length; i++) {
			col = jetelType2Brighthouse(metadata.getField(cloverFieldIndexes[i]), md.getPrecision(i + 1), jdbcSpecific, logger);
			col.setCheckValues(checkValues);
			columns.add(col);
		}
	    rs.close();
	    stmt.close();
		return dataFormat.createRecord(columns, chset, logger);
	}
	
	/**
	 * Convert clover type to brighthouse type
	 * 
	 * @param field field metadata
	 * @param precision length for character and binary fields, precision for decimal field
	 * @param jdbcSpecific connection specific (should be MYSQL)
	 * @param logger
	 * @return
	 * @throws ComponentNotReadyException
	 */
	private AbstractColumnType jetelType2Brighthouse(DataFieldMetadata field, int precision, JdbcSpecific jdbcSpecific, EtlLogger logger) throws ComponentNotReadyException{
		String columnName = field.getName(); // label is not necessary, the name is not used for mapping
		String columnTypeName = field.getTypeAsString();
		switch (field.getType()) {
		case DataFieldMetadata.BOOLEAN_FIELD:
			logger.info(String.format("Column: %s %s(%d)", columnName, columnTypeName, 0));
			return new BooleanType();
		case DataFieldMetadata.BYTE_FIELD:
		case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			return AbstractColumnType.getInstance(columnName, field.isFixed() ? Types.BINARY : Types.VARBINARY, columnTypeName, 
					field.isFixed() ? field.getSize() : precision, 0, chset, logger);
		case DataFieldMetadata.DATE_FIELD:
			return AbstractColumnType.getInstance(columnName, jdbcSpecific.jetelType2sql(field), columnTypeName, 0, 0, chset, logger);
		case DataFieldMetadata.DECIMAL_FIELD:
			return AbstractColumnType.getInstance(columnName, Types.DECIMAL, columnTypeName, 
					field.getFieldProperties().getIntProperty(DataFieldMetadata.LENGTH_ATTR),
					field.getFieldProperties().getIntProperty(DataFieldMetadata.SCALE_ATTR), chset, logger);
		case DataFieldMetadata.INTEGER_FIELD:
			return AbstractColumnType.getInstance(columnName, Types.INTEGER, columnTypeName, precision, 0, chset, logger);
		case DataFieldMetadata.LONG_FIELD:
			return AbstractColumnType.getInstance(columnName, Types.BIGINT, columnTypeName, precision, 0, chset, logger);
		case DataFieldMetadata.NUMERIC_FIELD:
			return AbstractColumnType.getInstance(columnName, Types.DOUBLE, columnTypeName, precision, 0, chset, logger);
		case DataFieldMetadata.STRING_FIELD:
			return AbstractColumnType.getInstance(columnName, field.isFixed() ? Types.CHAR : Types.VARCHAR, columnTypeName, 
					field.isFixed() ? field.getSize() : precision, 0, chset, logger);
		default:
		      throw new ComponentNotReadyException(this, "Unsupported type (" + columnTypeName  + ") for column " + columnName);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#preExecute()
	 */
	@Override
	public void preExecute() throws ComponentNotReadyException {
		try {
			bRecord = createBrighthouseRecord(getInputPort(READ_FROM_PORT).getMetadata(), dbConnection.getJdbcSpecific(), log);
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, e);
		}

		try {
			loader = new InfobrightNamedPipeLoader(table, sqlConnection, log, dataFormat, chset, agentPort);
			// TODO Labels: 
//			String quotedTable = "`" + table + "`";
//			loader = new InfobrightNamedPipeLoader(quotedTable, sqlConnection, log, dataFormat, chset, agentPort);
			// TODO Labels end
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, e);
		}
		try {
			if (logFile != null) {
				loader.setDebugOutputStream(FileUtils.getOutputStream(getGraph().getRuntimeContext().getContextURL(), logFile, append, -1));
			} else if (getOutputPort(WRITE_TO_PORT) != null) {//prepare parser for output port
				final TextParserConfiguration parserCfg = new TextParserConfiguration();
				parserCfg.setCharset(charset);
				parserCfg.setQuotedStrings(true);
				parserCfg.setMetadata(getOutputPort(WRITE_TO_PORT).getMetadata());
				dataParser = TextParserFactory.getParser(parserCfg);
				dataParser.init();
				PipedInputStream parserInput = new PipedInputStream();
				PipedOutputStream loaderOutput = new PipedOutputStream(	parserInput);
				dataParser.setDataSource(parserInput);
				loader.setDebugOutputStream(loaderOutput);
			}
		} catch (IOException e) {
			throw new ComponentNotReadyException(this, e);
		}
		if (pipeNamePrefix != null) {//has no effect - infobright-core bug
			loader.setPipeNamePrefix(pipeNamePrefix);
		}
		if (timeout > -1) {
			loader.setTimeout(timeout);
		}
		super.preExecute();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		Result result = Result.RUNNING;
		Throwable ex = null;
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord inRecord = DataRecordFactory.newRecord(inPort.getMetadata());
		inRecord.init();
		//thread that writes data to database
		InfoBrightWriter infobrightWriter = 
			new InfoBrightWriter(Thread.currentThread(), inPort, inRecord, cloverFieldIndexes, bRecord, converter, loader);
		infobrightWriter.start();
		registerChildThread(infobrightWriter);
		PortWriter portWriter = null;
		//thread that sends debug data to output port
		if (dataParser != null) {
			OutputPort outPort = getOutputPort(WRITE_TO_PORT);
			DataRecord out_record = DataRecordFactory.newRecord(outPort.getMetadata());
			out_record.init();
			portWriter  = new PortWriter(Thread.currentThread(), outPort, out_record, dataParser);
			portWriter.start();
			registerChildThread(portWriter);
			try {
				portWriter.join();
			} catch (InterruptedException e) {
				runIt = false;
				infobrightWriter.stop_it();//interrupt writing to database
				resultMessage = "Writing to output port interrupted";
			}
			result = portWriter.getResultCode();
			if (result == Result.ERROR){
				resultMessage = "Port writer error: " + portWriter.getResultMsg();
				ex = portWriter.getResultException();
			}
		}
		try {
			infobrightWriter.join();
		} catch (InterruptedException e) {
			runIt = false;
			if (portWriter != null) {
				portWriter.stop_it();//interrupt sending data to output port
			}
			resultMessage = "Writing to database interrupted";
		}
		if (infobrightWriter.getResultCode() == Result.ERROR) {
			result = Result.ERROR;
			resultMessage = (resultMessage == null ? "" : resultMessage + "\n") + "Infobright writer error: " + infobrightWriter.getResultMsg();
			ex = infobrightWriter.getResultException();
		}
		if (result == Result.ERROR) {
			throw new JetelException(resultMessage, ex);
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#postExecute(org.jetel.graph.TransactionMethod)
	 */
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
	}
	
	
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
	
	@Override
	public void commit() {
		super.commit();
		try {
			sqlConnection.commit();
		} catch (SQLException e) {
			throw new RuntimeException(this.getId() + " Rollback failed!");
		}
	}

	@Override
	public void rollback() {
		super.rollback();
		try {
			sqlConnection.rollback();
		} catch (SQLException e) {
			throw new RuntimeException(this.getId() + " Rollback failed!");
		}
		logger.warn(this.getId() + " finished with error. The current transaction has been rolled back.");
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#toXML(org.w3c.dom.Element)
	 */
	
	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		xmlElement.setAttribute(XML_DATA_FORMAT_ATTRIBUTE, dataFormat.getBhDataFormat());
		xmlElement.setAttribute(XML_TABLE_ATTRIBUTE, table);
		xmlElement.setAttribute(XML_DBCONNECTION_ATTRIBUTE, connectionName != null ? connectionName : dbConnection.getId());
		xmlElement.setAttribute(XML_CHECK_VALUES_ATTRIBUTE, String.valueOf(checkValues));
		xmlElement.setAttribute(XML_AGENT_PORT_ATTRIBUTE, String.valueOf(agentPort));
		if (logFile != null) {
			xmlElement.setAttribute(XML_LOG_FILE_ATTRIBUTE, logFile);
			xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(append));
		}
		if (pipeNamePrefix != null) {
			xmlElement.setAttribute(XML_PIPE_NAMEPREFIX_ATTRIBUTE, pipeNamePrefix);
		}
		if (timeout > -1){
			xmlElement.setAttribute(XML_TIMEOUT_ATTRIBUTE, String.valueOf(timeout));
		}
		if (cloverFields != null){
			xmlElement.setAttribute(XML_CLOVER_FIELDS_ATTRIBUTE, StringUtils.stringArraytoString(cloverFields, Defaults.Component.KEY_FIELDS_DELIMITER));
		}
	}
	
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
        InfobrightDataWriter loader;

        loader = new InfobrightDataWriter(xattribs.getString(XML_ID_ATTRIBUTE));
        loader.setDbConnection(xattribs.getString(XML_DBCONNECTION_ATTRIBUTE));
        loader.setTable(xattribs.getString(XML_TABLE_ATTRIBUTE));
        if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
			loader.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
		}
        if (xattribs.exists(XML_DATA_FORMAT_ATTRIBUTE)) {
			loader.setDataFormat(xattribs.getString(XML_DATA_FORMAT_ATTRIBUTE));
		}
        if (xattribs.exists(XML_LOG_FILE_ATTRIBUTE)) {
			loader.setLogFile(xattribs.getString(XML_LOG_FILE_ATTRIBUTE));
			loader.setAppend(xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false));
		}
        if (xattribs.exists(XML_PIPE_NAMEPREFIX_ATTRIBUTE)) {
			loader.setPipeNamePrefix(xattribs.getString(XML_PIPE_NAMEPREFIX_ATTRIBUTE));
		}
        if (xattribs.exists(XML_TIMEOUT_ATTRIBUTE)) {
			loader.setTimeout(xattribs.getInteger(XML_TIMEOUT_ATTRIBUTE));
		}
        if (xattribs.exists(XML_CLOVER_FIELDS_ATTRIBUTE)){
        	loader.setCloverFields(xattribs.getString(XML_CLOVER_FIELDS_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
        }
        if (xattribs.exists(XML_CHECK_VALUES_ATTRIBUTE)){
        	loader.setCheckValues(xattribs.getBoolean(XML_CHECK_VALUES_ATTRIBUTE));
        }
        if (xattribs.exists(XML_AGENT_PORT_ATTRIBUTE)){
        	loader.setAgentPort(xattribs.getInteger(XML_AGENT_PORT_ATTRIBUTE));
        }
		return loader;
	}
    
	public void setAgentPort(int agentPort) {
		this.agentPort = agentPort;
	}

	/**
	 * Should strings and binary types be checked for size before being passed to the database? 
	 * (Should be set to TRUE if you support an error path)
	 * 
	 * @param checkValues
	 */
	public void setCheckValues(boolean checkValues) {
		this.checkValues = checkValues;
	}

	/**
	 * @param table the table to set
	 */
	public void setTable(String table) {
		this.table = table;
	}

	/**
	 * @param timeout the timeout to set
	 */
	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	/**
	 * @param cloverFields the cloverFields to set
	 */
	public void setCloverFields(String[] cloverFields) {
		this.cloverFields = cloverFields;
	}

	/**
	 * @param pipeNamePrefix the pipeNamePrefix to set
	 */
	public void setPipeNamePrefix(String pipeNamePrefix) {
		this.pipeNamePrefix = pipeNamePrefix;
	}

	/**
	 * @param logFile the logFile to set
	 */
	public void setLogFile(String logFile) {
		this.logFile = logFile;
	}

	/**
	 * @param append the append to set
	 */
	public void setAppend(boolean append) {
		this.append = append;
	}

	/**
	 * @param dbConnection the dbConnection to set
	 */
	public void setDbConnection(String dbConnection) {
		this.connectionName = dbConnection;
	}

	/**
	 * @param dbConnection the dbConnection to set
	 */
	public void setDbConnection(DBConnection dbConnection) {
		this.dbConnection = dbConnection;
	}

	/**
	 * @param dataFormat the dataFormat to set
	 */
	public void setDataFormat(String dataFormat) {
		dataFormatStr = dataFormat;
	}

	/**
	 * @param charset the charset to set
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}

	/**
	 * Wrapper for commons logger to pass it to Infobright components
	 * 
	 * @author avackova (info@cloveretl.com)
	 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
	 *
	 * @created 13 Nov 2009
	 */
	private static class CommonsLogger implements EtlLogger {

		Log logger;
		
		/**
		 * @param logger
		 */
		public CommonsLogger(Log logger) {
			this.logger = logger;
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#debug(java.lang.String)
		 */
		
		@Override
		public void debug(String s) {
			logger.debug(s);
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#error(java.lang.String)
		 */
		
		@Override
		public void error(String s) {
			logger.error(s);
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#error(java.lang.String, java.lang.Throwable)
		 */
		
		@Override
		public void error(String s, Throwable cause) {
			logger.error(s, cause);
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#fatal(java.lang.String)
		 */
		
		@Override
		public void fatal(String s) {
			logger.fatal(s);
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#info(java.lang.String)
		 */
		
		@Override
		public void info(String s) {
			logger.info(s);
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#trace(java.lang.String)
		 */
		
		@Override
		public void trace(String s) {
			logger.trace(s);
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#warn(java.lang.String)
		 */
		
		@Override
		public void warn(String s) {
			logger.warn(s);
		}
		
	}
	
	/**
	 * Thread for loading data to Infobright database
	 * 
	 * @author avackova (info@cloveretl.com)
	 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
	 *
	 * @created 13 Nov 2009
	 */
	private static class InfoBrightWriter extends Thread {

		InputPort inPort;
		DataRecord in_record;
		String resultMsg=null;
		Result resultCode;
        volatile boolean runIt;
		Throwable resultException;
		private int[] cloverFields;
		private BrighthouseRecord bRecord;
		private ValueConverter converter;
		private OutputStream output;
		private InfobrightNamedPipeLoader loader;
	
		InfoBrightWriter(Thread parentThread,InputPort inPort,DataRecord in_record,int[] cloverFields, BrighthouseRecord bRecord, 
				ValueConverter converter, InfobrightNamedPipeLoader loader) throws IOException{
			super(parentThread.getName()+".InfoBrightWriter");
			this.in_record=in_record;
			this.inPort=inPort;
			this.cloverFields = cloverFields;
			this.bRecord = bRecord;
			this.converter = converter;
			runIt=true;
			this.loader = loader;
		}
		
		public void stop_it(){
			runIt=false;	
		}
		
		@Override
		public synchronized void start() {
			try {
				loader.start();
			} catch (Exception e) {
				throw new RuntimeException(e);
			} 
			super.start();
		}
		
		@Override
		public void run() {
			resultCode = Result.RUNNING;
			try{
				output = loader.getOutputStream2();
				while (runIt && (( in_record=inPort.readRecord(in_record))!= null )) {
					for (int i = 0; i < cloverFields.length; i++) {
						bRecord.setData(i, in_record.getField(cloverFields[i]), converter);
					}
					bRecord.writeTo(output);
                    SynchronizeUtils.cloverYield();
				}
			}catch(IOException ex){
				resultMsg = ExceptionUtils.getMessage(ex);
				resultCode = Result.ERROR;
				resultException = ex;
			}catch (InterruptedException ex){
				resultCode =  Result.ABORTED;
			}catch(Exception ex){
				resultMsg = ExceptionUtils.getMessage(ex);
				resultCode = Result.ERROR;
				resultException = ex;
			} finally{
				try {
					loader.stop();
				} catch (Exception e) {
					resultMsg = ExceptionUtils.getMessage(e);
					resultCode = Result.ERROR;
					resultException = e;
				}
			}
			if (resultCode==Result.RUNNING){
	           if (runIt){
	        	   resultCode=Result.FINISHED_OK;
	           }else{
	        	   resultCode = Result.ABORTED;
	           }
			}
		}

		public Result getResultCode() {
			return resultCode;
		}

		public String getResultMsg() {
			return resultMsg;
		}

		public Throwable getResultException() {
			return resultException;
		}
	}

	/**
	 * Thread for sending data to output port
	 * 
	 * @author avackova (info@cloveretl.com)
	 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
	 *
	 * @created 13 Nov 2009
	 */
	private static class PortWriter extends Thread {

		DataRecord out_record;
		OutputPort outPort;
		Parser parser;
		String resultMsg=null;
		Result resultCode;
		volatile boolean runIt;
		Throwable resultException;
		
		PortWriter(Thread parentThread,OutputPort outPort,DataRecord out_record,Parser parser){
			super(parentThread.getName()+".SendData");
			this.out_record=out_record;
			this.parser=parser;
			this.outPort=outPort;
			this.runIt=true;
		}
		
		public void stop_it(){
			runIt=false;	
		}
		
		@Override
		public void run() {
            resultCode=Result.RUNNING;
			try{
				while (runIt && ((out_record = parser.getNext(out_record))!= null) ) {
					outPort.writeRecord(out_record);
					SynchronizeUtils.cloverYield();
				}
			}catch(IOException ex){	
				resultMsg = ExceptionUtils.getMessage(ex);
				resultCode = Result.ERROR;
				resultException = ex;
			}catch (InterruptedException ex){
				resultCode = Result.ABORTED;
			}catch(Exception ex){
				resultMsg = ExceptionUtils.getMessage(ex);
				resultCode = Result.ERROR;
				resultException = ex;
			}finally{
				try {
					parser.close();
                } catch (Exception e) {
    				resultMsg = ExceptionUtils.getMessage(e);
    				resultCode = Result.ERROR;
    				resultException = e;
                }
			}
			if (resultCode == Result.RUNNING)
				if (runIt) {
					resultCode = Result.FINISHED_OK;
				} else {
					resultCode = Result.ABORTED;
				}
		}

        /**
		 * @return Returns the resultCode.
		 */
        public Result getResultCode() {
            return resultCode;
        }

		public String getResultMsg() {
			return resultMsg;
		}

		public Throwable getResultException() {
			return resultException;
		}
	}

}
