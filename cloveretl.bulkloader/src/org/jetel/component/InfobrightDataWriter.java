/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
 *   
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation; either
 *    version 2.1 of the License, or (at your option) any later version.
 *   
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU   
 *    Lesser General Public License for more details.
 *   
 *    You should have received a copy of the GNU Lesser General Public
 *    License along with this library; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 *
 */
package org.jetel.component;

import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.infobright.BooleanType;
import org.jetel.component.infobright.CloverValueConverter;
import org.jetel.connection.jdbc.DBConnection;
import org.jetel.connection.jdbc.specific.JdbcSpecific;
import org.jetel.connection.jdbc.specific.JdbcSpecific.OperationType;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.DataParser;
import org.jetel.database.IConnection;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransactionMethod;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
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
 * @author avackova (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
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
	private static final String XML_PIPE_NAMEPREFIX_ATTRIBUTE = "pipeNamePrefix";
	private static final String XML_TIMEOUT_ATTRIBUTE = "timeout";
	private static final String XML_CLOVER_FIELDS_ATTRIBUTE = "cloverFields";

    public final static String COMPONENT_TYPE = "INFOBRIGHT_DATA_WRITER";

	static Log logger = LogFactory.getLog(CheckForeignKey.class);

	private final static int READ_FROM_PORT = 0;
	private final static int WRITE_TO_PORT = 0;

	private String table;
	private String connectionName;
	private DBConnection dbConnection;
	private Connection sqlConnection;
	private DataFormat dataFormat = DataFormat.TXT_VARIABLE;
	private String charset;
	private String logFile;
	private boolean append = false;
	private String pipeNamePrefix;
	private int timeout = -1;
	private String[] cloverFields;
	
	private OutputStream output;
	private BrighthouseRecord bRecord;
	private ValueConverter converter;
	private InfobrightNamedPipeLoader loader;
	private int[] cloverFieldIndexes;
	private DataParser dataParser;
	
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
		 
		if(!checkInputPorts(status, 1, 1)
				|| !checkOutputPorts(status, 0, 1)) {
			return status;
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
		if (dbConnection != null && !dbConnection.isInitialized()) {
			try {
				dbConnection.init();
				sqlConnection = dbConnection.getConnection(getId(), OperationType.WRITE).getSqlConnection();
			} catch (ComponentNotReadyException e) {
				status.add(e, Severity.ERROR, this, Priority.NORMAL, XML_DBCONNECTION_ATTRIBUTE);
			} catch (JetelException e) {
				status.add(e.getMessage(), Severity.ERROR, this, Priority.NORMAL, XML_DBCONNECTION_ATTRIBUTE);
			}
		}        
		
		Charset chset = null;
		try {
			chset = Charset.forName(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		} catch (Exception e) {
			status.add(e.getMessage(), Severity.ERROR, this, Priority.NORMAL, XML_DBCONNECTION_ATTRIBUTE);
		}

		try {
			if (logFile != null && !FileUtils.canWrite(getGraph().getProjectURL(), logFile)) {
				status.add(new ConfigurationProblem("Can't write to " + logFile, Severity.WARNING, this, Priority.NORMAL, XML_LOG_FILE_ATTRIBUTE));
			}
		} catch (ComponentNotReadyException e) {
			status.add(new ConfigurationProblem(e, Severity.WARNING, this, Priority.NORMAL, XML_LOG_FILE_ATTRIBUTE));
		}
		
		EtlLogger log = new DumbLogger();
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
		try {
			loader = new InfobrightNamedPipeLoader(table, sqlConnection, log, dataFormat, chset);
			if (sqlConnection != null) {
				bRecord = createBrighthouseRecord(metadata, table, dbConnection.getJdbcSpecific(), chset, log);
			}
		} catch (Exception e) {
			status.add(e.getMessage(), Severity.ERROR, this, Priority.NORMAL);
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
		
		// get dbConnection from graph
	    if (dbConnection == null){
	        IConnection conn = getGraph().getConnection(connectionName);
            if(conn == null) {
                throw new ComponentNotReadyException("Can't find DBConnection ID: " + connectionName);
            }
            if(!(conn instanceof DBConnection)) {
                throw new ComponentNotReadyException("Connection with ID: " + connectionName + " isn't instance of the DBConnection class.");
            }
            dbConnection = (DBConnection) conn;
	    }
		if (!dbConnection.isInitialized()) {
			dbConnection.init();
		}        
		try {
			sqlConnection = dbConnection.getConnection(getId(), OperationType.WRITE).getSqlConnection();
		} catch (JetelException e) {
			throw new ComponentNotReadyException(this, XML_DBCONNECTION_ATTRIBUTE, e.getMessage());
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

		Charset chset = Charset.forName(charset != null ? charset : Defaults.DataFormatter.DEFAULT_CHARSET_ENCODER);
		
		EtlLogger log = new Logger(logger);
		try {
			loader = new InfobrightNamedPipeLoader(table, sqlConnection, log, dataFormat, chset);
			if (logFile != null) {
				loader.setDebugOutputStream(FileUtils.getOutputStream(getGraph().getProjectURL(), logFile, append, -1));
			}else if (getOutputPort(WRITE_TO_PORT) != null){
				dataParser = new DataParser(charset);
				dataParser.init(getOutputPort(WRITE_TO_PORT).getMetadata());
				PipedInputStream parserInput = new PipedInputStream();
				PipedOutputStream loaderOutput = new PipedOutputStream(parserInput);
				dataParser.setDataSource(parserInput);
				loader.setDebugOutputStream(loaderOutput);
			}
			if (pipeNamePrefix != null){
				loader.setPipeNamePrefix(pipeNamePrefix);
			}
			if (timeout > -1){
				loader.setTimeout(timeout);
			}
			bRecord = createBrighthouseRecord(metadata, table, dbConnection.getJdbcSpecific(), chset, log);
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, e);
		}
		converter = new CloverValueConverter();
	}

	private BrighthouseRecord createBrighthouseRecord(DataRecordMetadata metadata, String table, JdbcSpecific jdbcSpecific, Charset charset, 
			EtlLogger logger) throws Exception{
	    Statement stmt = sqlConnection.createStatement();
	    ResultSet rs = stmt.executeQuery("select * from `" + table + "` limit 0");
	    ResultSetMetaData md = rs.getMetaData();
	    if (md.getColumnCount() != cloverFieldIndexes.length) {
	    	throw new ComponentNotReadyException(this, "Number of db fields (" + md.getColumnCount()+ ") is different then " +
					"number of clover fields " + cloverFieldIndexes.length + ")." );
	    }
		List<AbstractColumnType> columns = new ArrayList<AbstractColumnType>(md.getColumnCount());
		for (int i = 0; i < cloverFieldIndexes.length; i++) {
			columns.add(jetelType2Brighthouse(metadata.getField(cloverFieldIndexes[i]), md.getPrecision(i + 1), jdbcSpecific, charset, logger));
		}
	    rs.close();
	    stmt.close();
		return dataFormat.createRecord(columns, charset, logger);
	}
	
	private AbstractColumnType jetelType2Brighthouse(DataFieldMetadata field, int precision, JdbcSpecific jdbcSpecific, Charset charset, EtlLogger logger) throws ComponentNotReadyException{
		String columnName = field.getName();
		String columnTypeName = field.getTypeAsString();
		switch (field.getType()) {
		case DataFieldMetadata.BOOLEAN_FIELD:
			logger.info(String.format("Column: %s %s(%d)", columnName, columnTypeName, 0));
			return new BooleanType();
		case DataFieldMetadata.BYTE_FIELD:
		case DataFieldMetadata.BYTE_FIELD_COMPRESSED:
			return AbstractColumnType.getInstance(columnName, field.isFixed() ? Types.BINARY : Types.VARBINARY, columnTypeName, 
					field.isFixed() ? field.getSize() : precision, 0, charset, logger);
		case DataFieldMetadata.DATE_FIELD:
			return AbstractColumnType.getInstance(columnName, jdbcSpecific.jetelType2sql(field), columnTypeName, 0, 0, charset, logger);
		case DataFieldMetadata.DECIMAL_FIELD:
			return AbstractColumnType.getInstance(columnName, Types.DECIMAL, columnTypeName, 
					field.getFieldProperties().getIntProperty(DataFieldMetadata.LENGTH_ATTR),
					field.getFieldProperties().getIntProperty(DataFieldMetadata.SCALE_ATTR), charset, logger);
		case DataFieldMetadata.INTEGER_FIELD:
			return AbstractColumnType.getInstance(columnName, Types.INTEGER, columnTypeName, precision, 0, charset, logger);
		case DataFieldMetadata.LONG_FIELD:
			return AbstractColumnType.getInstance(columnName, Types.BIGINT, columnTypeName, precision, 0, charset, logger);
		case DataFieldMetadata.NUMERIC_FIELD:
			return AbstractColumnType.getInstance(columnName, Types.DOUBLE, columnTypeName, precision, 0, charset, logger);
		case DataFieldMetadata.STRING_FIELD:
			return AbstractColumnType.getInstance(columnName, field.isFixed() ? Types.CHAR : Types.VARCHAR, columnTypeName, 
					field.isFixed() ? field.getSize() : precision, 0, charset, logger);
		default:
		      throw new ComponentNotReadyException(this, "Unsupported type (" + columnTypeName  + ") for column " + columnName);
		}
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#preExecute()
	 */
	@Override
	public void preExecute() throws ComponentNotReadyException {
		try {
			loader.start();
			output = loader.getOutputStream2();
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, e);
		}
		super.preExecute();
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord inRecord = new DataRecord(inPort.getMetadata());
		inRecord.init();
		while (runIt && (inRecord = inPort.readRecord(inRecord)) != null){
			for (int i = 0; i < cloverFieldIndexes.length; i++) {
				bRecord.setData(i, inRecord.getField(cloverFieldIndexes[i]), converter);
			}
			bRecord.writeTo(output);
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#postExecute(org.jetel.graph.TransactionMethod)
	 */
	@Override
	public void postExecute(TransactionMethod transactionMethod) throws ComponentNotReadyException {
		try {
			loader.stop();
			sqlConnection.commit();
		} catch (Exception e) {
			throw new ComponentNotReadyException(this, e);
		}
		super.postExecute(transactionMethod);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#getType()
	 */
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
	
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
        InfobrightDataWriter loader;

		try {
            loader = new InfobrightDataWriter(
                    xattribs.getString(XML_ID_ATTRIBUTE));
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
			return loader;
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
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
		this.dataFormat = dataFormat.equalsIgnoreCase(DataFormat.BINARY.getBhDataFormat()) ?
				DataFormat.BINARY : DataFormat.TXT_VARIABLE;
	}

	/**
	 * @param charset the charset to set
	 */
	public void setCharset(String charset) {
		this.charset = charset;
	}

	private static class Logger implements EtlLogger {

		Log logger;
		
		/**
		 * @param logger
		 */
		public Logger(Log logger) {
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
	
	private static class DumbLogger implements EtlLogger {

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#debug(java.lang.String)
		 */
		@Override
		public void debug(String s) {
			// TODO Auto-generated method stub
			
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#error(java.lang.String)
		 */
		@Override
		public void error(String s) {
			// TODO Auto-generated method stub
			
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#error(java.lang.String, java.lang.Throwable)
		 */
		@Override
		public void error(String s, Throwable cause) {
			// TODO Auto-generated method stub
			
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#fatal(java.lang.String)
		 */
		@Override
		public void fatal(String s) {
			// TODO Auto-generated method stub
			
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#info(java.lang.String)
		 */
		@Override
		public void info(String s) {
			// TODO Auto-generated method stub
			
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#trace(java.lang.String)
		 */
		@Override
		public void trace(String s) {
			// TODO Auto-generated method stub
			
		}

		/* (non-Javadoc)
		 * @see com.infobright.logging.EtlLogger#warn(java.lang.String)
		 */
		@Override
		public void warn(String s) {
			// TODO Auto-generated method stub
			
		}
		
	}
}
