package org.jetel.component;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.Defaults;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.FileDataConsumer;
import org.jetel.util.exec.PortDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.exec.StringDataConsumer;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.QuotingDecoderMysql;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>MySQL data reader</h3>
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>MySQL data reader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Uses external program (mysql.exe) to load records from mysql database and sends the records
 * to first output port (or specified file). Arbitrary record delimiter, and field lengths and/or delimiters
 * can be set in the output metadata. Component's output will adjust to these settings.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]- output records</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"MYSQL_DATA_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>mysql</b></td><td>path to mysql client (default mysql).
 *  could be either absolute or relative to DEFAULT_BINARY_PATH</td></tr>
 *  <tr><td><b>hostname</b></td><td>mysql server hostname</td></tr>
 *  <tr><td><b>port</b></td><td>mysql server port</td></tr>
 *  <tr><td><b>username</b></td><td>user name for mysql database connection</td></tr>
 *  <tr><td><b>password</b></td><td>password for mysql database connection</td></tr>
 *  <tr><td><b>database</b></td><td>database name</td></tr>
 *  <tr><td><b>sqlQuery</b></td><td>SQL statement to be used to retrieve data</td></tr>
 *  <tr><td><b>fetchSize</b></td><td>number of rows fetched from the DB engine in one step (default 1000)</td></tr>
 *  <tr><td><b>outputFile</b></td><td>output file (could be used instead of output port)</td></tr>
 *  <tr><td><b>append</b></td><td>specifies whether pre-existing output file is to be appended or overwritten (default false)</td></tr>
 *  </table>
 *
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/05/06  
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 */
public class MysqlDataReader extends Node {

	private static final String XML_MYSQL_ATTRIBUTE = "mysql";
	private static final String XML_USERNAME_ATTRIBUTE = "username";
	private static final String XML_PASSWORD_ATTRIBUTE = "password";
	private static final String XML_DATABASE_ATTRIBUTE = "database";
	private static final String XML_SQLQUERY_ATTRIBUTE = "sqlQuery";
	private static final String XML_HOSTNAME_ATTRIBUTE = "hostname";
	private static final String XML_PORT_ATTRIBUTE = "port";	
	private static final String XML_OUTPUT_FILE_ATTRIBUTE = "outputFile";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	
	private static final String COMPONENT_TYPE = "MYSQL_DATA_READER";

	private final static int OUTPUT_PORT = 0;

	private final static String DEFAULT_EXECUTABLE = "mysql"; 

	private OutputPort outPort;
	private Writer fileWriter;

	private String mysql;
	private String user;
	private String pwd;
	private String dbase;
	private String sqlStmt;
	private String host;
	private int port; 
	private String outputFile;
	private boolean append;
	
	private Process proc;
	private DataConsumer consumer;
	private StringDataConsumer errConsumer;
	
	static Log logger = LogFactory.getLog(MysqlDataReader.class);
	
	/**
	 * Sole ctor.
	 * @param id
	 * @param mysql mysql executable
	 * @param user DB user
	 * @param pwd user's password
	 * @param dbase database name
	 * @param hostname DB server hostname
	 * @param port DB server port
	 * @param sqlStmt SQL statement to be used for data selection
	 * @param outputFile Name of output file to be used to store output
	 * @param append Append mode for file writing.
	 */
	public MysqlDataReader(String id, String mysql, String user, String pwd, String dbase, 
			String hostname, int port, String sqlStmt, String outputFile, boolean append) {
		super(id);
		this.mysql = mysql;
		this.user = user;
		this.pwd = pwd;
		this.dbase = dbase;
		this.host = hostname;
		this.port = port;
		this.sqlStmt = sqlStmt;
		this.outputFile = outputFile;
		this.append = append;
	}

	/**
	 * 
	 * @param mysql
	 * @param user
	 * @param pwd
	 * @param dbase
	 * @param sqlStmt
	 * @return argv-like command array
	 */
	private String[] getCmdArray() {

		StringBuffer param = new StringBuffer();
		List<String> cmdList = new ArrayList<String>();

		cmdList.add(getExec(mysql));
		cmdList.add("--batch");
		cmdList.add("--skip-column-names");
		if (user != null) {
			param.setLength(0);
			param.append("--user=");
			param.append(user);
			cmdList.add(param.toString());
		}
		if (pwd != null) {
			param.setLength(0);
			param.append("--password=");
			param.append(pwd);
			cmdList.add(param.toString());
		}
		if (dbase != null) {
			param.setLength(0);
			param.append("--database=");
			param.append(dbase);
			cmdList.add(param.toString());
		}
		if (host != null) {
			param.setLength(0);
			param.append("--host=");
			param.append(host);
			cmdList.add(param.toString());
		}
		if (port != 0) {
			param.setLength(0);
			param.append("--port=");
			param.append(port);
			cmdList.add(param.toString());
		}
		if (sqlStmt != null) {
			param.setLength(0);
			param.append("--execute=");
			param.append(sqlStmt);
			cmdList.add(param.toString());
		}


		return cmdList.toArray(new String[cmdList.size()]);
	}
	
	private static String getExec(String mysql) {
		if (mysql == null) {
			mysql = DEFAULT_EXECUTABLE;
		}
		File exec = new File(mysql);
		if (!exec.isAbsolute()) {
			exec = new File(Defaults.DEFAULT_BINARY_PATH, mysql);
		}
		return exec.getAbsolutePath();
	}

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 0, 0)
        		|| !checkOutputPorts(status, 0, 1)) {
        	return status;
        }

        try {
            init();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } finally {
        	free();
        }
        
        return status;
    }

	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		outPort = getOutputPort(OUTPUT_PORT);
		if (outPort == null && outputFile == null) {
			throw new ComponentNotReadyException(getId() + ": missing output");			
		}
		String[] cmdArray = getCmdArray();

		StringBuffer msg = new StringBuffer("Executing command: \"");
		msg.append(cmdArray[0]).append("\" with parameters:\n");
		for (int idx = 1; idx < cmdArray.length; idx++) {
			msg.append(idx).append(": ").append(cmdArray[idx]).append("\n");
		}
		logger.info(msg.toString());

		try {
			if (outPort != null) {				
				DataRecordMetadata metadata = outPort.getMetadata().duplicate();
				if (metadata.getRecType() != DataRecordMetadata.DELIMITED_RECORD) {
					throw new ComponentNotReadyException(getId() + ": delimited metadata expected");								
				}
				// modify metadata so that they correspond to mysql client output
				for (int idx = 0; idx < metadata.getNumFields() - 1; idx++) {
					metadata.getField(idx).setDelimiter("\t");
				}
				metadata.getField(metadata.getNumFields() - 1).setDelimiter("\n");
				consumer = new PortDataConsumer(outPort, metadata, new DelimitedDataParser(new QuotingDecoderMysql()));
			} else {
				fileWriter = new FileWriter(outputFile, append);
				consumer = new FileDataConsumer(fileWriter);
			}
			proc = Runtime.getRuntime().exec(cmdArray);
			errConsumer = new StringDataConsumer(0);
		} catch (IOException e) {
			throw new ComponentNotReadyException(getId() + ": initialization failed: " + e.getMessage());
		}

	}
	
	@Override
	public Result execute() throws Exception {
		ProcBox pbox = new ProcBox(proc, null, consumer, errConsumer);
		int retval;
		try {
			retval = pbox.join();
		} catch (InterruptedException e1) {
			throw e1;
		}
		
		String resultMsg = errConsumer.getMsg();
		if (retval != 0) {
			logger.error(getId() + ": subprocess finished with error code " + retval + "\n" + resultMsg);
			throw new JetelException(resultMsg);
		} else {
            return runIt ? Result.FINISHED_OK : Result.ABORTED;
		}
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		
		// DO NOTHING
	}
	
	@Override
	public synchronized void free() {
		if(!isInitialized()) return;
		super.free();
		
		if (outPort != null) {
			try {
				outPort.close();
			} catch (Exception e) {
				logger.warn("Unable to close output port " + outPort, e);
			}
		} else {
			try {
				fileWriter.close();
			} catch (IOException e) {
				logger.warn("Unable to close output file " + outputFile, e);
			}
		}
	}

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		MysqlDataReader mysqlReader;
		try {
			mysqlReader = new MysqlDataReader(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_MYSQL_ATTRIBUTE, null),
					xattribs.getString(XML_USERNAME_ATTRIBUTE, null),
					xattribs.getString(XML_PASSWORD_ATTRIBUTE, null),
					xattribs.getString(XML_DATABASE_ATTRIBUTE, null),
					xattribs.getString(XML_HOSTNAME_ATTRIBUTE, null),
					xattribs.getInteger(XML_PORT_ATTRIBUTE, 0),
					xattribs.getString(XML_SQLQUERY_ATTRIBUTE, null),
					xattribs.getString(XML_OUTPUT_FILE_ATTRIBUTE, null),
					xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false));
			return mysqlReader;
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}
	
	@Override public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		if (mysql != null) {
			xmlElement.setAttribute(XML_MYSQL_ATTRIBUTE, mysql);
		}
		xmlElement.setAttribute(XML_USERNAME_ATTRIBUTE, user);
		xmlElement.setAttribute(XML_PASSWORD_ATTRIBUTE, pwd);
		xmlElement.setAttribute(XML_DATABASE_ATTRIBUTE, dbase);
		xmlElement.setAttribute(XML_HOSTNAME_ATTRIBUTE, host);
		if (port != 0) {
			xmlElement.setAttribute(XML_HOSTNAME_ATTRIBUTE, String.valueOf(port));
		}
		xmlElement.setAttribute(XML_SQLQUERY_ATTRIBUTE, sqlStmt);
		if (outputFile!=null){
			xmlElement.setAttribute(XML_OUTPUT_FILE_ATTRIBUTE, outputFile);
			xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(append));
		}
	}
	
}
