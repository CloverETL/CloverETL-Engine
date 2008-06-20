/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
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
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/
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
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.FileDataConsumer;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.PortDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Oracle data reader</h3>
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Oracle data reader</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Uses external program (oradump.exe) to load records from oracle database and sends the records
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
 *  <tr><td><b>type</b></td><td>"ORACLE_DATA_READER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>oradump</b></td><td>path to oradump utility (default ./oradump).
 *  could be either absolute or relative to DEFAULT_BINARY_PATH</td></tr>
 *  <tr><td><b>username</b></td><td>user name for oracle database connection</td></tr>
 *  <tr><td><b>password</b></td><td>password for oracle database connection</td></tr>
 *  <tr><td><b>tnsname</b></td><td>tns name identifier</td></tr>
 *  <tr><td><b>sqlQuery</b></td><td>SQL statement to be used to retrieve data</td></tr>
 *  <tr><td><b>fetchSize</b></td><td>number of rows fetched from the DB engine in one step (default 1000)</td></tr>
 *  <tr><td><b>capturedErrorLines</b></td><td>number of lines that are print out if command finishes with errors (default 100)</td></tr>
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
public class OracleDataReader extends Node {

	private static final String XML_ORADUMP_ATTRIBUTE = "oradump";
	private static final String XML_USERNAME_ATTRIBUTE = "username";
	private static final String XML_PASSWORD_ATTRIBUTE = "password";
	private static final String XML_TNSNAME_ATTRIBUTE = "tnsname";
	private static final String XML_SQLQUERY_ATTRIBUTE = "sqlQuery";
	private static final String XML_FETCHSIZE_ATTRIBUTE = "fetchSize";
	
	private static final String XML_ERROR_LINES_ATTRIBUTE = "capturedErrorLines";
	private static final String XML_OUTPUT_FILE_ATTRIBUTE = "outputFile";
	private static final String XML_APPEND_ATTRIBUTE = "append";
	
	private static final int DEFAULT_ROWBUFFSIZE = 1000;
	private static final int DEFAULT_ERRLINES = 8;

	private static final String COMPONENT_TYPE = "ORACLE_DATA_READER";

	private final static int OUTPUT_PORT = 0;

	private final static String DEFAULT_EXECUTABLE = "oradump"; 

	private OutputPort outPort;
	private Writer fileWriter;

	private String oradump;
	private String user;
	private String pwd;
	private String dbase;
	private String sqlStmt;
	private int rowBuffSize;
	private int errLines;
	private String outputFile;
	private boolean append;
	
	private Process proc;
	private DataConsumer consumer;
	private DataConsumer errConsumer;
	
	static Log logger = LogFactory.getLog(OracleDataReader.class);

	/**
	 * Sol ctor
	 * @param id
	 * @param oradump oradump executable
	 * @param user DB user
	 * @param pwd user's password
	 * @param dbase tnsname of database
	 * @param sqlStmt SQL statement to be used for data selection
	 * @param rowBuffSize Number of rows retrieved by one fetch
	 * @param errorLinesNumber Number of lines of error output to be shown to the user. 
	 * @param outputFile Name of output file to be used to store output
	 * @param append Append mode for file writing.
	 */
	public OracleDataReader(String id, String oradump, String user, String pwd, String dbase, String sqlStmt,
			int rowBuffSize, int errorLinesNumber, String outputFile, boolean append) {
		super(id);
		this.oradump = oradump;
		this.user = user;
		this.pwd = pwd;
		this.dbase = dbase;
		this.sqlStmt = sqlStmt;
		this.rowBuffSize = rowBuffSize;
		this.errLines = errorLinesNumber;
		this.outputFile = outputFile;
		this.append = append;
	}

	/**
	 * 
	 * @param oradump
	 * @param user
	 * @param pwd
	 * @param tnsname
	 * @param sqlStmt
	 * @param rowBuffSize
	 * @param metadata
	 * @return argv-like command array 
	 */
	private static String[] getCmdArray(String oradump, String user, String pwd, String tnsname,
			String sqlStmt, int rowBuffSize, DataRecordMetadata metadata) {

		StringBuffer param = new StringBuffer();
		List<String> cmdList = new ArrayList<String>();

		cmdList.add(getExec(oradump));
		param.setLength(0);
		param.append("--array-size=");
		param.append(rowBuffSize);
		cmdList.add(param.toString());
		param.setLength(0);
		param.append("--select=");
		param.append(sqlStmt);
		cmdList.add(param.toString());
		param.setLength(0);
		param.append("--username=");
		param.append(user);
		cmdList.add(param.toString());
		param.setLength(0);
		param.append("--password=");
		param.append(pwd);
		cmdList.add(param.toString());
		param.setLength(0);
		param.append("--database=");
		param.append(tnsname);
		cmdList.add(param.toString(
				));
		if (metadata != null) {
			param.setLength(0);
			param.append("--record-delimiter=");
			//param.append(metadata.isSpecifiedRecordDelimiter() ? metadata.getRecordDelimiters()[0] : "");
			cmdList.add(param.toString());
			for (int i = 0; i < metadata.getNumFields(); i++) {
				param.setLength(0);
				DataFieldMetadata field = metadata.getField(i) ;
				if (field.isFixed()) {
					param.append(field.getSize());
				}
				param.append(",");
				param.append(field.getDelimiters() == null ? "" : field.getDelimiters()[0]);
				if (param.length() > 0) {
					cmdList.add(param.toString());
				}
			}
		}
		return cmdList.toArray(new String[cmdList.size()]);
	}
	
	/**
	 * @param oradump Relative or absolute path to oradump executable. 
	 * @return Absolute path to oradump.
	 */
	private static String getExec(String oradump) {
		if (oradump == null) {
			oradump = DEFAULT_EXECUTABLE;
		}
		File exec = new File(oradump);
		if (!exec.isAbsolute()) {
			exec = new File(Defaults.DEFAULT_BINARY_PATH, oradump);
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
		
		fileWriter = null;
		outPort = getOutputPort(OUTPUT_PORT);
		if (outPort == null && outputFile == null) {
			throw new ComponentNotReadyException(getId() + ": missing output");			
		}
		String[] cmdArray = getCmdArray(oradump, user, pwd, dbase, sqlStmt, rowBuffSize,
				outPort != null ? outPort.getMetadata() : null);

		StringBuffer msg = new StringBuffer("Executing command: \"");
		msg.append(cmdArray[0]).append("\" with parameters:\n");
		for (int idx = 1; idx < cmdArray.length; idx++) {
			msg.append(idx).append(": ").append(cmdArray[idx]).append("\n");
		}
		logger.info(msg.toString());

		try {
			proc = Runtime.getRuntime().exec(cmdArray);
			errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, errLines);
			if (outPort != null) {
				consumer = new PortDataConsumer(outPort);
			} else {
				fileWriter = new FileWriter(outputFile, append);
				consumer = new FileDataConsumer(fileWriter);
			}
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
		
		if (retval != 0) {
			logger.error(getId() + ": subprocess finished with error " + retval);			
			throw new JetelException("Subprocess finished with error " + retval);
		} else {
			return Result.FINISHED_OK;
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
		OracleDataReader oraReader;
		try {
			oraReader = new OracleDataReader(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_ORADUMP_ATTRIBUTE, null),
					xattribs.getString(XML_USERNAME_ATTRIBUTE, null),
					xattribs.getString(XML_PASSWORD_ATTRIBUTE, null),
					xattribs.getString(XML_TNSNAME_ATTRIBUTE, null),
					xattribs.getString(XML_SQLQUERY_ATTRIBUTE, null),
					xattribs.getInteger(XML_FETCHSIZE_ATTRIBUTE, DEFAULT_ROWBUFFSIZE),
					xattribs.getInteger(XML_ERROR_LINES_ATTRIBUTE, DEFAULT_ERRLINES),
					xattribs.getString(XML_OUTPUT_FILE_ATTRIBUTE, null),
					xattribs.getBoolean(XML_APPEND_ATTRIBUTE, false));
			return oraReader;
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}
	
	@Override public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		if (oradump != null) {
			xmlElement.setAttribute(XML_ORADUMP_ATTRIBUTE, oradump);
		}
		xmlElement.setAttribute(XML_USERNAME_ATTRIBUTE, user);
		xmlElement.setAttribute(XML_PASSWORD_ATTRIBUTE, pwd);
		xmlElement.setAttribute(XML_TNSNAME_ATTRIBUTE, dbase);
		xmlElement.setAttribute(XML_SQLQUERY_ATTRIBUTE, sqlStmt);
		xmlElement.setAttribute(XML_FETCHSIZE_ATTRIBUTE, String.valueOf(rowBuffSize));
		xmlElement.setAttribute(XML_ERROR_LINES_ATTRIBUTE, String.valueOf(errLines));
		if (outputFile!=null){
			xmlElement.setAttribute(XML_OUTPUT_FILE_ATTRIBUTE, outputFile);
			xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, String.valueOf(append));
		}
	}
	
}
