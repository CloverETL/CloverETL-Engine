package org.jetel.component;


import java.io.File;
import java.io.IOException;
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
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.DataProducer;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.PortDataProducer;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>MySQL data writer</h3>
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>MySQL data writer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Uses external program (myload.exe) to load records to mysql database. It is considarably faster
 * than JDBC-based load components. </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] - input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"MYSQL_DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>myload</b></td><td>path to myload executable (default myload).
 *  could be either absolute or relative to DEFAULT_BINARY_PATH</td></tr>
 *  <tr><td><b>username</b></td><td>user name for mysql database connection</td></tr>
 *  <tr><td><b>password</b></td><td>password for mysql database connection</td></tr>
 *  <tr><td><b>database</b></td><td>database name</td></tr>
 *  <tr><td><b>dbTable</b></td><td>mysql table to store the records</td></tr>
 *  <tr><td><b>hostname</b></td><td>mysql server hostname</td></tr>
 *  <tr><td><b>port</b></td><td>mysql server port</td></tr>
 *  <tr><td><b>replace</b></td><td>specifies duplicate records policy (true for replace, false for ignore)</td></tr>
 *  </table>
 *
 * TODO Handle '\t' and '\n' characters inside record fields.
 * 
 * @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
 * @since 09/05/06  
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 */
public class MysqlDataWriter extends Node {

	private static final String XML_MYLOAD_ATTRIBUTE = "myload";
	private static final String XML_USERNAME_ATTRIBUTE = "username";
	private static final String XML_PASSWORD_ATTRIBUTE = "password";
	private static final String XML_DATABASE_ATTRIBUTE = "database";
	private static final String XML_DBTABLE_ATTRIBUTE = "dbTable";
	private static final String XML_HOSTNAME_ATTRIBUTE = "hostname";
	private static final String XML_PORT_ATTRIBUTE = "port";
	private static final String XML_REPLACE_ATTRIBUTE = "replace";	
	
	private static final String COMPONENT_TYPE = "MYSQL_DATA_WRITER";

	private final static int INPUT_PORT = 0;

	private final static String DEFAULT_EXECUTABLE = "myload"; 

	private InputPort inPort;

	private String myload;
	private String user;
	private String pwd;
	private String dbase;
	private String table;
	private String host;
	private int port;
	private boolean replace;
	
	private Process proc;
	private DataProducer producer;
	private DataConsumer consumer;
	private DataConsumer errConsumer;
	
	static Log logger = LogFactory.getLog(MysqlDataReader.class);
	
	/**
	 * Sole ctor.
	 * @param id
	 * @param myload myload executable
	 * @param user DB user
	 * @param pwd user's password
	 * @param dbase database name
	 * @param table DB table to store records
	 * @param host DB server hostname
	 * @param port DB server port
	 * @param replace replace policy
	 */
	public MysqlDataWriter(String id, String myload, String user, String pwd, String dbase, String table,
			String host, int port, boolean replace) {
		super(id);
		this.myload = myload;
		this.user = user;
		this.pwd = pwd;
		this.dbase = dbase;
		this.table = table;
		this.host = host;
		this.port = port;
		this.replace = replace;
	}

	/**
	 * 
	 * @return argv-like command array
	 */
	private String[] getCmdArray() {

		StringBuffer param = new StringBuffer();
		List<String> cmdList = new ArrayList<String>();

		cmdList.add(getExec(myload));
		if (user != null) {
			param.setLength(0);
			param.append("--user=").append(user);
			cmdList.add(param.toString());
		}
		if (pwd != null) {
			param.setLength(0);
			param.append("--password=").append(pwd);
			cmdList.add(param.toString());
		}
		if (dbase != null) {
			param.setLength(0);
			param.append("--database=").append(dbase);
			cmdList.add(param.toString());
		}
		if (host != null) {
			param.setLength(0);
			param.append("--hostname=").append(host);
			cmdList.add(param.toString());
		}
		if (table != null) {
			param.setLength(0);
			param.append("--table=").append(table);
			cmdList.add(param.toString());
		}
		param.setLength(0);
		param.append("--port=").append(port);
		cmdList.add(param.toString());
		
		return cmdList.toArray(new String[cmdList.size()]);
	}
	
	private static String getExec(String myload) {
		if (myload == null) {
			myload = DEFAULT_EXECUTABLE;
		}
		File exec = new File(myload);
		if (!exec.isAbsolute()) {
			exec = new File(Defaults.DEFAULT_BINARY_PATH, myload);
		}
		return exec.getAbsolutePath();
	}

	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

		inPort = getInputPort(INPUT_PORT);
		if (inPort == null) {
			throw new ComponentNotReadyException(getId() + ": missing input port");			
		}

		String[] cmdArray = getCmdArray();

		StringBuffer msg = new StringBuffer("Executing command: \"");
		msg.append(cmdArray[0]).append("\" with parameters:\n");
		for (int idx = 1; idx < cmdArray.length; idx++) {
			msg.append(idx).append(": ").append(cmdArray[idx]).append("\n");
		}
		logger.info(msg.toString());

		try {
			DataRecordMetadata metadata = inPort.getMetadata().duplicate();
			// modify metadata so that they correspond to myload input format
			metadata.setRecType(DataRecordMetadata.DELIMITED_RECORD);
			for (int idx = 0; idx < metadata.getNumFields() - 1; idx++) {
				metadata.getField(idx).setDelimiter("\t");
			}
			metadata.getField(metadata.getNumFields() - 1).setDelimiter("\n");
			metadata.setRecordDelimiter(null);
			producer = new PortDataProducer(inPort, metadata);
			consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
			errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);
			proc = Runtime.getRuntime().exec(cmdArray);
		} catch (IOException e) {
			throw new ComponentNotReadyException(getId() + ": initialization failed: " + e.getMessage());
		}

	}
	
	@Override
	public Result execute() throws Exception {
		ProcBox pbox = new ProcBox(proc, producer, consumer, errConsumer);
		int retval = pbox.join();
		if (retval != 0) {
			logger.error(getId() + ": subprocess finished with error code " + retval);
			throw new JetelException(getId() + ": subprocess finished with error code " + retval);
		} else {
            return runIt ? Result.FINISHED_OK : Result.ABORTED;
		}
	}
	

	@Override
	public String getType() {
		return COMPONENT_TYPE;
	}

    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 0, 0);

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

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		MysqlDataWriter mysqlWriter;
		try {
			mysqlWriter = new MysqlDataWriter(xattribs.getString(XML_ID_ATTRIBUTE),
					xattribs.getString(XML_MYLOAD_ATTRIBUTE, null),
					xattribs.getString(XML_USERNAME_ATTRIBUTE, null),
					xattribs.getString(XML_PASSWORD_ATTRIBUTE, null),
					xattribs.getString(XML_DATABASE_ATTRIBUTE, null),
					xattribs.getString(XML_DBTABLE_ATTRIBUTE, null),
					xattribs.getString(XML_HOSTNAME_ATTRIBUTE, null),
					xattribs.getInteger(XML_PORT_ATTRIBUTE, 0),
					xattribs.getBoolean(XML_REPLACE_ATTRIBUTE, false));
			return mysqlWriter;
		} catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
		}
	}

	@Override public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		if (myload != null) {
			xmlElement.setAttribute(XML_MYLOAD_ATTRIBUTE, myload);
		}
		xmlElement.setAttribute(XML_USERNAME_ATTRIBUTE, user);
		xmlElement.setAttribute(XML_PASSWORD_ATTRIBUTE, pwd);
		xmlElement.setAttribute(XML_DATABASE_ATTRIBUTE, dbase);
		xmlElement.setAttribute(XML_DBTABLE_ATTRIBUTE, table);
		xmlElement.setAttribute(XML_HOSTNAME_ATTRIBUTE, dbase);
		xmlElement.setAttribute(XML_PORT_ATTRIBUTE, String.valueOf(port));
		xmlElement.setAttribute(XML_PORT_ATTRIBUTE, String.valueOf(port));
		xmlElement.setAttribute(XML_REPLACE_ATTRIBUTE, String.valueOf(replace));
	}
	
}
