/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Oracle data writer</h3>
 *
 * <!-- All records from input port:0 are loaded into oracle database. Connection to database is not through JDBC driver,
 * this component uses the sqlldr utility for this purpose. -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Oracle data writer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>All records from input port:0 are loaded into oracle database. Connection to the database is not via JDBC driver,
 * this component uses the sqlldr utility for this purpose.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"ORACLE_DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>sqlldr</b></td><td>path to slqldr utility</td></tr>
 *  <tr><td><b>username</b></td><td>user name for oracle database connection</td></tr>
 *  <tr><td><b>password</b></td><td>password for oracle database connection</td></tr>
 *  <tr><td><b>tnsname</b></td><td>tns name identifier</td></tr>
 *  <tr><td><b>append</b></td><td>type of dealing with empty vs. nonempty tables; options - insert, append, replace, truncate; default - append</td></tr>
 *  <tr><td><b>table</b></td><td>table name, where data are loaded</td></tr>
 *  <tr><td><b>log</b></td><td>log file name</td></tr>
 *  <tr><td><b>bad</b></td><td>name of file where records that cause errors are written</td></tr>
 *  <tr><td><b>discard</b></td><td>name of file where records not meeting selection criteria are written</td></tr>
 *  <tr><td><b>control</b></td><td>a control script for the sqlldr utility; if this parameter is empty default control script is used</td></tr>
 *  <tr><td><b>dbFields</b></td><td>name of all columns in db table</td></tr>
 *  </table>
 *
 * @author      Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
 * @revision    $Revision: 1388 $
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 */
public class OracleDataWriter extends Node {

    private static Log logger = LogFactory.getLog(OracleDataWriter.class);

    /**  Description of the Field */
    private static final String XML_SQLLDR_ATTRIBUTE = "sqlldr";
    private static final String XML_USERNAME_ATTRIBUTE = "username";
    private static final String XML_PASSWORD_ATTRIBUTE = "password";
    private static final String XML_TNSNAME_ATTRIBUTE = "tnsname";
    private static final String XML_APPEND_ATTRIBUTE = "append";
    private static final String XML_TABLE_ATTRIBUTE = "table";
    private static final String XML_LOG_ATTRIBUTE = "log";
    private static final String XML_BAD_ATTRIBUTE = "bad";
    private static final String XML_DISCARD_ATTRIBUTE = "discard";
    private static final String XML_CONTROL_ATTRIBUTE = "control";
    private static final String XML_DBFIELDS_ATTRIBUTE = "dbFields";
    private static final String XML_USE_FILE_FOR_EXCHANGE_ATTRIBUTE = "useFileForExchange"; // default value: Unix = false; win = true
    private static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
    
    private final static String lineSeparator = System.getProperty("line.separator");
    
    public final static String COMPONENT_TYPE = "ORACLE_DATA_WRITER";
    private final static int READ_FROM_PORT = 0;
    
    private final static String EXCHANGE_FILE_PREFIX = "oracleExchange";
    private final static String LOADER_FILE_NAME_PREFIX = "loader";
    private final static String CONTROL_FILE_NAME_SUFFIX = ".ctl";
    private final static File TMP_DIR = new File(".");
    private DataFormatter formatter = null;
    private DataConsumer consumer = null; // consume data from out stream of sqlldr
	private DataConsumer errConsumer; // consume data from err stream of sqlldr
    
    private String sqlldrPath;
    private String username;
    private String password;
    private String tnsname;
    private String userId;
    private String tableName;
    private Append append = Append.append;
    private String controlFileName;
    private String logFileName;
    private String badFileName;
    private String discardFileName;
    private String control; //contains user-defined control script fot sqlldr utility
    private String[] dbFields; // contains name of all database columns 
    private boolean useFileForExchange = false;
    private boolean isDefinedUseFileForExchange = false;
    private String dataURL; // fileUrl from XML - data file that is used when no input port is connected or for log
    
    private File dataFile = null; // file that is used for exchange data between clover and sqlldr - file from dataURL

    /**
	 * true - data is read from in port; 
	 * false - data is read from file directly by psql utility
	 */
	private boolean isDataReadFromPort;
	
	private boolean isDataReadDirectlyFromFile;
    
    /**
     * Constructor for the OracleDataWriter object
     *
     * @param  id  Description of the Parameter
     */
    public OracleDataWriter(String id, String sqlldrPath, String username, String password, String tnsname, String tableName) {
        super(id);
        this.sqlldrPath = sqlldrPath;
        this.username = username;
        this.password = password;
        this.tnsname = tnsname;
        this.tableName = tableName;
    }


    /**
     *  Main processing method for the SimpleCopy object
     *
     * @since    April 4, 2002
     */
    public Result execute() throws Exception {
		int processExitValue = 0;

		if (isDataReadFromPort) {
			if (useFileForExchange) { // dataFile is used for exchange data
				readFromPortAndWriteByFormatter();
				ProcBox box = createProcBox();
				processExitValue = box.join();
			} else { // data is send to process through named pipe
				processExitValue = runWithPipe();
			}
		} else {
			processExitValue = readDataDirectlyFromFile();
		}

		if (processExitValue != 0) {
			throw new JetelException("Sqlldr utility has failed. See log file for details.");
		}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }
    
    private int runWithPipe() throws Exception {
    	createNamedPipe();
    	ProcBox box = createProcBox();
		
		new Thread() {
			public void run() {
				try {
					readFromPortAndWriteByFormatter();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}.start();
		return box.join();
    }
    
    private void createNamedPipe() throws Exception {
    	try {
			Process proc = Runtime.getRuntime().exec("mkfifo " + dataFile.getCanonicalPath());
			ProcBox box = new ProcBox(proc, null, consumer, errConsumer);
			box.join();
		} catch (Exception e) {
			throw e;
		}
    }

	private void readFromPortAndWriteByFormatter() throws Exception {
		formatter.setDataTarget(new FileOutputStream(dataFile));
		
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(inPort.getMetadata());
		record.init();

		try {
			while (runIt && ((record = inPort.readRecord(record)) != null)) {
				formatter.write(record);
			}
		} catch (Exception e) {
			throw e;
		} finally {
			formatter.finish();
			formatter.close();
		}
	}
    
	/**
	 * Call sqlldr process with parameters - sqlldr process reads data directly from file.
	 * 
	 * @return value of finished process
	 * @throws Exception
	 */
	private int readDataDirectlyFromFile() throws Exception {
		ProcBox box = createProcBox();
		return box.join();
	}

	/**
	 * Create instance of ProcBox.
	 * 
	 * @param process running process; when process is null, default process is created
	 * @return instance of ProcBox
	 * @throws IOException
	 */
	private ProcBox createProcBox() throws IOException {
		Process process = Runtime.getRuntime().exec(createCommandlineForSqlldr());
		return new ProcBox(process, null, consumer, errConsumer);
	}
	
    @Override
    public synchronized void free() {
    	super.free();
    	
    	if (formatter != null) {
			formatter.close();
		}
    	
    	deleteControlFile();
    	deleteDataFile();
    }
    
    /**
     * Deletes temp file with loader script (CONTROL_FILE_NAME). 
     */
    private void deleteControlFile() {
    	if (controlFileName == null) {
        	return;
        }
    	
        File controlFile = new File(controlFileName);
        
        if (controlFile == null) {
        	return;
        }
        
        if (!controlFile.delete()) {
        	logger.warn("Control file was not deleted.");        	
        }
    }
    
    /**
	 * Deletes data file which was used for exchange data.
	 */
	private void deleteDataFile() {
		if (dataFile == null) {
			return;
		}
		
		if (isDataReadFromPort && dataURL == null ) {
			if (!dataFile.delete()) {
				logger.warn("Temp data file was not deleted.");
			}
    	}
    }
    
    /**
     * Create command line for process, where sqlldr utility is running.
     * Example: c:\Oracle\Client\bin\sqlldr.exe control=loader.ctl userid=user/password@schema log=loader.log bad=loader.bad data=\"=\"
     * @return
     */
    private String[] createCommandlineForSqlldr() {
        String[] ret = new String[] {
                sqlldrPath, 
                "control='" + controlFileName + "'", 
                "userid=" + userId,
                "data=" + getData(),
                logFileName != null ? "log='" + logFileName + "'" : "",
                badFileName != null ? "bad='" + badFileName + "'" : "",
                discardFileName != null ? "discard='" + discardFileName + "'" : ""
//                "silent=all"
        };
        
        logger.debug("System command: " + Arrays.toString(ret));
        return ret;
    }
    
    private String getData() {
    	if (dataFile != null) {
    		try { // canonical - /xx/xx -- absolute - /xx/./xx
				return "'" + dataFile.getCanonicalPath() + "'";
			} catch (IOException e) {
				return "'" + dataFile.getAbsolutePath() + "'";
			}
    	}
    	
    	// it is used only at windows;
    	// temp file or named pipe is used at unix 
    	return "\\\"-\\\"";
    }

    /**
     *  Description of the Method
     *
     * @exception  ComponentNotReadyException  Description of the Exception
     * @since                                  April 4, 2002
     */
    public void init() throws ComponentNotReadyException {
        if (isInitialized()) return;
        super.init();
  
        isDataReadFromPort = !getInPorts().isEmpty();
        isDataReadDirectlyFromFile = !isDataReadFromPort && 
        		!StringUtils.isEmpty(dataURL);
        
        // set undefined useFileForExchange when input port is connected
        if (!isDefinedUseFileForExchange && isDataReadFromPort) {
        	useFileForExchange = getDefaultUsingFileForExchange();
        }
        
        checkParams();
        
        // data is read directly from file -> file isn't used for exchange
    	if (isDataReadDirectlyFromFile) {
    		dataFile = openFile(dataURL);
    		useFileForExchange = false;
    	}
        
        createFileForExchange();
        createControlFile();
        
        //compute userId as sqlldr parameter
        userId = getUserId();
        
        //init of data formatter
        if (isDataReadFromPort) {
        	formatter = new DataFormatter();
            formatter.init(getInputPort(READ_FROM_PORT).getMetadata());
		}
        
		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);
		consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
    }
    
    /**
	 * Checks if mandatory parameters are defined.
	 * And check combination of some parameters.
	 * 
	 * @throws ComponentNotReadyException if any of conditions isn't fulfilled
	 */
	private void checkParams() throws ComponentNotReadyException {
		if (!isDataReadFromPort && StringUtils.isEmpty(dataURL)) {
			throw new ComponentNotReadyException(this, "Input port or " + 
					StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + 
					" attribute	have to be defined.");
		}
		
		if (isDataReadDirectlyFromFile && StringUtils.isEmpty(control)) {
			throw new ComponentNotReadyException(this, "When no input port " +
					"is connected then " + 
					StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + 
					" attribute and " +
					StringUtils.quote(XML_CONTROL_ATTRIBUTE) +
					" attribute have to be defined.");
		}
        
        if ((useFileForExchange && isDefinedUseFileForExchange) 
        		&& !isDataReadFromPort) {
        	logger.warn("When no port is connected" +
        			" (data is read directly from file) then " +
        			StringUtils.quote(XML_USE_FILE_FOR_EXCHANGE_ATTRIBUTE) + 
        			" attribute is omitted.");
        }
        
        if (!useFileForExchange && isDataReadFromPort && !StringUtils.isEmpty(dataURL)) {
        	logger.warn("When port is connected and " +
        			StringUtils.quote(XML_USE_FILE_FOR_EXCHANGE_ATTRIBUTE) + " attribute" +
        			" is set to false then " + StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + 
        			" attribute is omitted.");
        }
	}
    
    private void createFileForExchange() throws ComponentNotReadyException {
    	if (!useFileForExchange) {
    		if (!ProcBox.isWindowsPlatform() && isDataReadFromPort) {
    			dataFile = createTempFile();
    		}
   			return;
    	}
    	
		if (isDataReadFromPort) {
			if (ProcBox.isWindowsPlatform() || dataURL != null) {
				if (dataURL != null) {
					dataFile = new File(dataURL);
					dataFile.delete();
				} else {
					dataFile = createTempFile();
				}
			} else {
				dataFile = createTempFile();
				useFileForExchange = false;
			}
		}
    }
    
    private File createTempFile() throws ComponentNotReadyException {
    	try {
			File file = File.createTempFile(EXCHANGE_FILE_PREFIX, null, TMP_DIR);
			file.delete();
			return file;
		} catch (IOException e) {
			free();
			throw new ComponentNotReadyException(this, 
					"Temporary data file wasn't created.");
		}
    }
    
    private File openFile(String fileURL) throws ComponentNotReadyException {
    	if (!new File(fileURL).exists()) {
			free();
			throw new ComponentNotReadyException(this, 
					"Data file " + StringUtils.quote(fileURL) + " not exists.");
		}
		return new File(fileURL);
    }
    
    @Override
    public synchronized void reset() throws ComponentNotReadyException {
    	super.reset();
    	
    	if (formatter != null) {
			formatter.reset();
		}
    }
    
    /**
     * Creates userid parameter for sqlldr utility. Builds string this form: "user/password@schema" 
     * @return
     */
    private String getUserId() {
        return username + "/" + password + "@" + tnsname;
    }


    /**
     * Create new temp file with control script for sqlldr utility.
     * @throws ComponentNotReadyException
     */
    private void createControlFile() throws ComponentNotReadyException {
    	try {
            controlFileName = File.createTempFile(LOADER_FILE_NAME_PREFIX, CONTROL_FILE_NAME_SUFFIX, TMP_DIR).getCanonicalPath();
        } catch(IOException e) {
            throw new ComponentNotReadyException(this, "Control file cannot be created.");
        }
        
        File controlFile = new File(controlFileName);
        FileWriter controlWriter;
        try {
            controlFile.createNewFile();
            controlWriter = new FileWriter(controlFile);
            String content = control == null ? getDefaultControlFileContent(tableName, append, getInputPort(READ_FROM_PORT).getMetadata(), dbFields) : control;
            logger.debug("Control file content: " + content);
            controlWriter.write(content);
            controlWriter.close();
        } catch (IOException ex){
            throw new ComponentNotReadyException(this, "Control file for sqlldr utility can't be created.", ex);
        }
    }

    /**
     *  Description of the Method
     *
     * @return    Description of the Returned Value
     * @since     May 21, 2002
     */
    public void toXML(Element xmlElement) {
        //TODO
        super.toXML(xmlElement);
    }


    /**
     *  Description of the Method
     *
     * @param  nodeXML  Description of Parameter
     * @return          Description of the Returned Value
     * @since           May 21, 2002
     */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);

        try {
            OracleDataWriter oracleDataWriter = new OracleDataWriter(xattribs.getString(XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_SQLLDR_ATTRIBUTE),
                    xattribs.getString(XML_USERNAME_ATTRIBUTE),
                    xattribs.getString(XML_PASSWORD_ATTRIBUTE),
                    xattribs.getString(XML_TNSNAME_ATTRIBUTE),
                    xattribs.getString(XML_TABLE_ATTRIBUTE));
            if(xattribs.exists(XML_APPEND_ATTRIBUTE)) {
                oracleDataWriter.setAppend(Append.valueOf(xattribs.getString(XML_APPEND_ATTRIBUTE)));
            }
            if(xattribs.exists(XML_CONTROL_ATTRIBUTE)) {
                oracleDataWriter.setControl(xattribs.getString(XML_CONTROL_ATTRIBUTE));
            }
            if(xattribs.exists(XML_LOG_ATTRIBUTE)) {
                oracleDataWriter.setLogFileName(xattribs.getString(XML_LOG_ATTRIBUTE));
            }
            if(xattribs.exists(XML_BAD_ATTRIBUTE)) {
                oracleDataWriter.setBadFileName(xattribs.getString(XML_BAD_ATTRIBUTE));
            }
            if(xattribs.exists(XML_DISCARD_ATTRIBUTE)) {
                oracleDataWriter.setDiscardFileName(xattribs.getString(XML_DISCARD_ATTRIBUTE));
            }
            if(xattribs.exists(XML_DBFIELDS_ATTRIBUTE)) {
                oracleDataWriter.setDbFields(xattribs.getString(XML_DBFIELDS_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
            }
            if(xattribs.exists(XML_USE_FILE_FOR_EXCHANGE_ATTRIBUTE)) {
                oracleDataWriter.setUseFileForExchange(xattribs.getBoolean(XML_USE_FILE_FOR_EXCHANGE_ATTRIBUTE));
                oracleDataWriter.isDefinedUseFileForExchange = true;
            }
            if (xattribs.exists(XML_FILE_URL_ATTRIBUTE)) {
            	oracleDataWriter.setInDataFileName(xattribs.getString(XML_FILE_URL_ATTRIBUTE));
			}
            return oracleDataWriter;
        } catch (Exception ex) {
               throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
    }

    private void setDiscardFileName(String discardFileName) {
        this.discardFileName = discardFileName;
    }
       
    private void setBadFileName(String badFileName) {
        this.badFileName = badFileName;
    }

    private void setLogFileName(String logFileName) {
        this.logFileName = logFileName;
    }

    private void setControl(String control) {
        this.control = control;
    }

    private void setDbFields(String[] dbFields) {
        this.dbFields = dbFields;
    }
    
    private void setUseFileForExchange(boolean useFileForExchange) {
    	this.useFileForExchange = useFileForExchange;
    }
    
    private boolean getDefaultUsingFileForExchange() {
    	if (ProcBox.isWindowsPlatform()) {
    		return true;
    	} else {
    		return false;
    	}
    }
    
    private void setInDataFileName(String inDataFileName) {
		this.dataURL = inDataFileName;
	}
    
    /**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
         
        if (!checkInputPorts(status, 0, 1)
        		|| !checkOutputPorts(status, 0, 0)) {
        	return status;
        }

//        try {
//            init();
//            free();
//        } catch (ComponentNotReadyException e) {
//            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
//            if(!StringUtils.isEmpty(e.getAttributeName())) {
//                problem.setAttributeName(e.getAttributeName());
//            }
//            status.add(problem);
//        }
        
        return status;
    }
    
    public String getType(){
        return COMPONENT_TYPE;
    }

    public Append getAppend() {
        return append;
    }

    public void setAppend(Append append) {
        this.append = append;
    }
    
    /**
     * All type of dealing with empty vs. nonempty tables.
     * 
     * @author Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
     *
     */
    public enum Append {
        insert, append, replace, truncate
    }
    
    /**
     * Generates default control script.
     * 
     * Default script:
     * 
     * LOAD DATA
     * INFILE *
     * INTO TABLE <table_name>
     * APPEND/REPLACE
     * ( <list_of fields> )
     * 
     * @param tableName
     * @param append
     * @param metadata
     * @return
     * @throws ComponentNotReadyException
     */
    public static String getDefaultControlFileContent(String tableName, Append append, DataRecordMetadata metadata, String[] dbFields) throws ComponentNotReadyException {
        if(StringUtils.isEmpty(tableName)) tableName = "<table_name>";
        if(append == null) append = Append.append;
        return 
            "LOAD DATA" + lineSeparator +
            "INFILE *" + // is omitted 
            lineSeparator +
            "INTO TABLE " + tableName + lineSeparator +
            append + lineSeparator +
            "(" + lineSeparator + ((metadata != null) ? convertMetadataToControlForm(metadata, dbFields) : "") + lineSeparator + ")";
        
    }
    
    /**
     * Converts clover metadata into form required by sqlldr in control file.
     * Example:
     * 
     * field0 TERMINATED BY ',',
     * field1 TERMINATED BY '\r\n'
     * 
     * or
     * 
     * field0 POSITION (1:3),
     * field1 POSITION (4:14)
     * 
     * @param metadata
     * @return
     * @throws ComponentNotReadyException 
     */
    private static String convertMetadataToControlForm(DataRecordMetadata metadata, String[] dbFields) throws ComponentNotReadyException {
        if(metadata.getRecType() == DataRecordMetadata.MIXED_RECORD) {
            throw new ComponentNotReadyException("Mixed data record metadata can't be valid convert for the sqlldr utility usage.");
        }
        if(dbFields != null && dbFields.length != metadata.getNumFields()) {
            throw new ComponentNotReadyException("dbFields size has to be same as metadata size.");
        }
        
        StringBuilder ret = new StringBuilder();
        DataFieldMetadata[] fields = metadata.getFields();
        int fixlenCounter = 0;
        
        for(int i = 0; i < fields.length; i++) {
            ret.append('\t');
            if(dbFields != null) {
                ret.append(dbFields[i]);
            } else {
                ret.append(fields[i].getName());
            }
            if(fields[i].isDelimited()) {
                ret.append(" TERMINATED BY '");
                ret.append(StringUtils.specCharToString(fields[i].getDelimiters()[0]));
                ret.append('\'');
            } else { //fixlen field
                fixlenCounter++;
                ret.append(" POSITION (");
                ret.append(Integer.toString(fixlenCounter));
                ret.append(':');
                fixlenCounter += fields[i].getSize() - 1;
                ret.append(Integer.toString(fixlenCounter));
                ret.append(')');
            }
            ret.append(',');
            ret.append(lineSeparator);
        }
        ret.setLength(ret.length() - (1 + lineSeparator.length())); //remove comma delimiter and line separator in last field
        
        return ret.toString();
    }
}