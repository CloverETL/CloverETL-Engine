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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
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
    //private static final String XML_CONNECTION_ATTRIBUTE = "dbConnection";
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
    
    private final static String lineSeparator = System.getProperty("line.separator");
    
    public final static String COMPONENT_TYPE = "ORACLE_DATA_WRITER";
    private final static int READ_FROM_PORT = 0;
    
    private final static String LOADER_FILE_NAME_PREFIX = "loader";
    private final static String CONTROL_FILE_NAME_SUFFIX = ".ctl";
    private final static String LOG_FILE_NAME_SUFFIX = ".log";
    private final static String BAD_FILE_NAME_SUFFIX = ".bad";
    private final static String DISCARD_FILE_NAME_SUFFIX = ".dis";
    private final static File TMP_DIR = new File(".");
    private DataFormatter formatter;
    
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
        InputPort inPort = getInputPort(READ_FROM_PORT);
        DataRecord record = new DataRecord(inPort.getMetadata());
        record.init();

        Process process = null;

        //creating sqlldr process 
        process = Runtime.getRuntime().exec(createCommandlineForSqlldr());
        
        //inits of all process streams
        OutputStream processIn = new BufferedOutputStream(process.getOutputStream());
        InputStream processOut = new BufferedInputStream(process.getInputStream());
        InputStream processErr = new BufferedInputStream(process.getErrorStream());

        //init of data formatter
        formatter = new DataFormatter();
        formatter.init(inPort.getMetadata());
        formatter.setDataTarget(Channels.newChannel(processIn));
        
        //all stdout and stderr data I send into black hole
        StreamReader outStreamReader = new StreamReader(processOut);
        outStreamReader.start();
        StreamReader errStreamReader = new StreamReader(processErr);
        errStreamReader.start();
        
        //reading incoming data and sending them into sqlldr process
        try {
			while (record != null && runIt) {
			    record = inPort.readRecord(record);
			    if (record != null) {
			        formatter.write(record);
			    }
			}
		} catch (Exception e) {
			throw e;
		}finally{
	        //close data stream
	        formatter.close();
		}
        
        //waiting for sqlldr process termination
        if(process.waitFor() != 0) {
            deleteControlFile();
            throw new JetelException("Sqlldr utility has failed. See log file for details.");
        }

        //move to free() method
        deleteControlFile();
        
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
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
                (System.getProperty("os.name").startsWith("Windows") ? "data=\\\"-\\\"" : "data='-'"), 
                "log='" + logFileName + "'",
                "bad='" + badFileName + "'",
                "discard='" + discardFileName + "'",
//                "silent=all"
        };
        
        logger.debug("System command: " + Arrays.toString(ret));
        return ret;
    }

    /**
     *  Description of the Method
     *
     * @exception  ComponentNotReadyException  Description of the Exception
     * @since                                  April 4, 2002
     */
    public void init() throws ComponentNotReadyException {
		super.init();
  
		try {
            controlFileName = File.createTempFile(LOADER_FILE_NAME_PREFIX, CONTROL_FILE_NAME_SUFFIX, TMP_DIR).getAbsolutePath();
            
            if(logFileName == null)
                logFileName = File.createTempFile(LOADER_FILE_NAME_PREFIX, LOG_FILE_NAME_SUFFIX, TMP_DIR).getAbsolutePath();
            if(badFileName == null) 
                badFileName = File.createTempFile(LOADER_FILE_NAME_PREFIX, BAD_FILE_NAME_SUFFIX, TMP_DIR).getAbsolutePath();
            if(discardFileName == null)
                discardFileName = File.createTempFile(LOADER_FILE_NAME_PREFIX, DISCARD_FILE_NAME_SUFFIX, TMP_DIR).getAbsolutePath();
        } catch(IOException e) {
            throw new ComponentNotReadyException(this, "Some of the log files cannot be created.");
        }

        //create control file
        createControlFile();
        
        //compute userId as sqlldr parameter
        userId = getUserId();
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
        File controlFile = new File(controlFileName);
        FileWriter controlWriter;
        try {
            controlFile.createNewFile();
            controlWriter = new FileWriter(controlFile);
            String content = control == null ? getDefaultControlFileContent(tableName, append, getInputPort(READ_FROM_PORT).getMetadata(), dbFields) : control;
            logger.debug("Control file content: " + content);
            controlWriter.write(content);
            controlWriter.close();
        }catch(IOException ex){
            throw new ComponentNotReadyException(this, "Can't create temp control file for sqlldr utility.", ex);
        }
    }

    /**
     * Deletes temp file with loader script (CONTROL_FILE_NAME). 
     */
    private void deleteControlFile() {
        File controlFile = new File(controlFileName);
        controlFile.delete();
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
    
    /**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		checkInputPorts(status, 1, 1);
        checkOutputPorts(status, 0, 0);

        try {
            init();
            free();
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        }
        
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
     * Easy stream reader, substitutes /dev/null (black hole).
     * @author Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
     *
     */
    class StreamReader extends Thread {
        InputStream stream;
        
        public StreamReader(InputStream stream) {
            this.stream = stream;
        }
        
        @Override
        public void run() {
            try {
                while(stream.read() != -1);
            } catch (IOException e) {
                //doesn't matter
            }
        }
        
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
            "INFILE *" + lineSeparator +
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
                ret.append(StringUtils.specCharToString(fields[i].getDelimiter()));
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

