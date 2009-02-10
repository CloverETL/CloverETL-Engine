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
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPort;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.file.FileUtils;
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
 *  <tr><td><b>maxErrors</b></td><td>number of errors to allow</td></tr>
 *  <tr><td><b>maxDiscards</b></td><td>number of discards to allow</td></tr>
 *  <tr><td><b>ignoreRows</b></td><td>number of logical records to skip</td></tr>
 *  <tr><td><b>commitInterval</b></td><td>number of rows in conventional path bind array or between direct path data saves</td></tr>
 *  <tr><td><b>parameters</b></td><td>See: http://wiki.cloveretl.org/doku.php?id=components:bulkloaders:oracle_data_writer</td></tr>
 *  </table>
 *
 * @author      Martin Zatopek, Javlin Consulting (www.javlinconsulting.cz)
 * @revision    $Revision: 1388 $
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 */
public class OracleDataWriter extends BulkLoader {

    private static Log logger = LogFactory.getLog(OracleDataWriter.class);

    /**  Description of the Field */
    private static final String XML_SQLLDR_ATTRIBUTE = "sqlldr";
    private static final String XML_TNSNAME_ATTRIBUTE = "tnsname";
    private static final String XML_APPEND_ATTRIBUTE = "append";
    private static final String XML_LOG_ATTRIBUTE = "log";
    private static final String XML_BAD_ATTRIBUTE = "bad";
    private static final String XML_DISCARD_ATTRIBUTE = "discard";
    private static final String XML_CONTROL_ATTRIBUTE = "control";
    private static final String XML_DBFIELDS_ATTRIBUTE = "dbFields";
    private static final String XML_USE_FILE_FOR_EXCHANGE_ATTRIBUTE = "useFileForExchange"; // default value: Unix = false; win = true
    private static final String XML_MAX_ERRORS_ATTRIBUTE = "maxErrors";
    private static final String XML_MAX_DISCARDS_ATTRIBUTE = "maxDiscards";
    private static final String XML_IGNORE_ROWS_ATTRIBUTE = "ignoreRows";
    private static final String XML_COMMIT_INTERVAL_ATTRIBUTE = "commitInterval";

    // keywords for sqlldr client, these keywords have own xml attributes
    private static final String SQLLDR_MAX_ERRORS_KEYWORD = "errors";
    private static final String SQLLDR_MAX_DISCARDS_KEYWORD = "discardmax";
    private static final String SQLLDR_IGNORE_ROWS_KEYWORD = "skip";
    private static final String SQLLDR_COMMIT_INTERVAL_KEYWORD = "rows";

    // keywords for sqlldr client
	private static final String SQLLDR_RECORD_COUNT_KEYWORD = "load";
	private static final String SQLLDR_BIND_SIZE_KEYWORD = "bindsize";
	private static final String SQLLDR_SILENT_KEYWORD = "silent";
	private static final String SQLLDR_DIRECT_KEYWORD = "direct";
	private static final String SQLLDR_PARALLEL_KEYWORD = "parallel";
	private static final String SQLLDR_FILE_KEYWORD = "file";
	private static final String SQLLDR_SKIP_UNUSABLE_INDEX_KEYWORD = "skip_unusable_indexes";
	private static final String SQLLDR_SKIP_INDEX_MAINTEANCE_KEYWORD = "skip_index_maintenance";
	private static final String SQLLDR_COMMIT_DISCONTINUED_KEYWORD = "commit_discontinued";
	private static final String SQLLDR_READ_SIZE_KEYWORD = "readsize";
	private static final String SQLLDR_EXTERNAL_TABLE_KEYWORD = "external_table";
	private static final String SQLLDR_COLUMNARRAYROWS_KEYWORD = "columnarrayrows";
	private static final String SQLLDR_STREAM_SIZE_KEYWORD = "streamsize";
	private static final String SQLLDR_MULTITHREADING_KEYWORD = "multithreading";  
	private static final String SQLLDR_RESUMABLE_KEYWORD = "resumable";
	private static final String SQLLDR_RESUMABLE_NAME_KEYWORD = "resumable_name";
	private static final String SQLLDR_RESUMABLE_TIMEOUT_KEYWORD = "resumable_timeout";
	private static final String SQLLDR_DATA_CACHE_KEYWORD = "date_cache";
    
    // params for sqlldr client - it's included in parameters attribute
	private static final String SQLLDR_RECORD_COUNT_PARAM = "recordCount";
	private static final String SQLLDR_BIND_SIZE_PARAM = "bindSize";
	private static final String SQLLDR_SILENT_PARAM = SQLLDR_SILENT_KEYWORD;
	private static final String SQLLDR_DIRECT_PARAM = SQLLDR_DIRECT_KEYWORD;
	private static final String SQLLDR_PARALLEL_PARAM = SQLLDR_PARALLEL_KEYWORD;
	private static final String SQLLDR_FILE_PARAM = SQLLDR_FILE_KEYWORD;
	private static final String SQLLDR_SKIP_UNUSABLE_INDEX_PARAM = "skipUnusableIndexes";
	private static final String SQLLDR_SKIP_INDEX_MAINTEANCE_PARAM = "skipIndexMaintenance";
	private static final String SQLLDR_COMMIT_DISCONTINUED_PARAM = "commitDiscontinued";
	private static final String SQLLDR_READ_SIZE_PARAM = "readSize";
	private static final String SQLLDR_EXTERNAL_TABLE_PARAM = "externalTable";
	private static final String SQLLDR_COLUMNARRAYROWS_PARAM = "columnArrayRows";
	private static final String SQLLDR_STREAM_SIZE_PARAM = "streamSize";
	private static final String SQLLDR_MULTITHREADING_PARAM = "multithreading";  
	private static final String SQLLDR_RESUMABLE_PARAM = SQLLDR_RESUMABLE_KEYWORD;
	private static final String SQLLDR_RESUMABLE_NAME_PARAM = "resumableName";
	private static final String SQLLDR_RESUMABLE_TIMEOUT_PARAM = "resumableTimeout";
	private static final String SQLLDR_DATA_CACHE_PARAM = "dateCache";
	
    private final static String lineSeparator = System.getProperty("line.separator");
    
    public final static String COMPONENT_TYPE = "ORACLE_DATA_WRITER";
    
    private final static String EXCHANGE_FILE_PREFIX = "oracleExchange";
    private final static String LOADER_FILE_NAME_PREFIX = "loader";
    
    private final static int EXEC_SQLLDR_SUCC = 0;
    private final static int EXEC_SQLLDR_WARN = 2;
    
	private OracleBadRowReaderWriter oracleBadRowReaderWriter;
    
    private String tnsname;
    private String userId;
    private Append append = Append.append;
    private String controlFileName;
    private String logFileName;
    private String badFileName;
    private String discardFileName;
    private String control; //contains user-defined control script fot sqlldr utility
    private String[] dbFields; // contains name of all database columns 
    private boolean useFileForExchange = false;
    private boolean isDefinedUseFileForExchange = false;
    private int maxErrors = UNUSED_INT;
    private int maxDiscards = UNUSED_INT;
    private int ignoreRows = UNUSED_INT;
    private int commitInterval = UNUSED_INT;
    
    private String[] commandLine; // command line of sqlldr
    private File badFile = null;
    private File discardFile = null;

    /**
	 *  flag that determine if execute() method was already executed;
	 *  used for deleting temp data file and reporting about it
	 */
	private boolean alreadyExecuted = false;
    
	private boolean isDataReadDirectlyFromFile;
    
    /**
     * Constructor for the OracleDataWriter object
     *
     * @param  id  Description of the Parameter
     */
    public OracleDataWriter(String id, String sqlldrPath, String username, String password, String tnsname, String tableName) {
        super(id, sqlldrPath, null);
        this.user = username;
        this.password = password;
        this.tnsname = tnsname;
        this.table = tableName;
    }


    /**
     *  Main processing method for the SimpleCopy object
     *
     * @since    April 4, 2002
     */
    public Result execute() throws Exception {
    	alreadyExecuted = true;
		int processExitValue = 0;
		boolean unstableStdinIsUsed = false;

		if (isDataReadFromPort) {
			if (useFileForExchange) { // dataFile is used for exchange data
				readFromPortAndWriteByFormatter();
				ProcBox box = createProcBox();
				processExitValue = box.join();
			} else { // data is send to process through stdin of sqlldr
				if (ProcBox.isWindowsPlatform()) {
					unstableStdinIsUsed = true;
					Process process = Runtime.getRuntime().exec(commandLine);
		            ProcBox box = createProcBox(process);
		            
		            // stdin (-) is used for exchange data - set data target to stdin (-) of process
		        	OutputStream processIn = new BufferedOutputStream(process.getOutputStream());
		        	readFromPortAndWriteByFormatter(processIn);
		        	processExitValue = box.join();
				} else { // data is send to process through named pipe
					processExitValue = runWithPipe();
				}
			}
		} else {
			processExitValue = readDataDirectlyFromFile();
		}

		switch (processExitValue) {
		case EXEC_SQLLDR_SUCC:
			logger.info("Sqlldr utility execution successful.");
			break;
		case EXEC_SQLLDR_WARN:
			logger.warn("Sqlldr utility exited with WARN. See log file for details.");
			if (isDataWrittenToPort) {
				oracleBadRowReaderWriter.run();
			}
			break;
		default:
			if (unstableStdinIsUsed) {
				throw new JetelException("Sqlldr utility has failed. See log file for details.\n" +
						"You are using stdio of sqlldr that in mostly case of combination " +
						"(OS and oracle client) failed. Set " + 
						StringUtils.quote(XML_USE_FILE_FOR_EXCHANGE_ATTRIBUTE+"=true") +
						" to resolve the problem.");
			}
			throw new JetelException("Sqlldr utility has failed. See log file for details.");
		}

		if (isDataWrittenToPort) {
    		oracleBadRowReaderWriter.close();
    	}

		return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }
    
	/**
	 * Create instance of ProcBox.
	 * 
	 * @param process running process; when process is null, default process is created
	 * @return instance of ProcBox
	 * @throws IOException
	 */
	// TODO this method will be deleted from this class after removing commandLine attribute to superclass
	protected ProcBox createProcBox(Process process) throws IOException {
		if (process == null) {
			process = Runtime.getRuntime().exec(commandLine);
		}
		return new ProcBox(process, null, consumer, errConsumer);
	}
	
    @Override
    public synchronized void free() {
    	if(!isInitialized()) return;
    	super.free();

    	deleteFile(controlFileName, logger);
    	deleteDataFile();
    	alreadyExecuted = false;
    }

    /**
	 * Deletes data file which was used for exchange data.
	 */
	private void deleteDataFile() {
		if (dataFile == null) {
			return;
		}
		
		if (!alreadyExecuted) {
			return;
		}
		
		if (isDataReadFromPort && dataURL == null && !dataFile.delete()) {
			logger.warn("Temp data file was not deleted.");
    	}
    }

    /**
     * Create command line for process, where sqlldr utility is running.
     * Example: c:\Oracle\Client\bin\sqlldr.exe control=loader.ctl userid=user/password@schema log=loader.log bad=loader.bad data=\"=\"
     * @return
     */
    private String[] createCommandlineForSqlldr() {
    	OracleCommandBuilder cmdBuilder = new OracleCommandBuilder(loadUtilityPath, properties);
    	cmdBuilder.addAttribute("control", controlFileName, true);
    	cmdBuilder.addAttribute("userid", userId, false);
    	cmdBuilder.addAttribute("data", getData(), false);
    	cmdBuilder.addAttribute("log", logFileName, true);
    	cmdBuilder.addAttribute("bad", badFileName, true);
    	cmdBuilder.addAttribute("discard", discardFileName, true);
    	
    	cmdBuilder.addAttribute(SQLLDR_MAX_ERRORS_KEYWORD, maxErrors);
    	cmdBuilder.addAttribute(SQLLDR_MAX_DISCARDS_KEYWORD, maxDiscards);
    	cmdBuilder.addAttribute(SQLLDR_IGNORE_ROWS_KEYWORD, ignoreRows);
    	cmdBuilder.addAttribute(SQLLDR_COMMIT_INTERVAL_KEYWORD, commitInterval);
    	
    	// add parameters
    	cmdBuilder.addParam(SQLLDR_RECORD_COUNT_PARAM ,SQLLDR_RECORD_COUNT_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_BIND_SIZE_PARAM ,SQLLDR_BIND_SIZE_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_SILENT_PARAM ,SQLLDR_SILENT_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_DIRECT_PARAM ,SQLLDR_DIRECT_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_PARALLEL_PARAM ,SQLLDR_PARALLEL_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_FILE_PARAM ,SQLLDR_FILE_KEYWORD, true);
    	cmdBuilder.addParam(SQLLDR_SKIP_UNUSABLE_INDEX_PARAM ,SQLLDR_SKIP_UNUSABLE_INDEX_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_SKIP_INDEX_MAINTEANCE_PARAM ,SQLLDR_SKIP_INDEX_MAINTEANCE_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_COMMIT_DISCONTINUED_PARAM ,SQLLDR_COMMIT_DISCONTINUED_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_READ_SIZE_PARAM ,SQLLDR_READ_SIZE_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_EXTERNAL_TABLE_PARAM ,SQLLDR_EXTERNAL_TABLE_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_COLUMNARRAYROWS_PARAM ,SQLLDR_COLUMNARRAYROWS_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_STREAM_SIZE_PARAM ,SQLLDR_STREAM_SIZE_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_MULTITHREADING_PARAM ,SQLLDR_MULTITHREADING_KEYWORD, false);  
    	cmdBuilder.addParam(SQLLDR_RESUMABLE_PARAM ,SQLLDR_RESUMABLE_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_RESUMABLE_NAME_PARAM ,SQLLDR_RESUMABLE_NAME_KEYWORD, true);
    	cmdBuilder.addParam(SQLLDR_RESUMABLE_TIMEOUT_PARAM ,SQLLDR_RESUMABLE_TIMEOUT_KEYWORD, false);
    	cmdBuilder.addParam(SQLLDR_DATA_CACHE_PARAM ,SQLLDR_DATA_CACHE_KEYWORD, false);
    	
    	String[] ret = cmdBuilder.getCommand();
    	
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
        
        commandLine = createCommandlineForSqlldr();
        
        //init of data formatter
        if (isDataReadFromPort) {
        	dbMetadata = getInputPort(READ_FROM_PORT).getMetadata();
        	
        	formatter = new DataFormatter();
            formatter.init(dbMetadata);
		}
        
		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);
		consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);

		if (isDataWrittenToPort) {
			getRejectedAndDiscardedFile();
			oracleBadRowReaderWriter = new OracleBadRowReaderWriter();
    	}
    }

    private void getRejectedAndDiscardedFile() throws ComponentNotReadyException {
    	String name = dataFile.getName();
		String baseName = name.substring(0, name.lastIndexOf("."));
    	
		badFile = createFile(getGraph().getProjectURL(), badFileName, baseName, "bad");
		discardFile = createFile(getGraph().getProjectURL(), discardFileName, baseName, "dis");

    }
    
    private static File createFile(URL projectURL, String fileName, String baseName, 
    		String extension) throws ComponentNotReadyException {
    	
    	if (StringUtils.isEmpty(fileName)) {
    		try {
				return new File(FileUtils.getFile(projectURL, baseName + "." + extension));
			} catch (MalformedURLException e) {
				throw new ComponentNotReadyException(e);
			}
    	}
    	
    	return new File(fileName);
    }
    
    /**
	 * Checks if mandatory attributes are defined.
	 * And check combination of some parameters.
	 * 
	 * @throws ComponentNotReadyException if any of conditions isn't fulfilled
	 */
	private void checkParams() throws ComponentNotReadyException {
		if (StringUtils.isEmpty(loadUtilityPath)) {
			throw new ComponentNotReadyException(this, StringUtils.quote(XML_SQLLDR_ATTRIBUTE)
					+ " attribute have to be set.");
		}
		
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
    			dataFile = createTempFile(EXCHANGE_FILE_PREFIX);
    		}
   			return;
    	}
    	
		if (isDataReadFromPort) {
			if (ProcBox.isWindowsPlatform() || dataURL != null) {
				if (dataURL != null) {
					dataFile = new File(dataURL);
					dataFile.delete();
				} else {
					dataFile = createTempFile(EXCHANGE_FILE_PREFIX);
				}
			} else {
				dataFile = createTempFile(EXCHANGE_FILE_PREFIX);
				useFileForExchange = false;
			}
		}
    }

    /**
     * Creates userid parameter for sqlldr utility. Builds string this form: "user/password@schema" 
     * @return
     */
    private String getUserId() {
        return user + "/" + password + "@" + tnsname;
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
            String content = control == null ? getDefaultControlFileContent(table, append, getInputPort(READ_FROM_PORT).getMetadata(), dbFields) : control;
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
        super.toXML(xmlElement);
      
        xmlElement.setAttribute(XML_SQLLDR_ATTRIBUTE, loadUtilityPath);
        xmlElement.setAttribute(XML_TNSNAME_ATTRIBUTE, tnsname);
		xmlElement.setAttribute(XML_APPEND_ATTRIBUTE, append.name());
		
        if (!StringUtils.isEmpty(control)) {
			xmlElement.setAttribute(XML_CONTROL_ATTRIBUTE, control);
        }
        if (!StringUtils.isEmpty(logFileName)) {
			xmlElement.setAttribute(XML_LOG_ATTRIBUTE, logFileName);
        }
        if (!StringUtils.isEmpty(badFileName)) {
			xmlElement.setAttribute(XML_BAD_ATTRIBUTE, badFileName);
        }
        if (!StringUtils.isEmpty(discardFileName)) {
			xmlElement.setAttribute(XML_DISCARD_ATTRIBUTE, discardFileName);
        }
        if (dbFields != null) {
			String keys = dbFields[0];
			for (int i = 1; i < dbFields.length; i++) {
				keys += Defaults.Component.KEY_FIELDS_DELIMITER + dbFields[i];
			}
			xmlElement.setAttribute(XML_DBFIELDS_ATTRIBUTE, keys);
        }
        if (isDefinedUseFileForExchange) {
			xmlElement.setAttribute(XML_USE_FILE_FOR_EXCHANGE_ATTRIBUTE, String.valueOf(useFileForExchange));
        }
        if (maxErrors != UNUSED_INT) {
			xmlElement.setAttribute(XML_MAX_ERRORS_ATTRIBUTE, String.valueOf(maxErrors));
        }
        if (maxDiscards != UNUSED_INT) {
			xmlElement.setAttribute(XML_MAX_DISCARDS_ATTRIBUTE, String.valueOf(maxDiscards));
        }
        if (ignoreRows != UNUSED_INT) {
			xmlElement.setAttribute(XML_IGNORE_ROWS_ATTRIBUTE, String.valueOf(ignoreRows));
        }
        if (commitInterval != UNUSED_INT) {
			xmlElement.setAttribute(XML_COMMIT_INTERVAL_ATTRIBUTE, String.valueOf(commitInterval));
        }
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
                    xattribs.getString(XML_USER_ATTRIBUTE),
                    xattribs.getString(XML_PASSWORD_ATTRIBUTE),
                    xattribs.getString(XML_TNSNAME_ATTRIBUTE),
                    xattribs.getString(XML_TABLE_ATTRIBUTE));
            if (xattribs.exists(XML_APPEND_ATTRIBUTE)) {
                oracleDataWriter.setAppend(Append.valueOf(xattribs.getString(XML_APPEND_ATTRIBUTE)));
            }
            if (xattribs.exists(XML_CONTROL_ATTRIBUTE)) {
                oracleDataWriter.setControl(xattribs.getString(XML_CONTROL_ATTRIBUTE));
            }
            if (xattribs.exists(XML_LOG_ATTRIBUTE)) {
                oracleDataWriter.setLogFileName(xattribs.getString(XML_LOG_ATTRIBUTE));
            }
            if (xattribs.exists(XML_BAD_ATTRIBUTE)) {
                oracleDataWriter.setBadFileName(xattribs.getString(XML_BAD_ATTRIBUTE));
            }
            if (xattribs.exists(XML_DISCARD_ATTRIBUTE)) {
                oracleDataWriter.setDiscardFileName(xattribs.getString(XML_DISCARD_ATTRIBUTE));
            }
            if (xattribs.exists(XML_DBFIELDS_ATTRIBUTE)) {
                oracleDataWriter.setDbFields(xattribs.getString(XML_DBFIELDS_ATTRIBUTE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
            }
            if (xattribs.exists(XML_USE_FILE_FOR_EXCHANGE_ATTRIBUTE)) {
                oracleDataWriter.setUseFileForExchange(xattribs.getBoolean(XML_USE_FILE_FOR_EXCHANGE_ATTRIBUTE));
                oracleDataWriter.isDefinedUseFileForExchange = true;
            }
            if (xattribs.exists(XML_FILE_URL_ATTRIBUTE)) {
            	oracleDataWriter.setFileUrl(xattribs.getString(XML_FILE_URL_ATTRIBUTE));
			}
            if (xattribs.exists(XML_MAX_ERRORS_ATTRIBUTE)) {
            	oracleDataWriter.setMaxErrors(xattribs.getInteger(XML_MAX_ERRORS_ATTRIBUTE));
			}
            if (xattribs.exists(XML_MAX_DISCARDS_ATTRIBUTE)) {
            	oracleDataWriter.setMaxDiscards(xattribs.getInteger(XML_MAX_DISCARDS_ATTRIBUTE));
			}
            if (xattribs.exists(XML_IGNORE_ROWS_ATTRIBUTE)) {
            	oracleDataWriter.setIgnoreRows(xattribs.getInteger(XML_IGNORE_ROWS_ATTRIBUTE));
			}
            if (xattribs.exists(XML_COMMIT_INTERVAL_ATTRIBUTE)) {
            	oracleDataWriter.setCommitInterval(xattribs.getInteger(XML_COMMIT_INTERVAL_ATTRIBUTE));
			}
			if (xattribs.exists(XML_PARAMETERS_ATTRIBUTE)) {
				oracleDataWriter.setParameters(xattribs.getString(XML_PARAMETERS_ATTRIBUTE));
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
    	return ProcBox.isWindowsPlatform();
    }
    
    private void setMaxErrors(int maxErrors) {
		this.maxErrors = maxErrors;
	}
    
    private void setMaxDiscards(int maxDiscards) {
		this.maxDiscards = maxDiscards;
	}
    
    private void setIgnoreRows(int ignoreRows) {
		this.ignoreRows = ignoreRows;
	}
    
    private void setCommitInterval(int commitInterval) {
		this.commitInterval = commitInterval;
	}
    
    /**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
         
        if (!checkInputPorts(status, 0, 1)
        		|| !checkOutputPorts(status, 0, 1)) {
        	return status;
        }

        try {
			init();
		} catch (ComponentNotReadyException e) {
			ConfigurationProblem problem = new ConfigurationProblem(e.getMessage(),
					ConfigurationStatus.Severity.ERROR, this,ConfigurationStatus.Priority.NORMAL);
			if (!StringUtils.isEmpty(e.getAttributeName())) {
				problem.setAttributeName(e.getAttributeName());
			}
			status.add(problem);
        } finally {
        	free();
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
    
    @Override
	protected void setLoadUtilityDateFormat(DataFieldMetadata field) {
    	throw new UnsupportedOperationException("Not use in this class.");
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
    
    /**
	 * Class for creating command for sqldr from string pieces and parameters.
	 * Each parameter is one field in array.
	 * 
	 * @author Miroslav Haupt (Mirek.Haupt@javlin.cz) 
	 * (c) Javlin a.s. (www.javlin.cz)
	 * @since 13.1.2009
	 */
	private static class OracleCommandBuilder {
		private Properties params;
		private List<String> cmdList;

		private OracleCommandBuilder(String command, Properties properties) {
			this.params = properties;
			cmdList = new ArrayList<String>();
			cmdList.add(command);
		}

		/**
		 *  Adds attribute and it's value:
		 *  for exmaple:  host=localhost or host='localhost'
		 * 
		 * @param attrName
		 * @param attrValue
		 * @param singleQuoted
		 */
		private void addAttribute(String attrName, String attrValue, boolean singleQuoted) {
			if (attrValue == null) {
				return;
			}

			if (singleQuoted) {
				cmdList.add(attrName + EQUAL_CHAR + "'" + attrValue + "'");
			} else {
				cmdList.add(attrName + EQUAL_CHAR + attrValue);
			}
		}
		
		/**
		 *  Adds attribute and it's value:
		 *  attrName=attrValue
		 *  for exmaple:  port=123
		 * 
		 * @param attrName
		 * @param attrValue
		 */
		private void addAttribute(String attrName, int attrValue) {
			if (attrValue == UNUSED_INT) {
				return;
			}
			addAttribute(attrName, String.valueOf(attrValue), false);
		}
		
		/**
		 *  Adds param and it's value:
		 *  for exmaple:  host=localhost or host='localhost'
		 * 
		 * @param paramName name of parameter used in 'parameters' attribute
		 * @param paramKeyword name of parameter used in sqlldr command
		 * @param singleQuoted
		 */
		private void addParam(String paramName, String paramKeyword, boolean singleQuoted) {
			if (!params.containsKey(paramName)) {
				return;
			}
			addAttribute(paramKeyword, params.getProperty(paramName), singleQuoted);
		}

		private String[] getCommand() {
			return cmdList.toArray(new String[cmdList.size()]);
		}
	}
	
	/**
	 * Class for reading and parsing data from input files, 
	 * and sends them to output port.
	 * 
	 * @author 		Miroslav Haupt (Mirek.Haupt@javlin.cz)
	 *				(c) Javlin a.s. (www.javlin.cz)
	 * @since 30.1.2009
	 */
	private class OracleBadRowReaderWriter {
		private Parser parser = null;
		private DataRecordMetadata metadata;
		private OutputPort outPort = null;
		
		public OracleBadRowReaderWriter() throws ComponentNotReadyException {
			outPort = getOutputPort(WRITE_TO_PORT);
			checkErrPortMetadata(getInputPort(READ_FROM_PORT), outPort);
		}
		
		private void init() throws ComponentNotReadyException {
	    	metadata = outPort.getMetadata();
	    	
	    	if (metadata.getRecType() == DataRecordMetadata.DELIMITED_RECORD) {
	    		parser = new DelimitedDataParser();
	    	} else { // FIXEDLEN_RECORD
	    		parser = new FixLenCharDataParser();
	    	}
	    	
			parser.init(metadata);
		}
		
		public void run() throws Exception {
	    	try {
	    		init();

		    	writeDataToOutPort(badFile, "bad");
		    	writeDataToOutPort(discardFile, "discard");
	    	} catch (Exception e) {
	    		throw new JetelException("Error while writing output record", e);
			} finally {
				close();
			}
		}
		
	    private void writeDataToOutPort(File sourceFile, String fileType) throws Exception {
	    	if (sourceFile == null || !sourceFile.exists()) {
	    		logger.info("File " + ((sourceFile == null) ? "" : StringUtils.quote(sourceFile.getAbsolutePath()) + " ") + 
	    				 "with " + fileType + " records doesn't exist. Any " + fileType + " records don't exist.");
	    		return;
	    	}
	    	
	    	parser.setDataSource(new BufferedInputStream(new FileInputStream(sourceFile)));
	    	
	    	DataRecord record = new DataRecord(metadata);
			record.init();
	    	
	    	while ((record = parser.getNext(record)) != null) {
	    		outPort.writeRecord(record);
	    	}
	    }
	    
	    public void close() {
	    	if (parser != null) {
	    		parser.close();
	    		parser = null;
	    	}

	    	try {
	    		if (outPort != null) {
	    			outPort.eof();
	    			outPort = null;
	    		}
			} catch (Exception ie) {
				logger.warn("Out port wasn't closed.", ie);
			}
	    }

	    /**
    	 * check metadata at output port against metadata at input port
    	 * if metadata isn't correct then throws ComponentNotReadyException
    	 * @throws ComponentNotReadyException when metadata isn't correct
    	 */
    	private void checkErrPortMetadata(InputPort inPort, OutputPort outPort) throws ComponentNotReadyException {
    		if (inPort == null || outPort == null) {
    			return;
    		}
    		DataRecordMetadata inMeta = inPort.getMetadata();
    		DataRecordMetadata outMeta = outPort.getMetadata();
    		
    		// check number of fields; if inNumFields == outNumFields
			if (inMeta.getNumFields() != outMeta.getNumFields()) {
				throw new ComponentNotReadyException("Number of fields of " +  StringUtils.quote(outMeta.getName()) +  
						" isn't equal number of fields of " +  StringUtils.quote(inMeta.getName()) + ".");
			}
    	}
	}
}