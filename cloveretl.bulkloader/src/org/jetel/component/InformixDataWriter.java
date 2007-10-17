/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
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
*/

package org.jetel.component;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.DataFormatter;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.BadDataFormatException;
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
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.PortDataConsumer;
import org.jetel.util.exec.ProcBox;
import org.w3c.dom.Element;

/**
 *  <h3>Informix data writer</h3>
 *
 * <!-- All records from input port 0 are loaded into informix database. Connection to database is not through JDBC driver,
 * this component uses the dbload utility for this purpose. Bad rows send to output port 0.-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Informix data writer</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>This component loads data to Informix database using the dbload utility. 
 * There is created temporary file with dbload commands depending on input parameters. Data are read from given input file or from the input port and loaded to database. 
 * On Linux/Unix system data transfer can be processed by stdin.<br>
 * Any generated scripts/commands can be optionally logged to help diagnose problems.<br>
 * To use this component Informix client must be installed and configured on the local host.
 * </td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0] - input records. It can be omitted - then <b>fileURL</b> has to be provided.</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0] - optionally one output port defined/connected - rejected records.
 * Metadata on this port must have the same type of field as input metadata. Output metadata 
 * has a additional fields with row number (integer) and error message (string).
 * These two fields are after data fields.
 * </td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"INFORMIX_DATA_WRITER"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>dbLoaderPath</b></td><td>path to loadDb utility</td></tr>
 *  <tr><td><b>database</b></td><td>the name of the database to receive the data<br/>
 *  example 1: //server_name/directory_on_server/database_name<br/>
 *  example 2: //server_name/database_name</td></tr>
 *  <tr><td><b>table</b><br><i>optional</i></td><td>table name, where data are loaded<br/>
 *  Note: table attribute or command attribute must be defined</td></tr>
 *  <tr><td><b>command</b><br><i>optional</i></td><td>a control script for the dbload utility;
 *  	if this parameter is empty default control script is used<br/>
 *  Note: table attribute or command attribute must be defined</td></tr>
 *  <tr><td><b>errorLog</b><br><i>optional</i></td><td>the filename or pathname of an error log file
 *  	if this parameter is empty default errorLog name is used (default = ./error.log)</td></tr</td></tr>
 *  <tr><td><b>maxErrors</b><br><i>optional</i></td><td>the number of bad rows that dbload reads before terminating.
 *   	if this parameter is empty default value is used (default = 10).</td></tr>
 *  <tr><td><b>ignoreRows</b><br><i>optional</i></td><td>the number of rows to ignore in the input file;<br/>
 *  	instructs dbload to read and ignore the specified number of new-line 
 *  characters in the input file before it begins to process.</td></tr>
 *  <tr><td><b>commitInterval</b><br><i>optional</i></td><td>commit interval in number of rows;
 *   	if this parameter is empty default value is used (default = 100)</td></tr>
 *  <tr><td><b>columnDelimiter</b><br><i>optional</i></td><td>char delimiter used for each column in data (default = '|')</br>
 *  Delimiter has length one and value of the delimiter mustn't be contained in data.</td></tr>
 *  <tr><td><b>fileURL</b><br><i>optional</i></td><td>Path to data file to be loaded.<br>
 *  Normally this file is a temporary storage for data to be passed to dbload utility 
 *  (if stdio isn't used instead). If file URL is not specified, 
 *  the file is created in Clover or OS temporary directory and deleted after load finishes.<br>
 *  If file URL is specified, temporary file is created within given path and name and not 
 *  deleted after being loaded. Next graph run overwrites it. 
 *  <br>
 *  There is one more meaning of this parameter. If input port is not specified, this file 
 *  is used only for reading by dbload utility and must already contain data in format expected by 
 *  load. The file is neither deleted nor overwritten.</td></tr>
 *  </table>
 *
 *	<h4>Example:</h4>
 *  Reading data from input port:
 *  <pre>&lt;Node dbLoaderPath="dbload" commitInterval="1000" database="//demo_on/test" 
	errorLog="error.log" table="customers" columnDelimiter="|" 
	id="INFORMIX_DATA_WRITER0" type="INFORMIX_DATA_WRITER"/&gt;
 *  </pre>
 *  Reading data from flat file:
 *  <pre>&lt;Node dbLoaderPath="dbload" database="//demo_on/test" 
	errorLog="error.log" table="customers" columnDelimiter=";" 
	fileURL="/home/student/informix_data/inPlain.txt"
	id="INFORMIX_DATA_WRITER0" type="INFORMIX_DATA_WRITER"/&gt;
 *
 * @author      Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
 *(c) Javlin Consulting (www.javlinconsulting.cz)
 * @see         org.jetel.graph.TransformationGraph
 * @see         org.jetel.graph.Node
 * @see         org.jetel.graph.Edge
 * @since 		20.8.2007
 */
public class InformixDataWriter extends Node {

	private static Log logger = LogFactory.getLog(InformixDataWriter.class);

    /**  Description of the Field */
	private static final String XML_DB_LOADER_PATH_ATTRIBUTE = "dbLoaderPath";
    private static final String XML_COMMAND_ATTRIBUTE = "command";
    private static final String XML_DATABASE_ATTRIBUTE = "database";
    private static final String XML_ERROR_LOG_ATTRIBUTE = "errorLog";
    private static final String XML_MAX_ERRORS_ATTRIBUTE = "maxErrors";
    private static final String XML_IGNORE_ROWS_ATTRIBUTE = "ignoreRows";
    private static final String XML_COMMIT_INTERVAL_ATTRIBUTE = "commitInterval";
    private static final String XML_TABLE_ATTRIBUTE = "table";
    private static final String XML_COLUMN_DELIMITER_ATTRIBUTE = "columnDelimiter";
    private static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
    
    public final static String COMPONENT_TYPE = "INFORMIX_DATA_WRITER";
    private final static int READ_FROM_PORT = 0;
    private final static int WRITE_TO_PORT = 0;	//port for write bad record

    private final static String INFORMIX_COMMAND_PATH_OPTION = "-c";
    private final static String INFORMIX_DATABASE_OPTION = "-d";
    private final static String INFORMIX_ERROR_LOG_OPTION = "-l";
    private final static String INFORMIX_ERRORS_OPTION = "-e";
    private final static String INFORMIX_IGNORE_ROWS_OPTION = "-i";
    private final static String INFORMIX_COMMIT_INTERVAL_OPTION = "-n";
    
    private final static String DATA_FILE_NAME_PREFIX = "data";
    private final static String DATA_FILE_NAME_SUFFIX = ".dat";
    private final static String LOADER_FILE_NAME_PREFIX = "loader";
    private final static String CONTROL_FILE_NAME_SUFFIX = ".ctl";
    private final static String DEFAULT_ERROR_FILE = "error.log";
    private final static File TMP_DIR = new File(".");
    private final static int UNUSED_INT = -1;
    private final static String DEFAULT_COLUMN_DELIMITER = "|";
    private final static String LINE_SEPARATOR = System.getProperty("line.separator");
    private final static String UNIX_STDIN = "/dev/stdin";
    private final static String CHARSET_NAME = "UTF-8";
    private final static String DEFAULT_DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private final static String DEFAULT_DATE_FORMAT = "MM/dd/yyyy"; 
    private final static String DEFAULT_TIME_FORMAT = "HH:mm:ss";
    
    // variables for dbload's command
	private String dbLoaderPath;
    private String command; //contains user-defined control script fot dbload utility
    private String database;
    private String errorLog;
    private int maxErrors = UNUSED_INT;
    private int ignoreRows = UNUSED_INT;
    private int commitInterval = UNUSED_INT;
    private String table;
    private String columnDelimiter = DEFAULT_COLUMN_DELIMITER;
    private String commandFileName; //file where dbload command is saved
    private String dataURL; // fileUrl from XML - data file that is used when no input port is connected

    private String tmpDataFileName; // file that is used for exchange data between clover and dbload
    private DataRecordMetadata dbMetadata; // it correspond to dbload input format
    private DataFormatter formatter; // format data to dbload format and write them to dataFileName 
    private DataConsumer consumer = null; // consume data from out stream of dbload
    private DataConsumer errConsumer; // consume data from err stream of dbload - write them to by logger
    
    /**
     * true - data is read from in port;
     * false - data is read from file directly by dbload utility
     */
    private boolean isDataReadFromPort;
    
    /**
     * true - bad rows is written to out port;
     * false - bad rows isn't written to anywhere
     */
    private boolean isDataWrittenToPort;
    
    /**
     * Constructor for the InformixDataWriter object
     *
     * @param  id  Description of the Parameter
     */
    public InformixDataWriter(String id, String dbLoaderPath, String database) { 
        super(id);
        this.dbLoaderPath = dbLoaderPath;
        this.database = database;
    }
    
    /**
     *  Main processing method for the InformixDataWriter object
     *
     * @since    April 4, 2002
     */
    public Result execute() throws Exception {
        ProcBox box;
        int processExitValue = 0;

        if (isDataReadFromPort) {
	        if (ProcBox.isWindowsPlatform()) {
	        	// temp file is used for exchange data
	        	formatter.setDataTarget(Channels.newChannel(new FileOutputStream(tmpDataFileName)));
	        	readFromPortAndWriteByFormatter();
	        	
	            box = createProcBox(null);
	        } else {
	        	Process process = Runtime.getRuntime().exec(createCommandLineForDbLoader());
	            box = createProcBox(process);
	            
	            // stdin is used for exchange data - set data target to stdin of process
	        	OutputStream processIn = new BufferedOutputStream(process.getOutputStream());
	        	formatter.setDataTarget(Channels.newChannel(processIn));
	        	readFromPortAndWriteByFormatter();
	        }
    		
    		processExitValue = box.join();
        } else {
        	processExitValue = readDataDirectlyFromFile();
        }
        
        if (processExitValue != 0) {
        	throw new JetelException("Dbload utility has failed.");
		}
        
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
    }

    /**
     * This method reads incoming data from port and sends them by formatter to dbload process.
     * 
	 * @throws Exception
	 */
	private void readFromPortAndWriteByFormatter() throws Exception {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = new DataRecord(dbMetadata);
		record.init();
		
		try {
			while (runIt && ((record = inPort.readRecord(record)) != null)) {
		        formatter.write(record);
			}
		} catch (Exception e) {
			throw e;
		} finally {
		    formatter.close();
		}
	}
	
	/**
	 * Call dbload process with parameters - dbload process reads data directly from file.  
	 * @return value of finished process
	 * @throws Exception
	 */
	private int readDataDirectlyFromFile() throws Exception {
        ProcBox box = createProcBox(null);
        return box.join();
	}

	/**
	 * Create instance of ProcBox.
	 * @param process running process; when process is null, default process is created
	 * @return instance of ProcBox
	 * @throws IOException
	 */
	private ProcBox createProcBox(Process process) throws IOException {
		if (process == null) {
			process = Runtime.getRuntime().exec(createCommandLineForDbLoader());			
		}
        return new ProcBox(process, null, consumer, errConsumer);
	}
    
    /**
     * Create command line for process, where dbload utility is running.
     * Example: /home/Informix/bin/dbload -d test@pnc -c cmdData.ctl -l data.log
     * @return
     */
    private String[] createCommandLineForDbLoader() {
		List<String> cmdList = new ArrayList<String>();
		
		cmdList.add(dbLoaderPath);
		
		cmdList.add(INFORMIX_COMMAND_PATH_OPTION);
		cmdList.add(commandFileName);
		
		cmdList.add(INFORMIX_DATABASE_OPTION);
		cmdList.add(database);
		
		cmdList.add(INFORMIX_ERROR_LOG_OPTION);
		cmdList.add(errorLog);
		
		if (maxErrors != UNUSED_INT) {
			cmdList.add(INFORMIX_ERRORS_OPTION);
			cmdList.add(String.valueOf(maxErrors));
		}
		if (ignoreRows != UNUSED_INT) {
			cmdList.add(INFORMIX_IGNORE_ROWS_OPTION);
			cmdList.add(String.valueOf(ignoreRows));
		}
		if (commitInterval != UNUSED_INT) {
			cmdList.add(INFORMIX_COMMIT_INTERVAL_OPTION);
			cmdList.add(String.valueOf(commitInterval));
		}
		
		String[] ret = cmdList.toArray(new String[cmdList.size()]);
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

		isDataReadFromPort = !getInPorts().isEmpty() && StringUtils.isEmpty(command);
		isDataWrittenToPort = !getOutPorts().isEmpty();

		checkAttributes();
		
		// prepare name for temporary data file
		try {
            commandFileName = File.createTempFile(LOADER_FILE_NAME_PREFIX, 
            		CONTROL_FILE_NAME_SUFFIX, TMP_DIR).getCanonicalPath();
            
            if (errorLog == null) {
            	errorLog = new File(TMP_DIR, DEFAULT_ERROR_FILE).getCanonicalPath();
            }
            
            if (isDataReadFromPort) {
	        	if (ProcBox.isWindowsPlatform()) {
	        		if (dataURL != null) {
	        			tmpDataFileName = new File(dataURL).getCanonicalPath();
	        		} else {
	        			tmpDataFileName = File.createTempFile(DATA_FILE_NAME_PREFIX, 
		            			DATA_FILE_NAME_SUFFIX, TMP_DIR).getCanonicalPath();
	        		}
	            } else {
	            	tmpDataFileName = UNIX_STDIN;
	            }
            } else {
            	if (dataURL == null) {
            		free();
            		throw new ComponentNotReadyException(this, "There is neither input port nor " 
            				+ StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + " attribute specified.");
            	}
            	if (!new File(dataURL).exists()) {
					free();
					throw new ComponentNotReadyException(this, "Data file "
								+ StringUtils.quote(dataURL) + " not exists.");
				}
            	
            	tmpDataFileName = new File(dataURL).getCanonicalPath();
            }
            
        } catch(IOException e) {
        	free();
            throw new ComponentNotReadyException(this, "Some of the log files cannot be created.");
        }

        createCommandFile();

        errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);
        
        if (isDataReadFromPort) {
	        InputPort inPort = getInputPort(READ_FROM_PORT);
	
	        dbMetadata = createInformixMetadata(inPort.getMetadata());
	        
	        // init of data formatter
	        formatter = new DataFormatter(CHARSET_NAME);
	        formatter.init(dbMetadata);
        }
        
        if (isDataWrittenToPort) {
        	try {
            	// create data consumer and check metadata
            	consumer = new InformixPortDataConsumer(getOutputPort(WRITE_TO_PORT));
            } catch (ComponentNotReadyException cnre) {
            	free();
            	throw new ComponentNotReadyException(this, "Error during initialization of InformixPortDataConsumer.", cnre);
            }
        } else {
        	consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
        }
    }
    
    /**
     * Modify metadata so that they correspond to dbload input format. 
     * Each field is delimited and it has the same delimiter.
     * Only last field is delimited by '\n'.
     *
     * @param oldMetadata original metadata
     * @return modified metadata
     */
    private DataRecordMetadata createInformixMetadata(DataRecordMetadata originalMetadata) {
    	DataRecordMetadata metadata = originalMetadata.duplicate();
		metadata.setRecType(DataRecordMetadata.DELIMITED_RECORD);
		for (int idx = 0; idx < metadata.getNumFields() - 1; idx++) {
			metadata.getField(idx).setDelimiter(columnDelimiter);
			setInformixDateFormat(metadata.getField(idx));
		}
		metadata.getField(metadata.getNumFields() - 1).setDelimiter("\n");
		setInformixDateFormat(metadata.getField(metadata.getNumFields() - 1));
	
		return metadata;
    }

    /**
     * If field has format of date or time then default informix format is set.
     * @param field 
     */
    private void setInformixDateFormat(DataFieldMetadata field) {
		if (field.getType() == DataFieldMetadata.DATE_FIELD ||
				field.getType() == DataFieldMetadata.DATETIME_FIELD) {
			boolean isDate = field.isDateFormat();
			boolean isTime = field.isTimeFormat();

			// if formatStr is undefined then DEFAULT_DATETIME_FORMAT is assigned
			if ((isDate && isTime) || (StringUtils.isEmpty(field.getFormatStr()))) {
				field.setFormatStr(DEFAULT_DATETIME_FORMAT);
			}else if (isDate) {
				field.setFormatStr(DEFAULT_DATE_FORMAT);
			}else{
				field.setFormatStr(DEFAULT_TIME_FORMAT);
			}
		}
	}
    
    private void checkAttributes() throws ComponentNotReadyException {
    	if (columnDelimiter.length() != 1) {
			throw new ComponentNotReadyException(this, XML_COLUMN_DELIMITER_ATTRIBUTE, "Max. length of column delimiter is one.");
		}
    	if (maxErrors != UNUSED_INT && maxErrors < 0) {
    		throw new ComponentNotReadyException(this, XML_MAX_ERRORS_ATTRIBUTE + " mustn't be less than 0.");
    	}
    	if (ignoreRows != UNUSED_INT && ignoreRows < 0) {
    		throw new ComponentNotReadyException(this, XML_IGNORE_ROWS_ATTRIBUTE + " mustn't be less than 0.");
    	}
		if (commitInterval != UNUSED_INT && commitInterval < 0) {
    		throw new ComponentNotReadyException(this, XML_COMMIT_INTERVAL_ATTRIBUTE + " mustn't be less than 0.");
		}
		
		// input port or dataURL or command have to be set
		if (!isDataReadFromPort && StringUtils.isEmpty(dataURL) && StringUtils.isEmpty(command)) {
			throw new ComponentNotReadyException(this, "Input port or " + 
					StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + " attribute or " +
					StringUtils.quote(XML_COMMAND_ATTRIBUTE) + " have to be set.");
		}

		// check if each of mandatory attributes is set
		if (StringUtils.isEmpty(dbLoaderPath) || StringUtils.isEmpty(database) || 
				(StringUtils.isEmpty(command) && StringUtils.isEmpty(table))) {
			throw new ComponentNotReadyException(this, "dbLoaderPath, database or (table and command simultaneously) argument isn't fill.");
		}
    }
    
    @Override
	public synchronized void free() {
		super.free();
		deleteCommandFile();
		deleteDataFile();
	}

	/**
     * Create new temp file with control script for dbload utility.
     * @throws ComponentNotReadyException
     */
    private void createCommandFile() throws ComponentNotReadyException {
    	File commandFile = new File(commandFileName);
        FileWriter commandWriter;
        try {
        	commandFile.createNewFile();
        	commandWriter = new FileWriter(commandFile);
        	String content = null;
        	if (command == null) {
        		if (isDataReadFromPort) {
        			content = getDefaultControlFileContent(table, getInputPort(READ_FROM_PORT).getMetadata().getNumFields(),
        						columnDelimiter, tmpDataFileName);
        		} else if (isDataWrittenToPort) {
        			content = getDefaultControlFileContent(table, 
        					getOutputPort(READ_FROM_PORT).getMetadata().getNumFields() - InformixPortDataConsumer.NUMBER_OF_ADDED_FIELDS,
        						columnDelimiter, dataURL);
        		} else { // data is read from file and bad rows isn't written to any port
        			content = getDefaultControlFileContent(table, getNumFieldsFromInFile(),
    						columnDelimiter, dataURL);
        		}
        		logger.debug("Control file content: " + content);
        	} else {
        		content = command;
        	}

            commandWriter.write(content);
            commandWriter.close();
        } catch(IOException ex) {
            throw new ComponentNotReadyException(this, "Can't create temp control file for dbload utility.", ex);
        }
    }

    /**
     * @return return number of field in one row of inDataFileName file
     * @throws ComponentNotReadyException
     */
    private int getNumFieldsFromInFile() throws ComponentNotReadyException {
		BufferedReader reader = null;
		String line = null;
		try {
			reader = new BufferedReader(new FileReader(new File(dataURL)));
			if (ignoreRows != UNUSED_INT) {
				// skip ignore rows
				for (int i = ignoreRows; i < ignoreRows; i++) {
					line = reader.readLine();
				}
			}
			line = reader.readLine();
		} catch (IOException ioe) {
			throw new ComponentNotReadyException(this, "Error during opening or reading file ." 
					+ StringUtils.quote(dataURL), ioe);
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					logger.warn("File" + StringUtils.quote(dataURL) + " wasn't closed.");
				}
			}
		}

		if (line == null) {
			throw new ComponentNotReadyException(this, "Error during reading file ." 
					+ StringUtils.quote(dataURL));
		}
		return line.split(columnDelimiter).length;
    }
    
    /**
     * Generates default control script.
     * 
     * Default script:
     * 
     * FILE <file> DELIMITER '<delimiter>' <numFields>;
     * INSERT INTO <tableName>;
     * 
     * @param tableName
     * @param numFields
     * @param delimiter
     * @param fileName
     * @return control script
     */
    private String getDefaultControlFileContent(String tableName, int numFields, String delimiter, String fileName) throws ComponentNotReadyException {
        return "FILE " + StringUtils.quote(fileName) + " DELIMITER '" + delimiter + "' " +
        	numFields + ";" + LINE_SEPARATOR +
        	"INSERT INTO " + tableName + ";";
    }
   
    /**
     * Deletes temp file with loader script. 
     */
    private void deleteCommandFile() {
    	if (StringUtils.isEmpty(commandFileName)) {
    		return;
    	}
    	
    	File controlFile = new File(commandFileName);
        controlFile.delete();
    }
    
    /**
     * Deletes data file which was used for exchange data.
     */
    private void deleteDataFile() {
    	if (StringUtils.isEmpty(tmpDataFileName)) {
    		return;
    	}
    	
    	if (isDataReadFromPort && !UNIX_STDIN.equals(tmpDataFileName) && dataURL == null) {
    		File dataFile = new File(tmpDataFileName);
    		if (!dataFile.delete()) {
    			logger.warn("Temp data file was not deleted.");
    		}
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
        	InformixDataWriter informixDataWriter = new InformixDataWriter(
        			xattribs.getString(XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_DB_LOADER_PATH_ATTRIBUTE),
                    xattribs.getString(XML_DATABASE_ATTRIBUTE));
        	if (xattribs.exists(XML_TABLE_ATTRIBUTE)) {
        		informixDataWriter.setTable(xattribs.getString(XML_TABLE_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_COMMAND_ATTRIBUTE)) {
        		informixDataWriter.setCommand(xattribs.getString(XML_COMMAND_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_ERROR_LOG_ATTRIBUTE)) {
        		informixDataWriter.setErrorLog(xattribs.getString(XML_ERROR_LOG_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_MAX_ERRORS_ATTRIBUTE)) {
        		informixDataWriter.setMaxErrors(xattribs.getInteger(XML_MAX_ERRORS_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_IGNORE_ROWS_ATTRIBUTE)) {
        		informixDataWriter.setIgnoreRows(xattribs.getInteger(XML_IGNORE_ROWS_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_COMMIT_INTERVAL_ATTRIBUTE)) {
        		informixDataWriter.setCommitInterval(xattribs.getInteger(XML_COMMIT_INTERVAL_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_COLUMN_DELIMITER_ATTRIBUTE)) {
        		informixDataWriter.setColumnDelimiter(xattribs.getString(XML_COLUMN_DELIMITER_ATTRIBUTE));
        	}
        	if (xattribs.exists(XML_FILE_URL_ATTRIBUTE)) {
        		informixDataWriter.setInDataFileName(xattribs.getString(XML_FILE_URL_ATTRIBUTE));
        	}
       	
            return informixDataWriter;
        } catch (Exception ex) {
               throw new XMLConfigurationException(COMPONENT_TYPE + ":" + 
            		   xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
    }
    
    @Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		xmlElement.setAttribute(XML_DB_LOADER_PATH_ATTRIBUTE, dbLoaderPath);
		xmlElement.setAttribute(XML_DATABASE_ATTRIBUTE, database);

		if (!StringUtils.isEmpty(table)) {
			xmlElement.setAttribute(XML_TABLE_ATTRIBUTE, table);
		}
		if (!StringUtils.isEmpty(command)) {
			xmlElement.setAttribute(XML_COMMIT_INTERVAL_ATTRIBUTE, command);
		}
		if (!StringUtils.isEmpty(errorLog)) {
			xmlElement.setAttribute(XML_ERROR_LOG_ATTRIBUTE, errorLog);
		}
		if (maxErrors != UNUSED_INT) {
			xmlElement.setAttribute(XML_MAX_ERRORS_ATTRIBUTE, String.valueOf(maxErrors));
		}
		if (ignoreRows != UNUSED_INT) {
			xmlElement.setAttribute(XML_IGNORE_ROWS_ATTRIBUTE, String.valueOf(ignoreRows));
		}
		if (commitInterval != UNUSED_INT) {
			xmlElement.setAttribute(XML_COMMIT_INTERVAL_ATTRIBUTE, String.valueOf(commitInterval));
		}
		if (!DEFAULT_COLUMN_DELIMITER.equals(columnDelimiter)) {
			xmlElement.setAttribute(XML_COLUMN_DELIMITER_ATTRIBUTE, columnDelimiter);
		}
		if (!StringUtils.isEmpty(dataURL)) {
			xmlElement.setAttribute(XML_FILE_URL_ATTRIBUTE, dataURL);
		}
	}

	/**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		 
		checkInputPorts(status, 0, 1);
        checkOutputPorts(status, 0, 1);

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

    private void setCommand(String command) {
		this.command = command;
	}
    
    private void setErrorLog(String errorLog) {
    	this.errorLog = errorLog;
	}
    
    private void setMaxErrors(int maxErrors) {
    	this.maxErrors = maxErrors;
	}
    
    private void setIgnoreRows(int ignoreRows) {
    	this.ignoreRows = ignoreRows;
	}
    
    private void setCommitInterval(int commitInterval) {
    	this.commitInterval = commitInterval;
	}
    
    private void setColumnDelimiter(String columnDelimiter) {
    	this.columnDelimiter = columnDelimiter;
	}
    
    private void setInDataFileName(String inDataFileName) {
    	this.dataURL = inDataFileName;
	}
    
    private void setTable(String table) {
    	this.table = table;
	}
    
    /**
     * Class for reading and parsing data from input stream, which is supposed to be connected to process' output,
     * and sends them to specified output port.
     * 
     * @see 		org.jetel.util.exec.ProcBox
     * @see 		org.jetel.util.exec.DataConsumer
     * @author      Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
     * 				(c) Javlin Consulting (www.javlinconsulting.cz)
     * @since 		20.8.2007
     */
    private class InformixPortDataConsumer implements DataConsumer {
    	private DataRecord dbRecord;				// record from informix db output
    	private DataRecordMetadata dbOutMetadata;	// format as informix db output
    	private BufferedReader reader;				// read from input stream (=output stream of dbload process)
    	private Parser dbParser;					// parse record from informix db output
    	private DataRecord errRecord = null;
    	private OutputPort errPort = null;
    	
    	private DataRecordMetadata errMetadata;		// format as output port
    	
    	private Log logger = LogFactory.getLog(PortDataConsumer.class);
    	
    	private String strBadRowPattern = "Row number (\\d+) is bad";
    	private Matcher badRowMatcher;
    	
    	private int rowNumberFieldNo; // last field -2
    	private int errMsgFieldNo; // last field
    	private final static int NUMBER_OF_ADDED_FIELDS = 2; // number of addded fields in errPortMetadata against dbIn(Out)Metadata

    	/**
    	 * @param port Output port receiving consumed data.
    	 * @throws ComponentNotReadyException 
    	 */
    	public InformixPortDataConsumer(OutputPort errPort) throws ComponentNotReadyException {
    		if (errPort == null) {
        		throw new ComponentNotReadyException("Output port wasn't found.");
    		}

    		this.errPort = errPort;
    		
    		errMetadata = errPort.getMetadata();
    		if (errMetadata == null) {
        		throw new ComponentNotReadyException("Output port hasn't assigned metadata.");
        	}
    		
    		getNumberOfAddedFields();
    		checkErrPortMetadata();
    		
    		dbParser = new DelimitedDataParser(CHARSET_NAME);
    		this.dbOutMetadata = createDbOutMetadata();

    		dbRecord = new DataRecord(dbOutMetadata);
    		dbRecord.init();
    		
			errRecord = new DataRecord(errMetadata);
			errRecord.init();
    		
    		Pattern badRowPattern = Pattern.compile(strBadRowPattern);
			badRowMatcher = badRowPattern.matcher("");
			
   			dbParser.init(dbOutMetadata);
    	}
    	
    	/**
    	 * Gets index of added fields (rowNumber and errMsg).
    	 */
    	private void getNumberOfAddedFields() {
    		int numFields = errMetadata.getNumFields();
    		rowNumberFieldNo = numFields - 2;
        	errMsgFieldNo = numFields - 1;
    	}
    	
    	/**
         * Create metadata so that they correspond to format of informix db output
         * 
         * @return modified metadata
    	 * @throws ComponentNotReadyException 
         */
        private DataRecordMetadata createDbOutMetadata() {
        	DataRecordMetadata metadata = errMetadata.duplicate();
        	metadata.setRecType(DataRecordMetadata.DELIMITED_RECORD);
        	// delete first and second field
        	for (int i = 0; i < NUMBER_OF_ADDED_FIELDS; i++) {
        		metadata.delField(metadata.getNumFields() - 1);
        	}
        	
        	for (DataFieldMetadata fieldMetadata: metadata) {
        		fieldMetadata.setDelimiter(columnDelimiter);
        	}

        	return metadata;
        }
    	
    	/**
    	 * @see org.jetel.util.exec.DataConsumer
    	 */
    	public void setInput(InputStream stream) {
    		reader = new BufferedReader(new InputStreamReader(stream));
    	}

    	/**
    	 * Example of bad row in stream:
		 * In INSERT statement number 1 of raw data file /home/informix_data/./data21672.dat.
	     * Row number 2 is bad.
	     * abc|def|ghi|
	     * Invalid month in date
	     *
    	 * @see org.jetel.util.exec.DataConsumer
    	 */
    	public boolean consume() throws JetelException {
    		try {
				String line;
				if ((line = readLine()) == null) {
					return false;
				}

				badRowMatcher.reset(line);
				if (badRowMatcher.find()) {
        			int rowNumber = Integer.valueOf(badRowMatcher.group(1));
        			if ((line = reader.readLine()) == null) {
        				return false;
        			}
        			dbParser.setDataSource(getInputStream(line, CHARSET_NAME));

        			// read empty line(s) and error message, first not empty line is errMsg
        			String errMsg;
        			errMsg = readLine();
        			while (StringUtils.isEmpty(errMsg) || StringUtils.isBlank(errMsg)) {
        				if (errMsg == null) {
        					return false;
        				}
        				errMsg = readLine();
        			}
        			
					try {
						if (dbParser.getNext(dbRecord) != null) {
							setErrRecord(dbRecord, errRecord, rowNumber, errMsg);
							errPort.writeRecord(errRecord);
    					}
					} catch (BadDataFormatException e) {
						logger.warn("Bad row - it couldn't be parsed and sent to out port. Line: " + line);
					}
        		}
    		} catch (Exception e) {
				throw new JetelException("Error while writing output record", e);
			}
    		
    		SynchronizeUtils.cloverYield();
    		return true;
    	}
    	
    	/**
    	 * Read line by reader and write it by logger and return it.
    	 * @return read line
    	 * @throws IOException
    	 */
    	private String readLine() throws IOException {
    		String line = reader.readLine();
    		logger.debug(line);
    		return line;
    	}
    	
    	/**
    	 * Set value in errRecord. In first field is set row number and other fields are copies from dbRecord
    	 * @param dbRecord source record
    	 * @param errRecord destination record
    	 * @param rowNumber number of bad row
    	 * @param errMsg errorMsg
    	 * @return destination record
    	 */
    	private DataRecord setErrRecord(DataRecord dbRecord, DataRecord errRecord, int rowNumber, String errMsg) {
    		errRecord.reset();
    		errRecord.getField(rowNumberFieldNo).setValue(rowNumber);
    		errRecord.getField(errMsgFieldNo).setValue(errMsg);
    		for (int dbFieldNum = 0; dbFieldNum < dbRecord.getNumFields(); dbFieldNum++) {
    			errRecord.getField(dbFieldNum).setValue(dbRecord.getField(dbFieldNum));
    		}
    		return errRecord;
    	}
    	
    	/**
    	 * It create and return InputStream from string
    	 * @param str string, returned InputStream contains this string  
    	 * @param charsetName
    	 * @return InputStream created from string
    	 */
    	private InputStream getInputStream(String str, String charsetName) throws UnsupportedEncodingException {
        	return new ByteArrayInputStream(str.getBytes(charsetName));
        }
    	
    	/**
    	 * check metadata at error port against metadata at input port
    	 * if metadata isn't correct then throws ComponentNotReadyException
    	 * @throws ComponentNotReadyException when metadata isn't correct
    	 */
    	private void checkErrPortMetadata() throws ComponentNotReadyException {
			// check if last - 1  field of errMetadata is integer - rowNumber
			if (errMetadata.getFieldType(rowNumberFieldNo) != DataFieldMetadata.INTEGER_FIELD) {
				throw new ComponentNotReadyException("First field of " +  StringUtils.quote(errMetadata.getName()) +  
						" has different type from integer.");
			}
			
			// check if last field of errMetadata is string - errMsg
			if (errMetadata.getFieldType(errMsgFieldNo) != DataFieldMetadata.STRING_FIELD) {
				throw new ComponentNotReadyException("Second field of " +  StringUtils.quote(errMetadata.getName()) +  
						" has different type from string.");
			}
    		
    		if (dbMetadata == null) {
    			return;
    		}
    		
    		// check number of fields; if inNumFields == outNumFields + NUMBER_OF_ADDED_FIELDS
			if (errMetadata.getNumFields() != dbMetadata.getNumFields() + NUMBER_OF_ADDED_FIELDS) {
				throw new ComponentNotReadyException("Number of fields of " +  StringUtils.quote(errMetadata.getName()) +  
						" isn't equal number of fields of " +  StringUtils.quote(dbMetadata.getName()) + " + " + NUMBER_OF_ADDED_FIELDS + ".");
			}
			
			// check if other fields' type of errMetadata are equals as dbMetadat
			int count = 0;
			for (DataFieldMetadata dbFieldMetadata: dbMetadata){
				if (!dbFieldMetadata.equals(errMetadata.getField(count++))) {
					throw new ComponentNotReadyException("Field "
							+ StringUtils.quote(errMetadata.getField(count - 1).getName()) + " in " 
							+ StringUtils.quote(errMetadata.getName()) + " has different type from field " 
							+ StringUtils.quote(dbFieldMetadata.getName()) + " in " + StringUtils.quote(dbMetadata.getName()) + ".");
				}
			}
    	}
    	
    	/**
    	 * @see org.jetel.util.exec.DataConsumer
    	 */
    	public void close() {
    		if (dbParser != null) {
    			dbParser.close();
    		}
    		
    		try {
				if (reader != null) {
					reader.close();
				}
			} catch (IOException ioe) {
				logger.warn("Reader wasn't closed.", ioe);
			}
    		
    		try {
        		if (errPort != null) {
        			errPort.eof();
        		}
			} catch (InterruptedException ie) {
				logger.warn("Out port wasn't closed.", ie);
			}
    	}
    }
}
