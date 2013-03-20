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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.formatter.DelimitedDataFormatter;
import org.jetel.data.formatter.Formatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.TempFileCreationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.LoggerDataConsumer;
import org.jetel.util.exec.PlatformUtils;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * It is a base class for all kinds of bulkloaders.
 * 
 * @author Miroslav Haupt (mirek.haupt@javlin.cz)<br>
 *         (c) Javlin Consulting (www.javlin.cz)
 * @see org.jetel.graph.TransformationGraph
 * @see org.jetel.graph.Node
 * @see org.jetel.graph.Edge
 * @since 5.2.2009
 *
 */
public abstract class BulkLoader extends Node {

	private static Log logger = LogFactory.getLog(BulkLoader.class);
	
	protected final static String XML_DATABASE_ATTRIBUTE = "database";
	protected final static String XML_TABLE_ATTRIBUTE = "table";
	protected final static String XML_USER_ATTRIBUTE = "username";
	protected final static String XML_PASSWORD_ATTRIBUTE = "password";
	protected final static String XML_COLUMN_DELIMITER_ATTRIBUTE = "columnDelimiter";
	protected final static String XML_FILE_URL_ATTRIBUTE = "fileURL";
	protected final static String XML_PARAMETERS_ATTRIBUTE = "parameters";
	
	protected final static char SPACE_CHAR = ' ';
	protected final static String SPACE_MARK = " ";
	protected final static String EQUAL_CHAR = "=";
	protected final static int UNUSED_INT = -1;
	protected final static String CONTROL_FILE_NAME_SUFFIX = ".ctl";
	protected final static String CHARSET_NAME = "UTF-8";
	
	protected final static int READ_FROM_PORT = 0;
    protected final static int WRITE_TO_PORT = 0;	//port for write bad record
	
	// variables for load utility's command
	protected String loadUtilityPath = null; // format data to load utility format and write them to dataFileName
	protected String dataURL = null; // fileUrl from XML - data file that is used when no input port is connected or for log
	protected String table = null;
	protected String user = null;
	protected String password = null;
	protected String columnDelimiter = null;
	protected String database = null;
	protected String host = null;
	protected String parameters = null;
	
	protected File dataFile = null; // file that is used for exchange data between clover and load utility - file from dataURL
	protected Properties properties = null;
	protected DataRecordMetadata dbMetadata; // it correspond to load utility input format
	protected DataConsumer consumer = null; // consume data from out stream of load utility
	protected DataConsumer errConsumer = null; // consume data from err stream of utility - write them to by logger
	protected Formatter formatter = null; // format data to load utility format and write them to dataFileName
	protected String[] commandLine; // command line of load utility

	
	/**
	 *  Flag that determine if execute() method was already executed.
	 *  used for deleting temp data file and reporting about it
	 */
	protected boolean alreadyExecuted = false;
	
	/**
     * true - data is read from in port;
     * false - data is read from file directly by load utility
     */
    protected boolean isDataReadFromPort;
    
    /**
     * true - bad rows is written to out port;
     * false - bad rows isn't written to anywhere
     */
    protected boolean isDataWrittenToPort;
    
    protected boolean isDataReadDirectlyFromFile;
	
	public BulkLoader(String id, String loadUtilityPath, String database) {
		super(id);
		this.loadUtilityPath = loadUtilityPath;
		this.database = database;
	}

	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
		} else {
			if (formatter != null) {
				formatter.reset();
			}
		}
	}


	@Override
	public Result execute() throws Exception {
		alreadyExecuted = true;
		return null;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) return;
		super.init();
		
		isDataReadFromPort = !getInPorts().isEmpty();
		isDataReadDirectlyFromFile = !isDataReadFromPort && !StringUtils.isEmpty(dataURL);
        isDataWrittenToPort = !getOutPorts().isEmpty();
		
		properties = parseParameters(parameters);
		
		checkParams();
		preInit();
		initDataFile();
		commandLine = createCommandLineForLoadUtility();
		printCommandLineToLog(commandLine);
		if (isDataReadFromPort) {
			initDataFormatter();
		}
		createConsumers();
	}
	
	/**
	 * This method can contain extra code executed before other code in init() method. 
	 * @throws ComponentNotReadyException
	 */
	protected void preInit() throws ComponentNotReadyException {
		// can be empty
	}

	/**
	 * Checks if mandatory attributes are defined.
	 * And check combination of some parameters.
	 * 
	 * @throws ComponentNotReadyException if any of conditions isn't fulfilled
	 */
	protected abstract void checkParams() throws ComponentNotReadyException;
	
	/**
	 * Initialization of data and other temporary files used for loading.
	 * 
	 * @throws ComponentNotReadyException
	 */
	protected abstract void initDataFile() throws ComponentNotReadyException;
	
	/**
     * Create command line for process, where load utility is running.
     * 
     * @return array first field is name of load utility and the others fields are parameters
	 * @throws ComponentNotReadyException when command file isn't created
     */
	protected abstract String[] createCommandLineForLoadUtility() throws ComponentNotReadyException;
	
	/**
	 * Initialization of data formatter used for loading data to the target that is read by load utility.
	 * If this method isn't overridden then getColumnDelimiter() and getRecordDelimiter() 
	 * methods must be implemented.
	 * @throws ComponentNotReadyException
	 */
	protected void initDataFormatter() throws ComponentNotReadyException {
		dbMetadata = createLoadUtilityMetadata(getColumnDelimiter(), getRecordDelimiter());

		// init of data formatter
		formatter = new DelimitedDataFormatter(CHARSET_NAME);
		formatter.init(dbMetadata);
	}

	/**
	 * Return column delimiter that is used for creating load utility metadata.
	 * If initDataFormatter() method is overridden then this method couldn't be implemented.
	 *	 
	 * @return column delimiter that is used for creating load utility metadata
	 */
	protected abstract String getColumnDelimiter();
	
	/**
	 * Return record delimiter that is used for creating load utility metadata.
	 * If initDataFormatter() method is overridden then this method couldn't be implemented.
	 *	 
	 * @return record delimiter that is used for creating load utility metadata
	 */
	protected abstract String getRecordDelimiter();
	
	/**
	 * Create consumers for reporting or parsing standard and error output 
	 * from load utility process.
	 * @throws ComponentNotReadyException
	 */
	protected void createConsumers() throws ComponentNotReadyException {
		errConsumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_ERROR, 0);
		consumer = new LoggerDataConsumer(LoggerDataConsumer.LVL_DEBUG, 0);
	}

	/**
	 * Create file for exchange for default behavior of bulkloaders.
	 * Default behaviour means: - not support reading from stdin;
	 *                          - support both windows and unix like systems (uses pipe at unix)
	 * 
	 * @param filePrefix
	 * @throws ComponentNotReadyException
	 */
	protected void defaultCreateFileForExchange(String filePrefix) throws ComponentNotReadyException {
		if (PlatformUtils.isWindowsPlatform() || !StringUtils.isEmpty(dataURL)) {
			if (!StringUtils.isEmpty(dataURL)) {
				dataFile = getFile(dataURL);
				dataFile.delete();
			} else {
				dataFile = createTempFile(filePrefix);
			}
		} else { // for named pipe
			dataFile = createTempFile(filePrefix);
		}
    }
	
	@Override
	public synchronized void free() {
		if(!isInitialized()) return;
		super.free();
		
		if (formatter != null) {
			try {
				formatter.close();
			} catch (IOException e) {
				logger.error(e);
			}
		}
		
		deleteTempFiles();
		
		alreadyExecuted = false;
	}
	
	/**
	 * Delete all temporary files created during execution of component.
	 */
	protected void deleteTempFiles() {
		deleteDataFile();
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
	 * This method reads incoming data from port and sends them by formatter to load utility process.
	 *
	 * @param dataTarget OutputStream where data will be sent
	 * @throws Exception
	 */
	protected void readFromPortAndWriteByFormatter(OutputStream dataTarget) throws Exception {
		formatter.setDataTarget(dataTarget);
		
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecord record = DataRecordFactory.newRecord(dbMetadata);
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
	 * This method reads incoming data from port and sends them by formatter 
	 * to load utility process through dataFile.
	 * 
	 * 
	 * @throws Exception
	 */
	protected void readFromPortAndWriteByFormatter() throws Exception {
		readFromPortAndWriteByFormatter(new FileOutputStream(dataFile));
	}
	
	/**
	 * Call load utility process with parameters,
	 * load utility process reads data directly from file.
	 * 
	 * @return value of finished process
	 * @throws Exception
	 */
	protected int readDataDirectlyFromFile() throws Exception {
		ProcBox box = createProcBox();
		return box.join();
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}
	
	/**
	 * Create instance of ProcBox.
	 * 
	 * @param process running process; when process is null, default process is created
	 * @return instance of ProcBox
	 * @throws IOException
	 */
	protected ProcBox createProcBox(Process process) throws IOException {
		if (process == null) {
			ProcessBuilder processBuilder = new ProcessBuilder(commandLine);

			logger.debug("Process is executed with following settings:");
			logger.debug("Command line: " + processBuilder.command());
			logger.debug("Environment variables: " + processBuilder.environment());
			logger.debug("Working director: " + processBuilder.directory());

			process = processBuilder.start();
		}
		ProcBox box = new ProcBox(process, null, consumer, errConsumer);
    List<Thread> threads = box.getChildThreads();
    for(Thread t:threads) {
      this.registerChildThread(t);
    }
		return box; 
	}
	
	/**
	 * Create instance of ProcBox. Default process is created.
	 * 
	 * @return instance of ProcBox
	 * @throws IOException
	 */
	protected ProcBox createProcBox() throws IOException {
		return createProcBox(null);
	}
	
	/**
	 * Create named pipe - gets name from dataFile.
	 * @throws Exception
	 */
	protected void createNamedPipe() throws Exception {
    	try {
			Process proc = Runtime.getRuntime().exec("mkfifo " + dataFile.getCanonicalPath());
			ProcBox box = new ProcBox(proc, null, consumer, errConsumer);
			List<Thread> threads = box.getChildThreads();
			for(Thread t:threads) {
			  this.registerChildThread(t);
			}
			box.join();
		} catch (Exception e) {
			throw e;
		}
    }

	/**
	 * This class cannot be anonymous, since "ProGuard" obfuscator cannot handle it.
	 * @author MVarecha - created from anonymous inner class
	 */
	private class PipeThread extends Thread {
		@Override
		public void run() {
			try {
				readFromPortAndWriteByFormatter();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	protected int runWithPipe() throws Exception {
    	createNamedPipe();
    	ProcBox box = createProcBox();
    	Thread t = new PipeThread();
		registerChildThread(t);
		t.start();
		return box.join();
    }
	
	/**
	 * Modify metadata so that they correspond to load utility input format.
	 * Each field is delimited and it has the same delimiter.
	 * Only last field can be delimited by another delimiter.
	 * If this method is used then setLoadUtilityDateFormat(DataFieldMetadata)
	 * method must be implemented.
	 *
	 * @param originalMetadata original metadata
	 * @param colDel columnDelimiter
	 * @param recDel recordDelimiter
	 * @return modified metadata
	 */
	private DataRecordMetadata createLoadUtilityMetadata(String colDel, String recDel) {
		InputPort inPort = getInputPort(READ_FROM_PORT);
		DataRecordMetadata metadata = inPort.getMetadata().duplicate();
		metadata.setRecType(DataRecordMetadata.DELIMITED_RECORD);
		
		for (int idx = 0; idx < metadata.getNumFields() - 1; idx++) {
			metadata.getField(idx).setDelimiter(colDel);
			metadata.getField(idx).setSize((short)0);
			setLoadUtilityDateFormat(metadata.getField(idx));
		}
		int lastIndex = metadata.getNumFields() - 1;
		metadata.getField(lastIndex).setDelimiter(recDel);
		metadata.getField(lastIndex).setSize((short)0);
		metadata.setRecordDelimiter("");
		setLoadUtilityDateFormat(metadata.getField(lastIndex));

		return metadata;
	}
	
	/**
	 * If field has format of date or time then default load utility format is set.
	 * This method must be implemented only if 
	 * createLoadUtilityMetadata(DataRecordMetadata, String, String) is used. 
	 * 
	 * @param field
	 */
	protected abstract void setLoadUtilityDateFormat(DataFieldMetadata field);
	
	/** 
	 * Helper method for implementation of setLoadUtilityDateFormat method.
	 * If yearFm is null then year format is not supported. 
	 * 
	 * @param field
	 * @param timeFm time format
	 * @param dateFm date format
	 * @param datetimeFm datetime format
	 * @param yearFm year format
	 */
	protected static void setLoadUtilityDateFormat(DataFieldMetadata field, String timeFm, 
			String dateFm, String datetimeFm, String yearFm) {
		if (field.getType() == DataFieldMetadata.DATE_FIELD || 
				field.getType() == DataFieldMetadata.DATETIME_FIELD) {
			boolean isDate = field.isDateFormat();
			boolean isTime = field.isTimeFormat();
			boolean isOnlyYearFormat = isDate && field.getFormat().matches("(y|Y)*");

			// if formatStr is undefined then DEFAULT_DATETIME_FORMAT is assigned
			if (isOnlyYearFormat && yearFm != null) {
				field.setFormatStr(yearFm);
			} else if ((isDate && isTime) || (StringUtils.isEmpty(field.getFormatStr()))) {
				field.setFormatStr(datetimeFm);
			} else if (isDate) {
				field.setFormatStr(dateFm);
			} else {
				field.setFormatStr(timeFm);
			}
		}
	}
	
	/**
	 * Return true if fileURL exists.
	 * @param fileURL
	 * @return true if fileURL exists else false
	 */
	protected boolean fileExists(String fileURL) throws ComponentNotReadyException {
		if (StringUtils.isEmpty(fileURL)) {
			return false;
		}
		try {
			if (!FileUtils.isServerURL(FileUtils.getInnerAddress(getGraph().getRuntimeContext().getContextURL(), fileURL)) && 
					!(getFile(fileURL).exists())) {
				return false;
			}
			return true;
		} catch (IOException e) {
			throw new ComponentNotReadyException(this, "Cannot access " + fileURL, e);
		}
	}
	
	/**
	 * Creates a new empty file in the temp directory, using the
     * given prefix and suffix strings to generate its name
     * and the file is deleted from file system.
     * 
	 * @param prefix
	 * @param suffix
	 * @return
	 * @throws ComponentNotReadyException
	 */
	protected File createTempFile(String prefix, String suffix) throws ComponentNotReadyException {
		try {
			File file = getGraph().getAuthorityProxy().newTempFile(prefix, suffix, -1);
			file.delete();
			return file;
		} catch (TempFileCreationException e) {
			free();
			throw new ComponentNotReadyException(this, "Temporary data file wasn't created.", e);
		}
	}
	
	protected File createTempFile(String prefix) throws ComponentNotReadyException {
    	return createTempFile(prefix, null);
    }
	
	protected File openFile(String fileURL) throws ComponentNotReadyException {
		if (!fileExists(fileURL)) {
			free();
			throw new ComponentNotReadyException(this, 
					"Data file " + StringUtils.quote(fileURL) + " not exists.");
		}
		return getFile(fileURL);
    }

	/**
	 * Delete file if the file is temporary.
	 * File is temporary when fileURL parameter isn't defined. 
	 * 
	 * @param commandFile
	 * @param commandURL
	 */
	protected static void deleteTempFile(File file, String fileURL) {
		deleteTempFile(file, fileURL, true);
	}
	
	/**
	 * Delete file if the file is temporary.
	 * File is temporary when fileURL parameter isn't defined. 
	 * 
	 * @param commandFile
	 * @param commandURL
	 */
	protected static void deleteTempFile(File file, String fileURL, boolean warn) {
		if (file == null) {
			return;
		}

		if (StringUtils.isEmpty(fileURL) && !file.delete() && warn) {
			logger.warn("Temp data file was not deleted.");
		}
	}
	
	/**
	 * Delete file and report to log when it isn't possible.
	 * @param fileURL
	 */
	protected static void deleteFile(String fileURL) {
    	if (StringUtils.isEmpty(fileURL)) {
    		return;
    	}
    	
    	File file = new File(fileURL);
    	if (!file.delete()) {
        	logger.warn("Temp file " + StringUtils.quote(fileURL) + " was not deleted.");        	
        }
    }
	
	/**
	 * Create instance of Properties from String. 
	 * Parse parameters from string "parameters" and fill properties by them.
	 * 
	 * @param parameters string that contains parameters
	 * @return instance of Properties created by parsing string
	 */
	public static Properties parseParameters(String parameters) {
		Properties properties = new Properties();

		if (parameters != null) {
			for (String param : StringUtils.split(parameters)) {
				String[] par = param.split(EQUAL_CHAR);
				properties.setProperty(par[0], par.length > 1 ? StringUtils.unquote(par[1]) : "true");
			}
		}

		return properties;
	}
	
	@SuppressWarnings("unchecked")
	public static String getPropertiesAsString(Properties properties) {
		StringBuilder props = new StringBuilder();
		boolean firstProp = true;
		for (Iterator iter = properties.entrySet().iterator(); iter.hasNext();) {
			if (!firstProp) {
				props.append('|');
			}
			Entry<String, String> element = (Entry<String, String>) iter.next();
			props.append(element.getKey());
			if (!equalsTrue(element.getValue())) {
				props.append('=');
				props.append(quote(element.getValue()));
			}
			
			firstProp = false;
		}
		return props.toString();
	}
	
	/** 
	 * Return quoted 'value'. In case quotes are present doesn't do anything.
	 * @param value
	 * @return quoted 'value'
	 */
	private static String quote(String value) {
		return StringUtils.isQuoted(value) ? value : StringUtils.quote(value);
	}
	
	/**
	 * @param value
	 * @return true if 'value' equals true with ignoring case sensitive
	 */
	private static boolean equalsTrue(String value) {
		return StringUtils.unquote(value).equalsIgnoreCase("true");
	}
	
	/**
	 * Print system command with it's parameters to log. 
	 * @param command
	 */
	private static void printCommandLineToLog(String[] command) {
		StringBuilder msg = new StringBuilder("System command: \"");
		msg.append(command[0]).append("\" with parameters:\n");
		for (int idx = 1; idx < command.length; idx++) {
			msg.append(idx).append(": ").append(command[idx]).append("\n");
		}
		logger.debug(msg.toString());
	}
	
	protected void setUser(String user) {
		this.user = user;
	}

	protected void setPassword(String password) {
		this.password = password;
	}
	
	protected void setColumnDelimiter(String columnDelimiter) {
		this.columnDelimiter = columnDelimiter;
	}
	
	protected void setFileUrl(String dataURL) {
		this.dataURL = dataURL;
	}
	
	protected void setTable(String table) {
		this.table = table;
	}
	
	protected void setParameters(String parameters) {
		this.parameters = parameters;
	}
	
	protected void setHost(String host) {
		this.host = host;
	}
	
	@Override
	public void toXML(Element xmlElement) {
		super.toXML(xmlElement);
		
		if (!StringUtils.isEmpty(database)) {
			xmlElement.setAttribute(XML_DATABASE_ATTRIBUTE, database);
		}
		if (!StringUtils.isEmpty(table)) {
			xmlElement.setAttribute(XML_TABLE_ATTRIBUTE, table);
		}
		if (!StringUtils.isEmpty(user)) {
			xmlElement.setAttribute(XML_USER_ATTRIBUTE, user);
		}
		if (!StringUtils.isEmpty(password)) {
			xmlElement.setAttribute(XML_PASSWORD_ATTRIBUTE, password);
		}
		if (!StringUtils.isEmpty(dataURL)) {
			xmlElement.setAttribute(XML_FILE_URL_ATTRIBUTE, dataURL);
		}
		if (!StringUtils.isEmpty(parameters)) {
			xmlElement.setAttribute(XML_PARAMETERS_ATTRIBUTE, parameters);
		} else if (!properties.isEmpty()) {
			xmlElement.setAttribute(XML_PARAMETERS_ATTRIBUTE, getPropertiesAsString(properties));
		}
	}
	
	/**
     * Creates absolute file path based on file and graph's projectURL. 
     * @param file file
     * @return
     * @throws ComponentNotReadyException
     */
	protected String getFilePath(File file) throws ComponentNotReadyException {
    	if (file == null) {
    		return null;
    	}

    	return getFilePath(file.getAbsolutePath());
    }
    
	/**
     * Creates absolute file path based on fileURL string and graph's projectURL. 
     * @param fileURL name of the file
     * @return
     * @throws ComponentNotReadyException
     */
	protected String getFilePath(String fileURL) throws ComponentNotReadyException {
    	File file = getFile(fileURL);
    	if (file == null) {
    		return null;
    	}
    	
    	try { // canonical - /xx/xx -- absolute - /xx/./xx
			return file.getCanonicalPath();
		} catch (IOException ioe) {
			return file.getAbsolutePath();
		}
    }
    
    /**
     * Creates File object based on fileURL string and graph's projectURL. 
     * @param fileURL name of the file
     * @return
     * @throws ComponentNotReadyException
     */
    protected File getFile(String fileURL) throws ComponentNotReadyException {
    	if (StringUtils.isEmpty(fileURL)) {
    		return null;
    	}
    	
		try {
			return new File(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), fileURL));
		} catch (MalformedURLException mue) {
			throw new ComponentNotReadyException(this, "Malformed file URL: " + fileURL, mue);
		}
    }
}