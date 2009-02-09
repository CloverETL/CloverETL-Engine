package org.jetel.component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.Formatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.exec.DataConsumer;
import org.jetel.util.exec.ProcBox;
import org.jetel.util.file.FileUtils;
import org.jetel.util.string.StringUtils;

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

	protected final static String XML_DATABASE_ATTRIBUTE = "database";
	protected final static String XML_TABLE_ATTRIBUTE = "table";
	protected final static String XML_USER_ATTRIBUTE = "username";
	protected final static String XML_PASSWORD_ATTRIBUTE = "password";
	protected final static String XML_COLUMN_DELIMITER_ATTRIBUTE = "columnDelimiter";
	protected final static String XML_FILE_URL_ATTRIBUTE = "fileURL";
	protected final static String XML_PARAMETERS_ATTRIBUTE = "parameters";
	
	protected final static String EQUAL_CHAR = "=";
	protected final static int UNUSED_INT = -1;
	protected final static File TMP_DIR = new File(".");
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
	
	public BulkLoader(String id, String loadUtilityPath, String database) {
		super(id);
		this.loadUtilityPath = loadUtilityPath;
		this.database = database;
	}

	@Override
	public void init() throws ComponentNotReadyException {
		if (isInitialized()) return;
		super.init();
		
		isDataReadFromPort = !getInPorts().isEmpty();
        isDataWrittenToPort = !getOutPorts().isEmpty();
		
		properties = parseParameters(parameters);
	}

	@Override
	public synchronized void free() {
		if(!isInitialized()) return;
		super.free();
		
		if (formatter != null) {
			formatter.close();
		}
	}
	
	/**
	 * Create instance of Properties from String. 
	 * Parse parameters from string "parameters" and fill properties by them.
	 * 
	 * @param parameters string that contains parameters
	 * @return instance of Properties created by parsing string
	 */
	private static Properties parseParameters(String parameters) {
		Properties properties = new Properties();

		if (parameters != null) {
			for (String param : StringUtils.split(parameters)) {
				String[] par = param.split(EQUAL_CHAR);
				properties.setProperty(par[0], par.length > 1 ? StringUtils.unquote(par[1]) : "true");
			}
		}

		return properties;
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
		DataRecord record = new DataRecord(dbMetadata);
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
		
		if (formatter != null) {
			formatter.reset();
		}
	}
	
	/**
	 * Create instance of ProcBox.
	 * 
	 * @param process running process; when process is null, default process is created
	 * @return instance of ProcBox
	 * @throws IOException
	 */
	protected abstract ProcBox createProcBox(Process process) throws IOException;
	// TODO implementation will be copied from descendant after removing commandLine attribute to this class
	
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
			box.join();
		} catch (Exception e) {
			throw e;
		}
    }
	
	protected int runWithPipe() throws Exception {
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
			if (!FileUtils.isServerURL(FileUtils.getInnerAddress(fileURL)) && 
					!(new File(FileUtils.getFile(getGraph().getProjectURL(), fileURL))).exists()) {
				return false;
			}
			return true;
		} catch (IOException e) {
			throw new ComponentNotReadyException(this, e);
		}
	}
	
	protected File createTempFile(String prefix) throws ComponentNotReadyException {
    	try {
			File file = File.createTempFile(prefix, null, TMP_DIR);
			file.delete();
			return file;
		} catch (IOException e) {
			free();
			throw new ComponentNotReadyException(this, 
					"Temporary data file wasn't created.");
		}
    }
	
	protected File openFile(String fileURL) throws ComponentNotReadyException {
		if (!fileExists(fileURL)) {
			free();
			throw new ComponentNotReadyException(this, 
					"Data file " + StringUtils.quote(fileURL) + " not exists.");
		}
		return new File(fileURL);
    }

	/**
	 * Delete file if the file is temporary.
	 * File is temporary when fileURL parameter is defined. 
	 * 
	 * @param commandFile
	 * @param commandURL
	 * @param logger
	 */
	protected static void deleteTempFile(File file, String fileURL, Log logger) {
		if (file == null) {
			return;
		}

		if (StringUtils.isEmpty(fileURL) && !file.delete()) {
			logger.warn("Temp command data file was not deleted.");
		}
	}
	
	/**
	 * Delete file and report to log when it isn't possible.
	 * @param fileName
	 * @param logger
	 */
	protected static void deleteFile(String fileName, Log logger) {
    	if (StringUtils.isEmpty(fileName)) {
    		return;
    	}
    	
    	File file = new File(fileName);
    	if (!file.delete()) {
        	logger.warn("Temp file " + StringUtils.quote(fileName) + " was not deleted.");        	
        }
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
}