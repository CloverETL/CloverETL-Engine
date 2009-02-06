package org.jetel.component;

import java.util.Properties;

import org.jetel.data.formatter.Formatter;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.Node;
import org.jetel.util.exec.DataConsumer;
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
	
	protected String parameters = null;
	protected Properties properties = null;
	
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
		super.init();
		
		isDataReadFromPort = !getInPorts().isEmpty();
        isDataWrittenToPort = !getOutPorts().isEmpty();
		
		properties = parseParameters(parameters);
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

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		
		if (formatter != null) {
			formatter.reset();
		}
	}
}
