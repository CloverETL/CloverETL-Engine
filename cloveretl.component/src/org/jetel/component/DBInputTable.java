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

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.connection.jdbc.SQLDataParser;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.parser.TextParser;
import org.jetel.data.parser.TextParserFactory;
import org.jetel.database.IConnection;
import org.jetel.database.sql.DBConnection;
import org.jetel.database.sql.JdbcSpecific.OperationType;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.AutoFilling;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.ReadableChannelIterator;
import org.jetel.util.file.FileUtils;
import org.jetel.util.joinKey.JoinKeyUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.PropertyRefResolver;
import org.jetel.util.property.RefResFlag;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>DatabaseInputTable Component</h3>
 *
 * <!-- This component reads data from DB. It first executes specified query on DB and then
 *  extracts all the rows returned. -->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>DBInputTable</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>This component reads data from DB. It first executes specified query on DB and then
 *  extracts all the rows returned.</td></tr>
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
 *  <tr><td><b>type</b></td><td>"DB_INPUT_TABLE"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>sqlQuery</b><br><i>optional</i></td><td>query to be sent to database. The query can contain mapping between clover and 
 *  database fields eg. query: <code>select $field1:=dbField1, $field2:=dbField2 from mytable</code> is interpreted as: 
 *  <code>select dbField1, dbField2 from mytable</code> and output field <i>field1</i> will be filled by value from
 *  <i>dbField1</i> and <i>field2</i> will be filled by value from <i>dbField2</i>. The query can be written without
 *  mapping also; then output fields will be fulfilled from the first in order data flows from database. For incremental 
 *  reading clause <code>where</code> defining new records must be present (see <i>incrementalKey, incrementalFile</i> 
 *  attributes), eg. query for incremental reading should look like: <code>select $f1:=db1, $f2:=db2, ... from myTable where dbX > #myKey1 and dbY <=#myKey2</code>,
 *   where <i>myKey1</i> and <i>myKey2</i> must be difined in <i>incrementalKey</i> attribute. 
 *  <br> <i>sqlQuery</i> or <i>url</i> must be defined</td>
 *  <tr><td><b>url</b><br><i>optional</i></td><td>url location of the query<br>the query will be loaded from file 
 *  referenced by url. Syntax of the query must be as described above. Also the query can be  read from input port (see {@link DataReader} component).</td></td>
 *  <tr><td><b>incrementalKey </b></td><td>defines on which db fields incremental values are defined and which record from result set
 *  will be stored (<b>last</b>, <b>first</b>, <b>min</b> or <b>max</b>). Key parts have to be separated by by :;| {colon, semicolon, pipe},
 *  eg.:<i>myKey1=first(dbX);myKey2=min(dbY)</i> (see <i>sqlQuery</i> attribute)</td></tr>
 *  <tr><td><b>incrementalFile </b></td><td>url to file where key values are stored. Values have to be set by user for 1st reading, then are set to 
 *   requested value (see <i>sqlQuery, incrementalKey</i> attributes) automatically, eg.<br> <i>myKey1=0<br>myKey2=1990-01-01</i><br>Dates, times and timestamps have be written
 *   in format defined in @see Defaults.DEFAULT_DATE_FORMAT, Defaults.DEFAULT_TIME_FORMAT, Defaults.DEFAULT_DATETIME_FORMAT</td></tr>
 *  <tr><td><b>charset </b><i>optional</i></td><td>encoding of extern query</td></tr>
 *  <tr><td><b>dbConnection</b></td><td>id of the Database Connection object to be used to access the database</td>
 *  <tr><td><b>fetchSize</b><br><i>optional</i></td><td>how many records should be fetched from db at once. <i>See JDBC's java.sql.Statement.setFetchSize()</i><br><b><code>MIN_INT</code></b> constant
 * is implemented - is resolved to Integer.MIN_INT value <i>(good for MySQL JDBC driver)</i></td>
 *  <tr><td>&lt;SQLCode&gt;<br><i>optional<small>!!XML tag!!</small></i></td><td>This tag allows for embedding large SQL statement directly into graph.. See example below.</td></tr>
 *  <tr><td><b>DataPolicy</b></td><td>specifies how to handle misformatted or incorrect data.  'Strict' (default value) aborts processing, 'Controlled' logs the entire record while processing continues, and 'Lenient' attempts to set incorrect data to default values while processing continues.</td></tr>
 *  <tr><td><b>autoCommit</b><i>optional</i></td><td>Whether the commit should automatically be called after retrieving data. Default: Yes</td></tr>
 *  </table>
 *
 *  <br>sqlQuery and url are mutually exclusive.  url is the primary and if found the sqlQuery will not be used.<br>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="INPUT" type="DB_INPUT_TABLE" dbConnection="NorthwindDB" sqlQuery="select * from employee_z"/&gt;</pre>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="INPUT" type="DB_INPUT_TABLE" dbConnection="NorthwindDB" url="c:/temp/test.sql"/&gt;</pre>
 *	
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="INPUT" type="DB_INPUT_TABLE" dbConnection="NorthwindDB" &gt;
 *  &lt;SQLCode&gt;
 *	select * from employee_z
 *  &lt;/SQLCode&gt;
 *  &lt;/Node&gt;
 *  </pre>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node dbConnection="DBConnection0" id="INPUT" 
 *  sqlQuery="select $last_name:=last_name,$full_name:=full_name from employee" type="DB_INPUT_TABLE"/&gt;</pre>
 *
 *  <h4>Incremental reading example:</h4>
 *  <pre>&lt;Node dbConnection="DBConnection0" id="INPUT" 
 *  incrementalFile="dbInc.txt" 
 *  incrementalKey="key1=last(id);key2=max(last_update)" 
 *  sqlQuery="select * from employee where id &gt; #key1 or last_update&gt;#key2" type="DB_INPUT_TABLE"/&gt;
 *  
 *  Starting content of dbInc.txt:
 *  	key1=0
 *  	key2=1999-12-31 </pre>
 *	
 * @author      dpavlis
 * @since       September 27, 2002
 * @see         org.jetel.database.AnalyzeDB
 */
public class DBInputTable extends Node {

    static Log logger = LogFactory.getLog(DBInputTable.class);
	
	public static final String XML_DATAPOLICY_ATTRIBUTE = "dataPolicy";
	public static final String XML_DBCONNECTION_ATTRIBUTE = "dbConnection";
	public static final String XML_SQLQUERY_ATTRIBUTE = "sqlQuery";
	public static final String XML_URL_ATTRIBUTE = "url";
	public static final String XML_FETCHSIZE_ATTRIBUTE = "fetchSize";
	public static final String XML_AUTOCOMMIT_ATTRIBUTE = "autoCommit";
	public static final String XML_SQLCODE_ELEMENT = "SQLCode";
	public static final String XML_CHARSET_ATTRIBUTE = "charset"; 
	public static final String XML_INCREMENTAL_FILE_ATTRIBUTE = "incrementalFile";
	public static final String XML_INCREMENTAL_KEY_ATTRIBUTE = "incrementalKey";
	public static final String XML_PRINTSTATEMENTS_ATTRIBUTE = "printStatements";
	
	private PolicyType policyType;
	private TextParser inputParser;

	private String dbConnectionName;
	private String sqlQuery;
	
	private int fetchSize=0;

	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DB_INPUT_TABLE";
	private final static int READ_FROM_PORT = 0;
	private final static int WRITE_TO_PORT = 0;
	private String url = null;
	private boolean printStatements;
	private String charset;
	private ReadableChannelIterator channelIterator;  // for reading queries from URL or input port
	private DBConnection connection;

	private DataRecordMetadata statementMetadata;
	
	private String incrementalFile;
	private Properties incrementalKeyDef;
	private Properties incrementalKeyPosition = new Properties();
	
    private AutoFilling autoFilling = new AutoFilling();
    private boolean autoCommit = true;
    
	/**
	 *Constructor for the DBInputTable object
	 *
	 * @param  id                Description of Parameter
	 * @param  dbConnectionName  Description of Parameter
	 * @param  sqlQuery          Description of Parameter
	 * @since                    September 27, 2002
	 */
	public DBInputTable(String id, String dbConnectionName, String sqlQuery) {
		super(id);
		this.dbConnectionName = dbConnectionName;
		this.sqlQuery = sqlQuery;
	}    
    
	/**
	 *Constructor for the DBInputTable object
	 *
	 * @param  id                Description of Parameter
	 * @param  dbConnectionName  Description of Parameter
	 * @param  sqlQuery          Description of Parameter
	 * @param  autoCommit        Description of Parameter
	 * @since                    September 27, 2002
	 */
	public DBInputTable(String id, String dbConnectionName, String sqlQuery, boolean autoCommit) {
		super(id);
		this.dbConnectionName = dbConnectionName;
		this.sqlQuery = sqlQuery;
		this.autoCommit = autoCommit;
	}


	/**
	 *  Description of the Method
	 *
	 * @exception  ComponentNotReadyException  Description of Exception
	 * @since                                  September 27, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
        IConnection conn = getGraph().getConnection(dbConnectionName);
        if (conn==null){
            throw new ComponentNotReadyException("Can't obtain DBConnection object: \""+dbConnectionName+"\"");
        }
        if(!(conn instanceof DBConnection)) {
            throw new ComponentNotReadyException("Connection with ID: " + dbConnectionName + " isn't instance of the DBConnection class.");
        }

        if (sqlQuery == null) {
        	// we'll be reading SQL from file or input port
        	channelIterator = new ReadableChannelIterator(getInputPort(READ_FROM_PORT),
        			getGraph().getRuntimeContext().getContextURL(), url);
			channelIterator.setCharset(charset);
			channelIterator.init();
			
			//statements are single strings delimited by delimiter
			statementMetadata = new DataRecordMetadata("_statement_metadata_", DataRecordMetadata.DELIMITED_RECORD);
			DataFieldMetadata statementField = new DataFieldMetadata("_statement_field_", DataFieldMetadata.STRING_FIELD, null);
			statementField.setEofAsDelimiter(true);
			statementField.setTrim(true);
			statementMetadata.addField(statementField);
			inputParser = TextParserFactory.getParser(statementMetadata, charset);
			inputParser.init();
        }
        
		connection = (DBConnection)conn;
        connection.init();
		if (incrementalFile != null) {
			try {
				//parser.setIncrementalFile(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), incrementalFile));
				incrementalFile = FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), incrementalFile);
			} catch (MalformedURLException e) {
				throw new ComponentNotReadyException(this,
						XML_INCREMENTAL_FILE_ATTRIBUTE, e);
			}
		}
		
		List<DataRecordMetadata> lDataRecordMetadata;
    	if ((lDataRecordMetadata = getOutMetadata()).size() > 0) 
    		autoFilling.addAutoFillingFields(lDataRecordMetadata.get(0));
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		
	}
	
	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
		if (firstRun()) {// a phase-dependent part of initialization
			//all necessary elements have been initialized in init()
		} else {
			autoFilling.reset();
			if (channelIterator != null) {
				channelIterator.reset();
			}
		}
	}


	@Override
	public Result execute() throws Exception {
		try {
			SQLDataParser parser = null;
			if (sqlQuery != null) {
				// we have only single query
				parser = processSqlQuery(sqlQuery);
			} else {
				// process queries from file or input port
				PropertyRefResolver propertyResolver = new PropertyRefResolver(getGraph().getGraphProperties());
				while (channelIterator.hasNext()) {
					Object source = channelIterator.next();
					if (source == null) break; // no more data in input port
					inputParser.setDataSource(source);
					DataRecord statementRecord = DataRecordFactory.newRecord(statementMetadata);
					statementRecord.init();
    				//read statements from byte channel
    				while ((statementRecord = inputParser.getNext(statementRecord)) != null) {
    					String sqlStatement = propertyResolver.resolveRef(statementRecord.getField(0).toString());
       					if (printStatements) {
    						logger.info("Executing statement: " + sqlStatement);
    					}
       					parser = processSqlQuery(sqlStatement);
    				}
				}
			}
			// save values of incremental key into file
			storeValues(parser);
		} finally {
    		broadcastEOF();
		}
        return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}
	
	private SQLDataParser processSqlQuery(String sqlQuery) throws Exception {
		SQLDataParser parser = new SQLDataParser(getOutputPort(WRITE_TO_PORT).getMetadata(), sqlQuery);
		try {
			parser.setIncrementalKey(incrementalKeyDef);
			parser.setIncrementalFile(incrementalFile);
			parser.setAutoCommit(autoCommit);
			
			//set fetch size (if defined)
			if (fetchSize != 0) parser.setFetchSize(fetchSize);
			parser.init();
			parser.setParentNode(this);
	        parser.setExceptionHandler(ParserExceptionHandlerFactory.getHandler(policyType));

    		// we need to create data record - take the metadata from first output port
    		DataRecord record = DataRecordFactory.newRecord(getOutputPort(WRITE_TO_PORT).getMetadata());
    		record.init();
    		record.reset();
			parser.setDataSource(connection.getConnection(getId(), OperationType.READ));
    		autoFilling.setFilename(sqlQuery);

    		// till it reaches end of data or it is stopped from outside
			while (record != null && runIt) {
				try {
					record = parser.getNext(record);
					if (record != null) {
						autoFilling.setAutoFillingFields(record);
						writeRecordBroadcast(record);
					}
				} catch (BadDataFormatException bdfe) {
					if (policyType == PolicyType.STRICT) {
						throw bdfe;
					} else {
						logger.info(ExceptionUtils.getMessage(bdfe));
					}
				}
			}
			// update values of incremental key. (We cannot store parser's incremental key now, but only after all SQL statement are processed.)
			parser.megrePosition(incrementalKeyPosition);
		} finally {
        	parser.close();
		}
		return parser;
	}
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		connection.closeConnection(getId(), OperationType.READ);
	}
	
	@Override
    public synchronized void free() {
    	super.free();
    }

	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 * @since           September 27, 2002
	 */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws Exception {
            ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
            ComponentXMLAttributes xattribsChild;
            DBInputTable aDBInputTable = null;
            org.w3c.dom.Node childNode;

            String query;
            if (xattribs.exists(XML_URL_ATTRIBUTE)) {
            	query = null;
            } else if (xattribs.exists(XML_SQLQUERY_ATTRIBUTE)){
                query = xattribs.getString(XML_SQLQUERY_ATTRIBUTE);
            }else if (xattribs.exists(XML_SQLCODE_ELEMENT)){
                query = xattribs.getString(XML_SQLCODE_ELEMENT);
            }else{
                
                childNode = xattribs.getChildNode(xmlElement, XML_SQLCODE_ELEMENT);
                if (childNode == null) {
                    throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ": Can't find <SQLCode> node !");
                }
                xattribsChild = new ComponentXMLAttributes(xmlElement, graph);
                query=xattribsChild.getText(childNode);

    			
            }

            aDBInputTable = new DBInputTable(xattribs.getString(XML_ID_ATTRIBUTE),
                    xattribs.getString(XML_DBCONNECTION_ATTRIBUTE),
                    query);
            
            aDBInputTable.setPolicyType(xattribs.getString(XML_DATAPOLICY_ATTRIBUTE,null));
            if (xattribs.exists(XML_FETCHSIZE_ATTRIBUTE)){
            	aDBInputTable.setFetchSize(xattribs.getInteger(XML_FETCHSIZE_ATTRIBUTE));
            }
            if (xattribs.exists(XML_URL_ATTRIBUTE)) {
            	aDBInputTable.setURL(xattribs.getStringEx(XML_URL_ATTRIBUTE, RefResFlag.URL));
            }
            if (xattribs.exists(XML_PRINTSTATEMENTS_ATTRIBUTE)) {
                aDBInputTable.setPrintStatements(xattribs.getBoolean(XML_PRINTSTATEMENTS_ATTRIBUTE));
            }
            if (xattribs.exists(XML_CHARSET_ATTRIBUTE)) {
            	aDBInputTable.setCharset(xattribs.getString(XML_CHARSET_ATTRIBUTE));
            }
            if (xattribs.exists(XML_INCREMENTAL_FILE_ATTRIBUTE)) {
            	aDBInputTable.setIncrementalFile(xattribs.getStringEx(XML_INCREMENTAL_FILE_ATTRIBUTE, RefResFlag.URL));
            }
            if (xattribs.exists(XML_INCREMENTAL_KEY_ATTRIBUTE)) {
            	aDBInputTable.setIncrementalKey(xattribs.getString(XML_INCREMENTAL_KEY_ATTRIBUTE));
            }
            if (xattribs.exists(XML_AUTOCOMMIT_ATTRIBUTE)) {
            	aDBInputTable.setAutoCommit(xattribs.getBoolean(XML_AUTOCOMMIT_ATTRIBUTE));
            }                

            return aDBInputTable;
	}

    /**
     * Stores all values as incremental reading.
     */
    private void storeValues(SQLDataParser parser) {
    	if (parser == null) return;
		try {
			Object dictValue = getGraph().getDictionary().getValue(Defaults.INCREMENTAL_STORE_KEY);
			if (dictValue != null && dictValue == Boolean.FALSE) {
				return;
			}
			parser.storeIncrementalReading(incrementalKeyPosition);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
    }

	/**
	 * @param xml_url_attribute2
	 */
	public void setURL(String url){
		this.url = url;
	}



	/**  Description of the Method */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        
        if(!checkInputPorts(status, 0, 1) 
        		|| !checkOutputPorts(status, 1, Integer.MAX_VALUE)) {
        	return status;
        }
        
        checkMetadata(status, getOutMetadata());
        
        try {
            IConnection conn = getGraph().getConnection(dbConnectionName);
            if (conn==null){
                throw new ComponentNotReadyException("Can't obtain DBConnection object: \""+dbConnectionName+"\"");
            }
            if(!(conn instanceof DBConnection)) {
                throw new ComponentNotReadyException("Connection with ID: " + dbConnectionName + " isn't instance of the DBConnection class.");
            }
            connection = (DBConnection)conn;
            connection.init();
            SQLDataParser parser = new SQLDataParser(getOutputPort(WRITE_TO_PORT).getMetadata(), sqlQuery); // TODO is it OK?
            parser.init();
    		if (incrementalFile != null) {
				try {
					parser.setIncrementalFile(FileUtils.getFile(getGraph().getRuntimeContext().getContextURL(), incrementalFile));
					parser.setIncrementalKey(incrementalKeyDef);
					parser.checkIncremental(connection.getJdbcSpecific());
				} catch (MalformedURLException e1) {
					// -pnajvar
					// Throwing and exception halts the entire graph which might not be correct as inc file
					// can be created at graph runtime. Instead just log it
					// issue #2127
		            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e1), ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL);
		            problem.setAttributeName(XML_INCREMENTAL_FILE_ATTRIBUTE);
		            status.add(problem);
				} catch (ComponentNotReadyException e2) {
					// -pnajvar
					// Throwing and exception halts the entire graph which might not be correct as inc file
					// can be created at graph runtime. Instead just log it
					// issue #2127
		            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e2), ConfigurationStatus.Severity.WARNING, this, ConfigurationStatus.Priority.NORMAL);
		            if(!StringUtils.isEmpty(e2.getAttributeName())) {
		                problem.setAttributeName(e2.getAttributeName());
		            }
		            status.add(problem);
				}
			}
        } catch (ComponentNotReadyException e) {
            ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
            if(!StringUtils.isEmpty(e.getAttributeName())) {
                problem.setAttributeName(e.getAttributeName());
            }
            status.add(problem);
        } 
        
        return status;
    }
	
	@Override
	public String getType(){
		return COMPONENT_TYPE;
	}

	public void setFetchSize(int fetchSize){
	    this.fetchSize=fetchSize;
	}

    public void setPolicyType(String strPolicyType) {
        setPolicyType(PolicyType.valueOfIgnoreCase(strPolicyType));
    }
    
	/**
	 * Adds BadDataFormatExceptionHandler to behave according to DataPolicy.
	 *
	 * @param  handler
	 */
	public void setPolicyType(PolicyType policyType) {
        this.policyType = policyType;
	}


	public void setIncrementalFile(String incrementalFile) throws MalformedURLException {
		this.incrementalFile = incrementalFile;
	}


	public void setIncrementalKey(String incrementalKey) {
		String[] key = StringUtils.split(incrementalKey);
		incrementalKeyDef = new Properties();
		String[] def;
		for (int i = 0; i < key.length; i++) {
			def = JoinKeyUtils.getMappingItemsFromMappingString(key[i], "=");
			String value = def[1].trim();
			if (value.startsWith("\"") && value.endsWith("\"")) {
				value = value.substring(1, value.length()-1);
			}
			incrementalKeyDef.setProperty(def[0].trim(), value);
		}
	}
	
	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setPrintStatements(boolean printStatements) {
		this.printStatements = printStatements;
	}

	public boolean isAutoCommit() {
		return autoCommit;
	}

	public void setAutoCommit(boolean autoCommit) {
		this.autoCommit = autoCommit;
	}
 }
