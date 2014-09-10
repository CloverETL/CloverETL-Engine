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
package com.cloveretl.tableau;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.BooleanDataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.DateDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.NumericDataField;
import org.jetel.data.StringDataField;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldContainerType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.date.DateFieldExtractor;
import org.jetel.util.date.DateFieldExtractorFactory;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

import com.cloveretl.tableau.TableauTableStructureParser.TableauTableColumnDefinition;
import com.tableausoftware.TableauException;
import com.tableausoftware.DataExtract.Collation;
import com.tableausoftware.DataExtract.Extract;
import com.tableausoftware.DataExtract.Row;
import com.tableausoftware.DataExtract.Table;
import com.tableausoftware.DataExtract.TableDefinition;
import com.tableausoftware.DataExtract.Type;

public class TableauWriter extends Node  {
	
	private static final ReentrantLock lock = new ReentrantLock();

	public final static String TABLEAU_WRITER = "TABLEAU_WRITER";

	public static final String XML_ACTION_ON_EXISTING_FILE = "actionOnExistingFile";
	
	/* 
	 * Note mtomcanyi:
	 * 	
	 * Tableau API seems to accept table name but throws exception when table name is anything else than "Extract".
	 * Component code is ready accept table name from user, so when Tableau API start accepting it we can just uncomment our code
	 * in fromXML() and customcomponents.xml and make table name configurable.
	 */
	public static final String XML_TABLE_NAME = "tableName";
	
	public static final String XML_OUTPUT_FILE = "outputFile";
	public static final String XML_DEFAULT_TABLE_COLLATION = "defaultTableCollation";
	public static final String XML_TABLE_STRUCTURE = "tableStructure";
	public static final String XML_TIMEOUT = "timeout";

	// output file suffix required by Tableau
	private static final String REQUIRED_FILE_SUFFIX = ".tde";
	
	// Attributes initialized from XML configuration
	
	private String outputFileName;
	private String tableName;
	private TableauActionOnExistingFile actionOnExistingFile;
	private String actionOnExistingFileRaw;
	private String rawTableCollation;
	private Collation defaultTableCollation;
	private String tableStructure;
	// how long should component wait for write lock, after this timeout the component run fails
	private long synchroLockTimeoutSeconds; 
	
	// field name and its table column definition
	private HashMap<String, TableauTableColumnDefinition> mappings;

	// Component inputs
	private InputPort inputPort;
	private DataRecordMetadata inputMetadata;
	private DataRecord inputRecord;

	// Output structures
	private Extract targetExtract;
	private Table targetTable;
	private TableDefinition tableDefinition;

	// Calendar instance to convert from input dates to Tableau date fields
	private DateFieldExtractor[] extractors;
	
	static Log logger = LogFactory.getLog(TableauWriter.class);
	
	public TableauWriter(String id, TransformationGraph graph, String outputFileName, String tableName, String rawTableCollation, String actionOnExistingFileRaw, String tableStructure, long timeout) {
		super(id, graph);
		this.outputFileName = outputFileName;
		this.tableName = tableName;
		this.rawTableCollation= rawTableCollation;
		this.actionOnExistingFileRaw = actionOnExistingFileRaw;
		this.tableStructure = tableStructure;
		this.synchroLockTimeoutSeconds = timeout;
	}
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		prepareInputRecord();
		
		if (defaultTableCollation == null) {
			checkDefaultCollation(null);
		}
		
		try {
        	actionOnExistingFile = TableauActionOnExistingFile.valueOf(actionOnExistingFileRaw);
        } catch (IllegalArgumentException e) {
        	throw new ComponentNotReadyException("Invalid value of Action on existing file. Value was: " + actionOnExistingFileRaw);
        }
		
		mappings = new TableauTableStructureParser(tableStructure, true, inputMetadata).getTableauMapping();
	}
	
	@Override
	protected Result execute() throws Exception {
		
		// Tableau API is not thread-safe. We use a lock to deal with it.
		// We wait for first record to come in. This prevents locking the tableau API too early.
		if (readRecord() && runIt) {
			logger.info(getName() + " is trying to acquire lock for Tableau API.");
			boolean lockSuccess = lock.tryLock(synchroLockTimeoutSeconds, TimeUnit.SECONDS); // thread is waiting
			
			if (!lockSuccess) {
				logger.error(getName() + " didn't acquire Tableau API lock in time. You can try increasing the"
						+ " component's timeout or putting the TableauWriters in different phases.");
				throw new Exception("TableauWriter: component " + getName() + " didn't get lock in time. "
						+ "Component waited for " + synchroLockTimeoutSeconds + " seconds.");
			}
			
			// we have the lock at this point
			logger.info(getName() + " acquired lock for Tableau API.");
			
			try {
				// check and prepare target file
				prepareTargetFile();
				prepareTargetTable();
				prepareValueConvertors();
				
				 // first record was already read
				writeRecord();
				
				while (readRecord() && runIt) {
					writeRecord();
				}
				
			} finally {
				if (lockSuccess) {
					if (targetExtract != null) {
						targetExtract.close();
					}
					lock.unlock();
					logger.info(getName() + " released lock for Tableau API.");
				}
			}
		}
		
		return Result.FINISHED_OK;
	}
	
	
	private boolean readRecord() throws InterruptedException, IOException{
		return (inputPort.readRecord(inputRecord) != null);
	}
	
	
	private void writeRecord() throws ComponentNotReadyException {
		try {
			Row outputRow = new Row(this.tableDefinition);
			
			for (int i=0; i<inputRecord.getNumFields();i++) {
				DataFieldMetadata fieldMetadata=inputMetadata.getField(i); 
				
				if (inputRecord.getField(i).isNull()) {
					outputRow.setNull(i);
					continue;
				}
				
				Type type = tableDefinition.getColumnType(i);
				
				switch (type) {
				case UNICODE_STRING:
					// null values are handled above so CloverString -> String conversion will always succeed
					outputRow.setString(i, ((StringDataField)inputRecord.getField(i)).getValue().toString());
					break;
				case BOOLEAN:
					outputRow.setBoolean(i, ((BooleanDataField)inputRecord.getField(i)).getBoolean());
					break;
				case DATETIME: case DATE: case DURATION:
					final Date inputDate = ((DateDataField)inputRecord.getField(i)).getDate();

					DateFieldExtractor extractor = extractors[i];
					extractor.setDate(inputDate);
					
					int year = extractor.getYear();
					int month = extractor.getMonth();
					int day = extractor.getDay();
					int hour = extractor.getHour();
					int minute = extractor.getMinute();
					int second = extractor.getSecond();
					int millisecond = extractor.getMilliSecond();

					switch (type) {
					case DURATION:
						outputRow.setDuration(i, day, hour, minute, second,	millisecond);
						break;
					case DATE:
						outputRow.setDate(i, year, month, day);
						break;
					default: // DATETIME
						outputRow.setDateTime(i, year, month, day, hour, minute, second, millisecond);
					}
					break;
				case DOUBLE:
					outputRow.setDouble(i, ((NumericDataField)inputRecord.getField(i)).getDouble());
					break;
				case INTEGER:
					outputRow.setInteger(i, ((IntegerDataField)inputRecord.getField(i)).getInt());
					break;	
				default: 
					throw new ComponentNotReadyException(
							"Unable to convert value of type \"" + fieldMetadata.getDataType() 
							+ "\" into any types supported by Tableau. Offending field:  " + fieldMetadata.getName());
				}
			}
			
			targetTable.insert(outputRow);
			
		} catch (TableauException e) {
			throw new ComponentNotReadyException(
					"Unable to write input record to the output file. Offending record: \n" + this.inputRecord, e);
		}
		
	}
	
	
	private void prepareInputRecord() throws ComponentNotReadyException {
		this.inputPort = getInputPort(0);
		this.inputMetadata = inputPort.getMetadata();
		if (inputMetadata == null) {
			throw new ComponentNotReadyException("Input edge is missing metadata information");
		}
		
		this.inputRecord = DataRecordFactory.newRecord(inputMetadata);
		this.inputRecord.init();
	}
	
	
	private Extract prepareTargetFile() throws ComponentNotReadyException {
		if (outputFileName == null || outputFileName.isEmpty()) {
			throw new ComponentNotReadyException(
					"Output file path is not set. Enter valid path pointing to a local file with \".tde\" suffix");
		}

		logger.debug("Input files is configured to: \"" + outputFileName + "\"");

		File targetFile = FileUtils.getJavaFile(getContextURL(), outputFileName);

		logger.debug("Resolved target file to: \"" + targetFile + "\"");

		// Create any missing directories
		FileUtils.createParentDirs(getContextURL(), outputFileName);

		if (targetFile.exists()) {
			if (actionOnExistingFile.isOverwrite()) {
				// Tableau API throws exception if target file exists. We must delete it, there is no "replace" option
				// See docs of constructor for Extract class
				if (!targetFile.delete()) {
					throw new ComponentNotReadyException("Unable to delete output file, the file is probably locked. Output file: " + targetFile);
				}
			}
		}

		try {
			this.targetExtract = new Extract(targetFile.getCanonicalPath());
			return this.targetExtract;
		} catch (IOException | TableauException e) {
			throw new ComponentNotReadyException("Unable to open output file. Output file: " + targetFile, e);
		}

	}

	private void prepareTargetTable() throws ComponentNotReadyException {
		if (tableName == null) {
			// we validate that input edge is connected in init() method before this code gets called.
			this.tableName = "Extract";
			logger.info("Target table name is not set. Using \"Extract\" as table name.");
		}
		
		try {
			if (targetExtract.hasTable(tableName)) {
				// table exists; open it if append mode is on
				if (actionOnExistingFile.isTerminate()) {
					throw new ComponentNotReadyException("Target table exists and terminate processing option was specified. Terminating processing. Table: " + tableName);
				}
				
				this.targetTable = targetExtract.openTable(tableName);
				this.tableDefinition = targetTable.getTableDefinition();
			} else {
				// table does not exist; create new definition
				logger.info("Target table does not exist. Creating new table definition.");
				this.tableDefinition = createTableDefinition();
				this.targetExtract.addTable(tableName, tableDefinition);
				this.targetTable = targetExtract.openTable(tableName);
				printTableDefinition(this.tableName, tableDefinition);
			}
		} catch (TableauException e) {
			throw new ComponentNotReadyException("Unable to create/open target table: " + tableName,e);
		}
	}

	
	private TableDefinition createTableDefinition() throws ComponentNotReadyException {
		
		try {
			TableDefinition tableDefinition = new TableDefinition();
			tableDefinition.setDefaultCollation(this.defaultTableCollation);
			
			for (int i=0; i<inputRecord.getNumFields();i++) {
				DataFieldMetadata fieldMetadata=inputMetadata.getField(i); 
				TableauTableColumnDefinition column = mappings.get(fieldMetadata.getName());
				
				Type tableauType;
				if (column.getTableauType().equals(TableauTableStructureParser.DEFAULT_TABLEAU_TYPE_STRING)) {
					tableauType = convertToDefaultType(fieldMetadata);
				} else {
					tableauType = Type.valueOf(column.getTableauType());
				}
				
				if (column.getCollation().equals(TableauTableStructureParser.DEFAULT_COLLATION_STRING)) {
					tableDefinition.addColumn(fieldMetadata.getName(), tableauType);
				} else {
					tableDefinition.addColumnWithCollation(
							fieldMetadata.getName(),
							tableauType,
							Collation.valueOf(column.getCollation()));
				}
			}
			
			return tableDefinition;
			
		} catch (TableauException e) {
			throw new ComponentNotReadyException("Unable to create target table definition from input metadata.",e);
		}
	}
	
	
	private static Type convertToDefaultType(DataFieldMetadata fieldMeta) throws ComponentNotReadyException {
		switch (fieldMeta.getDataType()) {
		case BOOLEAN:
			return Type.BOOLEAN;
		case DATE:
			return Type.DATETIME;
		case NUMBER:
			return Type.DOUBLE;
		case INTEGER:
			return Type.INTEGER;
		case STRING:
			return Type.UNICODE_STRING;
		default:
			throw new ComponentNotReadyException("Field type " + fieldMeta.getDataType() + " cannot be converted to any Tableau type!");
		}
	}
	
	public static Type[] getCompatibleTypes(DataFieldMetadata fieldMeta) {
		switch (fieldMeta.getDataType()) {
		case BOOLEAN:
			return new Type[] {Type.BOOLEAN};
		case DATE:
			return new Type[] {Type.DATETIME, Type.DATE, Type.DURATION};
		case NUMBER:
			return new Type[] {Type.DOUBLE};
		case INTEGER:
			return new Type[] {Type.INTEGER};
		case STRING:
			return new Type[] {Type.UNICODE_STRING};
		default:
			return null;
		}
	}
	
	
	private void prepareValueConvertors() {
		DataRecordMetadata recordMeta = getInputPort(0).getMetadata();
		this.extractors = new DateFieldExtractor[recordMeta.getNumFields()];
		for (int i=0; i<recordMeta.getNumFields(); i++) {
			DataFieldMetadata fieldMeta = recordMeta.getField(i);
			if (DataFieldType.DATE == fieldMeta.getDataType() ) {
				extractors[i]= DateFieldExtractorFactory.getExtractor(fieldMeta.getTimeZoneStr());
			}
		}
	}
	
	// Log table metadata definition
    private void printTableDefinition(String tableName, TableDefinition tableDef) throws TableauException {
    	int numColumns = tableDef.getColumnCount();
    	StringBuffer strBuf = new StringBuffer();
    	strBuf.append("\n------------------- Table [" + tableName + "] -------------------\n");
        for ( int i = 0; i < numColumns; ++i ) {
            Type type = tableDef.getColumnType(i);
            String name = tableDef.getColumnName(i);

            strBuf.append(String.format("Column %3d: %-25s %-14s\n", i+1, name, Type.enumForValue(type.getValue())));
        }
        strBuf.append("-------------------------------------------------------");
   
        logger.info(strBuf);
    }

	public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
        
		String componentID = xattribs.getString(XML_ID_ATTRIBUTE);
        String targetFileName = xattribs.getStringEx(XML_OUTPUT_FILE, null, RefResFlag.URL);
        
        String targetTableName = xattribs.getStringEx(XML_TABLE_NAME, "Extract", null);
        
        String actionOnExistingFile = xattribs.getStringEx(XML_ACTION_ON_EXISTING_FILE,
				TableauActionOnExistingFile.OVERWRITE_TABLE.name(),
				null);

        String rawTableCollation = xattribs.getStringEx(XML_DEFAULT_TABLE_COLLATION, null, null);
        
        String tableStructure = xattribs.getStringEx(XML_TABLE_STRUCTURE, "", null);
        
        long timeout = xattribs.getLong(XML_TIMEOUT, 300);

        return new TableauWriter(componentID, graph,targetFileName,targetTableName,rawTableCollation,actionOnExistingFile,tableStructure,timeout);
		
	}
	
	/**
	 * Sets default collation. If status is null, prints errors to System.err
	 * @param status
	 */
	private void checkDefaultCollation(ConfigurationStatus status) {
		String errMessage = null;
		
		/* 
		 * Ugly hack by mtomcanyi: Tableau Java libraries require native libraries to be on PATH. 
		 * Tableau does not currently (June 2014) provide libraries for Mac OS X so the code below will always fail on Mac.
		 * Additionally if user does not configure libraries on Win or Linux the call to Collation.valueOf() fails due to unsatisfied linking error. 
		 * The try/catch below is therefore our best effort to avoid crashing and return helpful error. 
		 */
		try {
	        if (rawTableCollation == null) {
	        	// The calls to Collation.valueOf() needs to be hard-coded here. Do not put it in a class constant (the class wouldn't initialize)
	        	this.defaultTableCollation = Collation.valueOf(TableauTableStructureParser.DEFAULT_COLLATION);
	        } else {
	        	try {
	        		defaultTableCollation = Collation.valueOf(rawTableCollation);
	        	} catch (IllegalArgumentException e) {
	        		errMessage = "Illegal value for default table collation: " + rawTableCollation;
	        	}
	        }
		} catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
			if (System.getProperty("os.name").startsWith("Mac")) {
				errMessage = "The " + getClass().getSimpleName() + "does not work on Mac OS X as Tableau does not provide libraries for Mac.";
			} else {
				errMessage = "Unable to initialize Tableau native libraries. Make sure they are installed and configured in PATH environment variable (see component docs). Underlying error: \n" + e.getMessage();
			}
		}

		if (errMessage != null) {
			if (status != null) {
				status.add(new ConfigurationProblem(errMessage, Severity.ERROR,	this, Priority.NORMAL));
			} else {
				System.err.println(errMessage);
			}
		}
	}
	
	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		status = super.checkConfig(status);
		
		checkDefaultCollation(status);
		
		// Tableau API requires that the target file ends with ".tde". See Extract constructor doc
		if (!outputFileName.endsWith(REQUIRED_FILE_SUFFIX)) {
			status.add(new ConfigurationProblem("Output file path must point to a file with \".tde\" suffix", Severity.ERROR, this, Priority.NORMAL));
		}
		
		for (Node n : getGraph().getPhase(getPhaseNum()).getNodes().values()) {
			if (n != this && getType().equals(n.getType())) {
				//TODO this is the hard check, do we want it?
				//status.add("\""	+ n.getName() + "\" writes in the same phase. Only one TableauWriter is allowed per phase!", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
				
				//soft check
				try {
					URL contextURL = getContextURL();
					URL url1 = FileUtils.getFileURL(contextURL, ((TableauWriter) n).outputFileName);
					URL url2 = FileUtils.getFileURL(contextURL, outputFileName);
					if (url1.equals(url2)) {
						status.add("\"" + n.getName() + "\" (ID: " + n.getId() + ") writes to the same file in the same phase!", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
					}
				} catch (MalformedURLException e) {
				}
			}
		}
		
		try {
			TableauActionOnExistingFile.valueOf(actionOnExistingFileRaw);
		} catch (Exception e) {
			status.add(new ConfigurationProblem(
					"Action on existing output file is not set properly!",
					Severity.ERROR, this, Priority.NORMAL));
		}
		
		DataRecordMetadata recordMeta = getInputPort(0).getMetadata();
		for (int i=0; i<recordMeta.getNumFields(); i++) {
			DataFieldMetadata fieldMeta = recordMeta.getField(i);
			DataFieldType fieldType= fieldMeta.getDataType();
			if (fieldType == DataFieldType.LONG || fieldType == DataFieldType.DECIMAL ) {
				status.add("Input metadata of \"" + getName() + "\" contain data type unsupported by Tableau! Metadata field "
						+ recordMeta.getField(i).getName() + " of metadata " + recordMeta.getName() + " has type " + fieldType.getName()
						+ "! Unsupported types are: " + DataFieldType.LONG.getName() + ", " 
						+ DataFieldType.DECIMAL.getName(), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
			}
			if (fieldMeta.getContainerType() != DataFieldContainerType.SINGLE) {
				status.add("Input metadata of \"" + getName() + "\" have container unsupported by Tableau! Metadata field "
						+ recordMeta.getField(i).getName() + " of metadata " + recordMeta.getName() + " has container " 
						+ fieldMeta.getContainerType().getDisplayName() +"! Container types " 
						+ DataFieldContainerType.MAP.getDisplayName() + " and " + DataFieldContainerType.LIST.getDisplayName() 
						+ " are not supported.", ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
			}
		}

		return status;
		
	}
	
	public static enum TableauActionOnExistingFile {

		OVERWRITE_TABLE,
		APPEND_TO_TABLE,
		TERMINATE_PROCESSING;
        
        public boolean isOverwrite() {
        	return (this == OVERWRITE_TABLE);
        }
        
        public boolean isAppend() {
        	return (this == APPEND_TO_TABLE);
        }
        
        public boolean isTerminate() {
        	return (this == TERMINATE_PROCESSING);
        }
    }
	
}
