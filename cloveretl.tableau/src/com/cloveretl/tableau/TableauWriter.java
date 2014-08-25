package com.cloveretl.tableau;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.BooleanDataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.DateDataField;
import org.jetel.data.DecimalDataField;
import org.jetel.data.IntegerDataField;
import org.jetel.data.LongDataField;
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
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.date.DateFieldExtractor;
import org.jetel.util.date.DateFieldExtractorFactory;
import org.jetel.util.file.FileUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

import com.tableausoftware.TableauException;
import com.tableausoftware.DataExtract.Collation;
import com.tableausoftware.DataExtract.Extract;
import com.tableausoftware.DataExtract.Row;
import com.tableausoftware.DataExtract.Table;
import com.tableausoftware.DataExtract.TableDefinition;
import com.tableausoftware.DataExtract.Type;


public class TableauWriter extends Node  {

	//TODO put this in messages properties file
	private final static String INVALID_SUFFIX_MESSAGE = "Output file path must point to a file with \".tde\" suffix";

	public final static String TABLEAU_WRITER = "TABLEAU_WRITER";

	public static final String XML_OUTPUT_FILE = "outputFile"; 
	
	
	/* 
	 * Note mtomcanyi:
	 * 	
	 * Tableau API seems to accept table name but throws exception when table name is anything else than "Extract".
	 * Component code is ready accept table name from user, so when Tableau API start accepting it we can just uncomment our code
	 * in fromXML() and customcomponents.xml and make table name configurable.
	 */
	public static final String XML_TABLE_NAME = "tableName";
	
	
	public static final String XML_DEFAULT_TABLE_COLLATION = "defaultTableCollation";
	public static final String XML_APPEND_TO_TABLE = "appendToTable";
	public static final String XML_OVERWRITE_OUTPUT_FILE = "overwrite";

	// output file suffix required by Tableau
	private static final String REQUIRED_FILE_SUFFIX = ".tde";
	
	// Attributes initialized from XML configuration
	
	/* final */ String outputFileName;
	/* final */ String tableName;
	/* final */ boolean appendToTableFlag;
	/* final */ boolean overwriteTargetFileFlag;
	/* final */ private String rawTableCollation;
	/* final */ Collation defaultTableCollation;

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
	
	
	public TableauWriter(String id, TransformationGraph graph, String outputFileName, String tableName, String rawTableCollation, boolean overwriteFileFlag, boolean appendToTableFlag) {
		super(id, graph);
		this.outputFileName = outputFileName;
		this.tableName = tableName;
		this.rawTableCollation= rawTableCollation;
		this.overwriteTargetFileFlag = overwriteFileFlag;
		this.appendToTableFlag = appendToTableFlag;
	}
	
	
	
	@Override
	public void init() throws ComponentNotReadyException {
		super.init();
		
		prepareInputRecord();
		
		if (defaultTableCollation == null) {
			checkDefaultCollation(null);
		}
	}
	
	@Override
	protected Result execute() throws Exception {
		// check and prepare target file
		try (Extract targetFile = prepareTargetFile()) {
			prepareTargetTable();
			prepareValueConvertors();
			
			while (readRecord() && runIt) {
				writeRecord();
			}
			
			targetFile.close();
			
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
				
				switch (fieldMetadata.getDataType()) {
				case STRING:
					// null values are handled above so CloverString -> String conversion will always succeed
					outputRow.setString(i, ((StringDataField)inputRecord.getField(i)).getValue().toString());
					break;
				case BOOLEAN:
					outputRow.setBoolean(i, ((BooleanDataField)inputRecord.getField(i)).getBoolean());
					break;
				case DATE:
					final Date inputDate = ((DateDataField)inputRecord.getField(i)).getDate();

					DateFieldExtractor extractor = extractors[i];
					extractor.setDate(inputDate);

					// setDateTime(int columnNumber, int year, int month, int day, int hour, int min, int sec, int frac)
					outputRow.setDateTime(i, extractor.getYear(), extractor.getMonth(), extractor.getDay(), extractor.getHour(),
							extractor.getMinute(),extractor.getSecond(),extractor.getMilliSecond()
					);
					break;
				
				case NUMBER:
					outputRow.setDouble(i, ((NumericDataField)inputRecord.getField(i)).getDouble());
					break;

				case DECIMAL:
					// FIXME test what happens on overflowing value
					outputRow.setDouble(i, ((DecimalDataField)inputRecord.getField(i)).getDouble());
					break;
					
				case INTEGER:
					outputRow.setInteger(i, ((IntegerDataField)inputRecord.getField(i)).getInt());
					break;
				case LONG:
					outputRow.setInteger(i, ((LongDataField)inputRecord.getField(i)).getInt());
					break;
				default: 
					throw new ComponentNotReadyException("Unable to convert value of type \"" + fieldMetadata.getDataType() + "\" into any types supported by Tableau. Offending field:  " + fieldMetadata.getName());
				}
			}
			
			targetTable.insert(outputRow);
			
		} catch (TableauException e) {
			throw new ComponentNotReadyException("Unable to write input record to the output file. Offending record: \n" + this.inputRecord, e);
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
		try {

			if (outputFileName == null || outputFileName.isEmpty()) {
				throw new ComponentNotReadyException("Output file path is not set. Enter valid path pointing to a local file with \".tde\" suffix");
			}

			logger.debug("Input files is configured to: \"" +  outputFileName + "\"");
			
			File targetFile = FileUtils.getJavaFile(getContextURL(), outputFileName);

			logger.debug("Resolved target file to: \"" +  targetFile + "\"");
			
			// Create any missing directories
			//FIXME FileUtils.makeDirs() fails on Server while it works fine in Designer .... 
			FileUtils.createParentDirs(getContextURL(), outputFileName);
			
			if (targetFile.exists()) {
				if (overwriteTargetFileFlag) {
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
			} catch (TableauException e) {
				throw new ComponentNotReadyException("Unable to open output file. Output file: " + targetFile,e);
			}
			
			
		} catch (Exception e) {
			throw new ComponentNotReadyException("Path to output file is invalid: '" + outputFileName + "'",e);
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
				if (!appendToTableFlag) {
					throw new ComponentNotReadyException("Target table exists and append mode is disabled. Terminating processing. Table: " + tableName);
				}
				
				this.targetTable = targetExtract.openTable(tableName);
				this.tableDefinition = targetTable.getTableDefinition();
			} else {
				// table does not exist; create new definition
				logger.info("Target table does not exist. Creating new table definition from input metadata.");
				this.tableDefinition = createTableDefinitionFromMetadata();
				this.targetExtract.addTable(tableName, tableDefinition);
				this.targetTable = targetExtract.openTable(tableName);
				printTableDefinition(this.tableName, tableDefinition);
				
				
			}
		} catch (TableauException e) {
			throw new ComponentNotReadyException("Unable to create/open target table: " + tableName,e);
		}
		
		
	}

	
	private TableDefinition createTableDefinitionFromMetadata() throws ComponentNotReadyException {
		
		try {
			TableDefinition tableDefinition = new TableDefinition();
			tableDefinition.setDefaultCollation(this.defaultTableCollation);
			
			for (int i=0; i<inputMetadata.getNumFields(); i++) {
				DataFieldMetadata fieldMeta =  inputMetadata.getField(i);
				// FIXME column collation for string columns ... must be done from component configuration
				Type fieldType = convertType(fieldMeta);
				tableDefinition.addColumn(fieldMeta.getName(), fieldType);
			}
			
			return tableDefinition;
			
		} catch (TableauException e) {
			throw new ComponentNotReadyException("Unable to create target table definition from input metadata.",e);
		}
		
		
	}
	
	
	private Type convertType(DataFieldMetadata fieldMeta) throws ComponentNotReadyException {
		switch (fieldMeta.getDataType()) {
		case BOOLEAN:
			return Type.BOOLEAN;
		case DATE:
			return Type.DATETIME;
		case DECIMAL:
		case NUMBER:
			return Type.DOUBLE;
		case INTEGER:
		case LONG:
			return Type.INTEGER;
		case STRING:
			return Type.UNICODE_STRING;
		default: throw new ComponentNotReadyException("The field type \"" + fieldMeta.getDataType() + "\" cannot be converted to any known Tableau type");
		}
	}
	
	
	private void prepareValueConvertors() {
		// Hahaha I'm using array after all these years!!! Incredible! 
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

		// FIXME instrukcie na instalaciu. jna.library.path nefunguje, musime tdserver64.exe musi byt na PATH. Uzivatel bude musiet nastavit sam. Mac nepodporujeme
		
		
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
        
		String componentID = xattribs.getString(XML_ID_ATTRIBUTE);
        String targetFileName = xattribs.getStringEx(XML_OUTPUT_FILE, null, RefResFlag.URL);
        boolean overwriteFileFlag = xattribs.getBoolean(XML_OVERWRITE_OUTPUT_FILE, true);
        
        String targetTableName = xattribs.getStringEx(XML_TABLE_NAME, "Extract", null);
        boolean appendToTableFlag = xattribs.getBoolean(XML_APPEND_TO_TABLE, true);
        
        String rawTableCollation = xattribs.getStringEx(XML_DEFAULT_TABLE_COLLATION, null, null);
        

        return new TableauWriter(componentID, graph,targetFileName,targetTableName,rawTableCollation,overwriteFileFlag,appendToTableFlag);
		
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
	        	this.defaultTableCollation= Collation.EN_US;
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
				errMessage = "Unable to initialize Tableu native libraries. Make sure they are installed and configured in PATH environment variable (see component docs). Underlying error: \n" + e.getMessage();
			}
		}

		if (errMessage != null) {
			if (status != null) {
				status.add(new ConfigurationProblem(errMessage, Severity.ERROR,
						this, Priority.NORMAL));
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
			status.add(new ConfigurationProblem(INVALID_SUFFIX_MESSAGE, Severity.ERROR, this, Priority.NORMAL));
		}
		
		return status;
		
	}
	
	//FIXME DataExtract.log log file of the API ... mention in documentation
	// FIXME !!!! Tableau nie je thread safe !!!!
	
	//
	// getType() nemusi byt
	// zabudol som pridat ID komponenty
	//
	// Unclear sections: 
	// Used mongoDBWriter ... what is a good "modern" component to start with?
	// No good component as base for Writer
	// Wizard ... setup component project, container/library setup
//	    -> Screenshot: add library -> engine
	// File utils ... URL conversion, native file path conversion
	// Get field type z meta ... ako? 
	// ExtString, expansion flags
	// Co v init/preExec
	// Alokacia recordu cez factory
	// Exceptions ... ComponentNotReady? Ina?
	// How to say I don't care for engine version in plugin.xml (0.0.0.devel). Does it mean MINIMUM version?
	
	// GUI ... Designer does not contain Eclipse SDK, so wizards do not show up. What should I install?
	//  --> Eclipse SDK from Eclipse update site 3.7
	//
	// XMLSchema for the component descriptor. Not dtd! For download from docs.
	
	
	/*
	 * After com.cloveretl.gui.component has been added, set the file property to the customcomponents.xml component definition file, and the classLoaderProvider field to the org.company.gui.CLProvider class name
	 * -> CLPROVIDER WTF?!!!
	 * -> The class does not exist: org.cloveretl.gui.plugin.engine.AbstractClassLoaderProvider
	 * -> it is com.cloveretl...
	 * 
	 * "Import requisities" ... requirements
	 * -> Chyba
	 * 
	 * AKO TEN PLUGIN AKTIVUJEM V DESIGNERI?!!!
	 *
	 *Dependencies on jface .. Import packages
	 * UIEnumPropertyToolkit ... miesto AbstractToolkit
	 * 
	 * GUI zavisi na Tableau kode ... ako pridam zavislosti do GUI pluginu koli editoru Collation?
	 * 
	 * Writers ... musi byt s malymi pismenami v customcomponents.xml
	 * 
	 * Ako debugovat
	 *  - eclipsec.exe
	 *  - Run as, Debug as + increase permgen
	 * 
	 * Mam problem ladit engine komponentu .. kde su pluginy?
	 * 
	 */
	
	
}
