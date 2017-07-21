/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002  David Pavlis
*
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.jetel.connection.jdbc.DBConnection;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.lookup.DBLookupTable;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

/**
 * Program parameters:
 * -plugins	pluginsDirectory		CloverETL plugins directory
 * -config propertiesFile			load default engine properties from specified file
 * -connection 						database connection configuration file
 * -query							lookup query
 * -key								lookup key
 * -metadataFile					metadata definition file
 * 
 * This example illustrates usage of LookupTable, in particular database lookup table (DBLookupTable).
 * It looks up in derby database for the records with requested key.
 * All examples require to provide CloverETL plugins. Plugins directory can be set as program argument or 
 * in params.txt file as plugins parameter or required plugins have to be set on classpath when running the program. 
 * When set in params.txt, the same directory is used for all examples and musn't be set 
 * for each example separately.
 * This examples requires some additional parameters: connection file, sql query, key and metadata. 
 * All the properties are read from params.txt, if they are not set as program arguments.
 * This example requires cloveretl.connection.jar and cloveretl.lookup.jar set on classpath.
 */
public class DatabaseLookup{

	//requested parameters
	private final static String PARAMETER_FILE = "params.txt"; 
	private final static String PLUGINS_PROPERTY = "plugins";
	private final static String PROPERTIES_FILE_PROPERTY = "config";
	private final static String CONNECTION_PROPERTY = "connection";
	private final static String QUERY_PROPERTY = "query";
	private final static String KEY_PROPERTY = "key";
	private final static String METADATA_PROPERTY = "metadataFile"; 
	
	public static void main(String args[]){
	DBConnection dbCon;
	
	//reading parameters from params.txt file
	Properties arguments = new Properties();
	if ((new File(PARAMETER_FILE)).exists()) {
		try {
			arguments.load(new FileInputStream(PARAMETER_FILE));
		} catch (FileNotFoundException e) {
			//do nothing: we checked it
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	//overriding requested parameters from program parameters 
	for (int i = 0; i < args.length; i++) {
		if (args[i].startsWith("-" + PLUGINS_PROPERTY)) {
			arguments.setProperty(PLUGINS_PROPERTY, args[++i]);
		}else if (args[i].startsWith("-" +PROPERTIES_FILE_PROPERTY)){
			arguments.setProperty(PROPERTIES_FILE_PROPERTY, args[++i]);
		}else if (args[i].startsWith("-" + CONNECTION_PROPERTY)){
			arguments.setProperty(CONNECTION_PROPERTY, args[++i]);
		}else if (args[i].startsWith("-" + QUERY_PROPERTY)){
			arguments.setProperty(QUERY_PROPERTY, args[++i]);
		}else if (args[i].startsWith("-" + KEY_PROPERTY)){
			arguments.setProperty(KEY_PROPERTY, args[++i]);
		}else if (args[i].startsWith("-" + METADATA_PROPERTY)){
			arguments.setProperty(METADATA_PROPERTY, args[++i]);
		}
	}
	
	boolean missingProperty = false;
	if (!arguments.containsKey(CONNECTION_PROPERTY)){
		missingProperty = true;
		System.out.println(CONNECTION_PROPERTY + " property not found");
	}
	if (!arguments.containsKey(QUERY_PROPERTY)){
		missingProperty = true;
		System.out.println(QUERY_PROPERTY + " property not found");
	}
	if (!arguments.containsKey(KEY_PROPERTY)){
		missingProperty = true;
		System.out.println(KEY_PROPERTY + " property not found");
	}
	if (missingProperty) {
		System.exit(1);
	}
	
	System.out.println("**************** Input parameters: ****************");
	System.out.println("Plugins directory: "+ arguments.getProperty(PLUGINS_PROPERTY));
	System.out.println("Properties file: "+ arguments.getProperty(PROPERTIES_FILE_PROPERTY));
	System.out.println("Connection propeties: "+ arguments.getProperty(CONNECTION_PROPERTY));
	System.out.println("SQL query: "+ arguments.getProperty(QUERY_PROPERTY));
	System.out.println("Key: "+ arguments.getProperty(KEY_PROPERTY));
	System.out.println("Metadata file: " + arguments.getProperty(METADATA_PROPERTY));
	System.out.println("***************************************************");
	
	//initialization; must be present
	EngineInitializer.initEngine(arguments.getProperty(PLUGINS_PROPERTY), arguments.getProperty(PROPERTIES_FILE_PROPERTY), null);
	EngineInitializer.forceActivateAllPlugins();

	//reading metadata from fmt file
	DataRecordMetadata metadataIn = null;	
	if (arguments.containsKey(METADATA_PROPERTY)) {
		DataRecordMetadataXMLReaderWriter metaReader = new DataRecordMetadataXMLReaderWriter();
		try {
			metadataIn = metaReader.read(new FileInputStream(arguments.getProperty(METADATA_PROPERTY)));
		} catch (FileNotFoundException ex) {
			throw new RuntimeException(ex.getMessage());
		}
	}
	//create connection object. Get driver and connect string from cfg file specified as a first argument
	dbCon=new DBConnection("Conn0",arguments.getProperty(CONNECTION_PROPERTY));
	try{
		dbCon.init();
		
		// create lookup table. Will use previously created connection. The query string
		// is specified as a parameter
		// query string should contain ? (questionmark) in where clause
		// e.g. select * from customers where customer_id = ? and customer_city= ?
		LookupTable lookupTable=new DBLookupTable("lookup",dbCon,metadataIn,arguments.getProperty(QUERY_PROPERTY));
				
		// we initialize lookup table
		lookupTable.init();
		lookupTable.preExecute();
		
		//creating data record for seeking
		DataRecordMetadata keyMetadata = new DataRecordMetadata("db_key_metadata", DataRecordMetadata.DELIMITED_RECORD);
		keyMetadata.addField(new DataFieldMetadata("department_id", DataFieldMetadata.INTEGER_FIELD, ";"));
		
		DataRecord keyRecord = new DataRecord(keyMetadata);
		keyRecord.init();
		RecordKey key = new RecordKey(keyMetadata.getFieldNamesArray(), keyMetadata);
		key.init();
		
		//create lookup query based on requested key
		Lookup lookup = lookupTable.createLookup(key, keyRecord);
		
		String[] keyValue = arguments.getProperty(KEY_PROPERTY).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		for (int i = 0; i < keyValue.length; i++) {
			keyRecord.getField(i).fromString(keyValue[i]);
		}
		
		//try to lookup based on specified parameter
		lookup.seek();

		//display results, if there are any
		while(lookup.hasNext()){
			System.out.println(lookup.next());
		}
		
		//free lookup table
		lookupTable.postExecute();
		lookupTable.free();
		
	}catch(Exception ex){
		ex.printStackTrace();
	}
	}
} 


