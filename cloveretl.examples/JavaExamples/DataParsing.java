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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.DataParser;
import org.jetel.data.parser.Parser;
import org.jetel.data.parser.TextParserFactory;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * Usage: DataParsing [-plugins pluginsDirectory] [-config propertiesFile]
 * 
 * This example illustrates usage of DataParser - the object for parsing plain text data.
 * Data from data/bonus.csv is parsed and saved in output/bonus.out
 * All examples require to provide CloverETL plugins. Plugins directory can be set as program argument or 
 * in params.txt file as plugins parameter or required plugins have to be set on classpath when running the program. 
 * When set in params.txt, the same directory is used for all examples and musn't be set 
 * for each example separately.
 *
 */
public class DataParsing {
	
	private final static String PARAMETER_FILE = "params.txt"; 
	private final static String PLUGINS_PROPERTY = "plugins";
	private final static String PROPERTIES_FILE_PROPERTY = "config";

	public static void main(String args[]){
		
	String plugins = null;
	String propertiesFile = null;
	
	//read parameters from parameter file
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
	plugins = arguments.getProperty(PLUGINS_PROPERTY);
	propertiesFile = arguments.getProperty(PROPERTIES_FILE_PROPERTY);
	//override parameters with program arguments
	for (int i = 0; i < args.length; i++) {
		if (args[i].startsWith("-" + PLUGINS_PROPERTY)) {
			plugins = args[++i];
		}else if (args[i].startsWith("-" + PROPERTIES_FILE_PROPERTY)) {
			propertiesFile = args[++i];
		}
	}
	
	FileInputStream in=null;
	PrintStream out=null;
	DataRecord record;
	
	System.out.println("**************** Input parameters: ****************");
	System.out.println("Plugins directory: "+ plugins);
	System.out.println("Default properties file: "+ propertiesFile);
	System.out.println("***************************************************");
	
	//Initialization of CloverETL engine
	EngineInitializer.initEngine(plugins, propertiesFile, null);
	
	//preparing input and output files
	try{
		in=new FileInputStream("data/bonus.csv");
		out=new PrintStream(new FileOutputStream("output/bonus.out"));
	}
	catch(FileNotFoundException e){
		e.printStackTrace();
		System.exit(1);
	}
	
	//Creating metadata
	DataRecordMetadata metadata=new DataRecordMetadata("TestInput",DataRecordMetadata.DELIMITED_RECORD);
	
	metadata.addField(new DataFieldMetadata("Client_id",DataFieldMetadata.INTEGER_FIELD, ";"));
	DataFieldMetadata numericField = new DataFieldMetadata("Revenue",DataFieldMetadata.NUMERIC_FIELD, ";");
	//locale must be set to a location, that uses comma as decimal point
	numericField.setLocaleStr("pl");
	metadata.addField(numericField);
	metadata.addField(new DataFieldMetadata("Contract_nr",DataFieldMetadata.INTEGER_FIELD, "\r\n"));
	
	//initialize parser
	Parser parser = TextParserFactory.getParser(metadata);
	try{
		parser.init();
		parser.setDataSource(in);
	}catch(Exception ex){
		ex.printStackTrace();
		System.exit(1);
	}
	//prepare data record for parsed data
	record = new DataRecord(metadata);
	record.init();
	
	//parse file in loop
	try {
		while((record=parser.getNext(record))!=null){
			out.print("Client:"+record.getField(0).toString());
			out.print(" Revenue:"+record.getField(1).toString());
			out.println(" Contract:"+record.getField(2).toString());
		}
		
		System.out.println(((DataParser) parser).getRecordCount() + " records parsed. See file ./output/bonus.out");
		parser.close();
	} catch (Exception e1) {
		e1.printStackTrace();
	}
	
	}
	
} 


