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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.runtime.EngineInitializer;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

public class testDataParsing {

	public static void main(String args[]){
		
	FileInputStream in=null;
	PrintStream out=null;
	DataRecord record;
	
	EngineInitializer.initEngine(args.length > 0 ? args[0] : null, args.length > 1 ? args[1] : null, null);
	System.out.println("**************** Input parameters: ****************");
	System.out.println("Plugins directory: "+ (args.length > 0 ? args[0] : " default"));
	System.out.println("Default properties file: "+ (args.length > 1 ? args[1] : " default"));
	System.out.println("***************************************************");
	
	try{
		in=new FileInputStream("data/delimited/bonus.csv");
		out=new PrintStream(new FileOutputStream("output/bonus.out"));
	}
	catch(FileNotFoundException e){
		e.printStackTrace();
	}
	
	DataRecordMetadata metadata=new DataRecordMetadata("TestInput",DataRecordMetadata.DELIMITED_RECORD);
	
	metadata.addField(new DataFieldMetadata("Client_id",DataFieldMetadata.INTEGER_FIELD, ";"));
	DataFieldMetadata numericField = new DataFieldMetadata("Revenue",DataFieldMetadata.NUMERIC_FIELD, ";");
	numericField.setLocaleStr("pl");
	metadata.addField(numericField);
	metadata.addField(new DataFieldMetadata("Contract_nr",DataFieldMetadata.INTEGER_FIELD, "\r\n"));
	
	DelimitedDataParser parser=new DelimitedDataParser();
	try{
		parser.init(metadata);
		parser.setDataSource(in);
	}catch(ComponentNotReadyException ex){
		ex.printStackTrace();
	}
	record = new DataRecord(metadata);
	record.init();
	
	try {
		while((record=parser.getNext(record))!=null){
			out.print("Client:"+record.getField(0).toString());
			out.print(" Revenue:"+record.getField(1).toString());
			out.println(" Contract:"+record.getField(2).toString());
		}
		
		System.out.println("Parsing succecsfull. See file ./output/bonus.out");
	} catch (JetelException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	
	}
	
} 


