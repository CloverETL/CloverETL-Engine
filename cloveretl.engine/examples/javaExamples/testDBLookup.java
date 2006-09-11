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

package javaExamples;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.jetel.component.ComponentFactory;
import org.jetel.connection.DBConnection;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.lookup.DBLookupTable;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

public class testDBLookup{

	public static void main(String args[]){
	DBConnection dbCon;
	
    //initialization; must be present
    Defaults.init();
    ComponentFactory.init();

	if (args.length!=4){
		System.out.println("Usage: testDBLookup <driver properties file> <sql query> <key> <db metadata file>");
		System.exit(1);
	}
	System.out.println("**************** Input parameters: ****************");
	System.out.println("Driver propeties: "+args[0]);
	System.out.println("SQL query: "+args[1]);
	System.out.println("Key: "+args[2]);
	System.out.println("Metadata file: "+args[3]);
	System.out.println("***************************************************");
	
	DataRecordMetadata metadataIn;
	DataRecord data;
	
	DataRecordMetadataXMLReaderWriter metaReader=new DataRecordMetadataXMLReaderWriter();
	try{
	    metadataIn=metaReader.read(new FileInputStream(args[3]));
	}catch(FileNotFoundException ex){
	    throw new RuntimeException(ex.getMessage());
	}
	
	//create connection object. Get driver and connect string from cfg file specified as a first argument
	dbCon=new DBConnection("Conn0",args[0]);
	try{
		
		// create lookup table. Will use previously created connection. The query string
		// is specified as a second parameter
		// query string should contain ? (questionmark) in where clause
		// e.g. select * from customers where customer_id = ? and customer_city= ?
		DBLookupTable lookup=new DBLookupTable("lookup",dbCon,metadataIn,args[1]);
		
		/*
		* in case the DB doesn't support getMetadata, use following constructor:
		* (don't forget to create metadata object first. For example by analyzing DB
		* first and then using DataRecordMetadataXMLReaderWriter
		
		 DBLookupTable lookup=new DBLookupTable(dbCon,metadataIn,args[1]);
		
		
		*/
		
		// we initialize lookup
		lookup.init();
		//try to lookup based on specified parameter
		//following version of get() method is valid for queries with one parameter only
		//in case you have more (as with the example shown above), use array of objects (strings, integers, etc.) and
		//call get(Object[])
		
		
		data=lookup.get(args[2]);
		
		//in case query returns more than one record, continue displaying it.
		while(data!=null){
			System.out.println(data);
			data=lookup.getNext();
		}
		//System.out.println(lookup.get(args[4]));
		
	}catch(Exception ex){
		ex.printStackTrace();
	}
	}
} 


