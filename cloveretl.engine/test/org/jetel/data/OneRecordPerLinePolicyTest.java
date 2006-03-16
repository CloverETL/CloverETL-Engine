/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on May 6, 2003
 *  Copyright (C) 2003, 2002  David Pavlis, Wes Maciorowski
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */

package test.org.jetel.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.channels.Channels;
import org.jetel.data.DataRecord;
import org.jetel.data.formatter.FixLenDataFormatter;
import org.jetel.data.parser.SQLDataParser;
import org.jetel.data.Defaults;

import org.jetel.database.DBConnection;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

import junit.framework.TestCase;

/**
 * @author maciorowski
 *
 */
public class OneRecordPerLinePolicyTest  extends TestCase {
private SQLDataParser aParser2 = null;
private FixLenDataFormatter aFixLenDataFormatter= null;
private DataRecord record;
private String testFile1 = null;	

protected void setUp() { 
	DataRecordMetadata metadata = null;
	DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
	DBConnection aDBConnection = null;

	testFile1 = "data\\out\\test1.txt";	
	File aFile=new File(testFile1);
	 if(!aFile.exists()) {
		new File(aFile.getParent()).mkdirs();
		try {
			aFile.createNewFile();
		} catch (IOException e3) {
			e3.printStackTrace();
		}
	 }
				
	try {
		metadata = xmlReader.read(new FileInputStream("config\\test\\rec_def\\db_null_def_rec.xml"));
		aDBConnection = new DBConnection("config\\test\\msaccess.clover_test.txt");
	} catch(FileNotFoundException e){
		e.printStackTrace();
	}

	record = new DataRecord(metadata);
	record.init();
	
	aParser2 = new SQLDataParser("connection","SELECT * FROM bad");
	try {
		aParser2.open(aDBConnection,metadata);
		aParser2.initSQLDataMap(record);
	} catch (ComponentNotReadyException e1) {
		e1.printStackTrace();
	}
	aFixLenDataFormatter = new FixLenDataFormatter();
	try {
		aFixLenDataFormatter.open(new FileOutputStream(testFile1),metadata);
	} catch (FileNotFoundException e2) {
		e2.printStackTrace();
	}
}
	
   protected void tearDown() {
	   aParser2.close();
	   aParser2 = null;
	   record  = null;
		aFixLenDataFormatter = null;
	   //remove testFile if any
	   File aFile=new File(testFile1);
		if(aFile.exists()) {
			aFile.delete();
		}
   }

public void test_FixLenDataFormatter_false() {
	int recCount = 0;
	try{
		while((record=aParser2.getNext(record))!=null){
			aFixLenDataFormatter.write(record);
		}
		aFixLenDataFormatter.flush();
		aFixLenDataFormatter.close();
	} catch (BadDataFormatException e){	
		fail("Should not raise an BadDataFormatException");
		e.printStackTrace();
	} catch (Exception ee){
		fail("Should not throw Exception");
		ee.printStackTrace();
	}
	
	try {
		FileInputStream fis = new FileInputStream(testFile1);
		BufferedReader reader = null;
		reader = new BufferedReader(Channels.newReader( (fis).getChannel(), Defaults.DataParser.DEFAULT_CHARSET_DECODER ) );

		String line = null;
		line = reader.readLine();
		while(line != null ) {	//skip blank lines
			recCount++;
			line = reader.readLine();
		}

		if((line) == null) {  //end of file been reached
			reader.close();
		}

	} catch (FileNotFoundException e1) {
		fail("Should not throw Exception - missing test data file");
		e1.printStackTrace();
	} catch (IOException e) {
		fail("Should not throw Exception - IOException");
		e.printStackTrace();
	}
	//should have only one line
   assertEquals(1,recCount);
}

public void test_FixLenDataFormatter_true() {
	int recCount = 0;
	
	aFixLenDataFormatter.setOneRecordPerLinePolicy(true);
	try{
		while((record=aParser2.getNext(record))!=null){
			aFixLenDataFormatter.write(record);
		}
		aFixLenDataFormatter.flush();
		aFixLenDataFormatter.close();
	} catch (BadDataFormatException e){	
		fail("Should not raise an BadDataFormatException");
		e.printStackTrace();
	} catch (Exception ee){
		fail("Should not throw Exception");
		ee.printStackTrace();
	}
	
	try {
		FileInputStream fis = new FileInputStream(testFile1);
		BufferedReader reader = null;
		reader = new BufferedReader(Channels.newReader( (fis).getChannel(), Defaults.DataParser.DEFAULT_CHARSET_DECODER ) );

		String line = null;
		line = reader.readLine();
		while(line != null ) {	//skip blank lines
			recCount++;
			line = reader.readLine();
		}

		if((line) == null) {  //end of file been reached
			reader.close();
		}

	} catch (FileNotFoundException e1) {
		fail("Should not throw Exception - missing test data file");
		e1.printStackTrace();
	} catch (IOException e) {
		fail("Should not throw Exception - IOException");
		e.printStackTrace();
	}
	//should have 3 lines; one line for each record
   assertEquals(3,recCount);
}
	

}
