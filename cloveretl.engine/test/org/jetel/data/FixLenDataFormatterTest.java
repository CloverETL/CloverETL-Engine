/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on May 7, 2003
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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.formatter.FixLenDataFormatter;
import org.jetel.data.parser.FixLenDataParser2;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.BadDataFormatExceptionHandlerFactory;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

import junit.framework.TestCase;

/**
 * @author maciorowski
 *
 */
public class FixLenDataFormatterTest extends TestCase {
private FixLenDataParser2 aParser = null;
private FixLenDataParser2 aParser2 = null;
private FixLenDataParser2 aParser3 = null;
private FixLenDataParser2 testParser = null;
private FixLenDataFormatter aFixLenDataFormatter= null;
private DataRecord record;
private String testFile1 = null;	
private DataRecordMetadata metadata = null;
	
protected void setUp() { 
	FileInputStream in = null;
	FileInputStream in2 = null;
	FileInputStream in3 = null;
	metadata = null;
	DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
			
	try {
		metadata = xmlReader.read(new FileInputStream("config\\test\\rec_def\\FL28_null_def_rec.xml"));
		in = new FileInputStream("data\\in\\good\\FL28_no_NL.txt");
		in2 = new FileInputStream("data\\in\\bad\\FL28_no_NL_nulls.txt");
		in3 = new FileInputStream("data\\in\\bad\\FL28_NL_nulls.txt");
	} catch(FileNotFoundException e){
		e.printStackTrace();
	}

    // we are going to write our test data here
	testFile1 = "data\\out\\test1.txt";	
	File aFile=new File(testFile1);
	 if(!aFile.exists()) {
		new File(aFile.getParent()).mkdir();
		try {
			aFile.createNewFile();
		} catch (IOException e3) {
			e3.printStackTrace();
		}
	 }
	aFixLenDataFormatter = new FixLenDataFormatter();
	try {
		aFixLenDataFormatter.open(new FileOutputStream(testFile1),metadata);
	} catch (FileNotFoundException e2) {
		e2.printStackTrace();
	}
	
	aParser = new FixLenDataParser2();
	aParser2 = new FixLenDataParser2();
	aParser3 = new FixLenDataParser2();
	testParser = new FixLenDataParser2();
	
	BadDataFormatExceptionHandler aHandler =  
	   BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.STRICT);
	aParser.addBDFHandler(aHandler);
	aParser.open(in2,metadata);
	record = new DataRecord(metadata);
	record.init();

	aParser2.addBDFHandler(aHandler);
	aParser2.open(in,metadata);

	aParser3.addBDFHandler(aHandler);
	aParser3.open(in,metadata);

	testParser.addBDFHandler(aHandler);
}
	
   protected void tearDown() {
		aParser3.close();
		aParser3 = null;

		aParser2.close();
		aParser2 = null;
	
	   aParser.close();
	   aParser = null;
	   record  = null;

	testParser.close();
	testParser = null;

		aFixLenDataFormatter = null;
	   //remove testFile if any
	   File aFile=new File(testFile1);
		if(aFile.exists()) {
			aFile.delete();
		}
   }

   /**
	* Tests parsing a file that has all records on one line. Some data is incorrect or null.
	*
	*/
public void test_parsing_bad() {
// the content of the test file
//	N/AStone    101   01/11/93-15.5          112  11/03/02 -0.7Bone Broo    99        //
	int recCount = 0;
   try{
	   record=aParser.getNext(record);
	   aFixLenDataFormatter.write(record);
	   fail("Should raise an BadDataFormatException");
   } catch (BadDataFormatException e){	
   } catch (Exception ee){
	   fail("Should not throw Exception");
	   ee.printStackTrace();
   }
   
   try{
	   while((record=aParser.getNext(record))!=null){
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

   
   
	try{
		FileInputStream fis = new FileInputStream(testFile1);
		testParser.open(fis,metadata);
		record = new DataRecord(metadata);
		record.init();

		while((record=testParser.getNext(record))!=null){
			if(recCount==0) {
				assertEquals(record.getField(0).toString(),"-15.5");
				assertTrue(record.getField(1).isNull());
				assertEquals(record.getField(2).toString(),"112");
				assertEquals(record.getField(3).toString(),"11/03/02");
			} else if(recCount==1) {
				assertEquals(record.getField(0).toString(),"-0.7");
				assertEquals(record.getField(1).toString(),"Bone Broo");
				assertEquals(record.getField(2).toString(),"99");
				assertTrue(record.getField(3).isNull());
			}
			recCount++;
		}
	} catch (BadDataFormatException e){	
		fail("Should not raise an BadDataFormatException");
		e.printStackTrace();
	} catch (Exception ee){
		fail("Should not throw Exception");
		ee.printStackTrace();
	}
   assertEquals(2,recCount);
}

/**
 * Tests parsing a file that has all records on one line. Some data is correct and not null.
 *
 */
public void test_parsing_good() {
// the content of the test file
//	1.0Stone    101   01/11/93-15.5  Brook   112  11/03/02 -0.7Bone Broo    9901/01/03 //
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

   
   
	 try{
		 FileInputStream fis = new FileInputStream(testFile1);
		 testParser.open(fis,metadata);
		record = new DataRecord(metadata);
		record.init();

		 while((record=testParser.getNext(record))!=null){
			if(recCount==0) {
				assertEquals(record.getField(0).toString(),"1.0");
				assertEquals(record.getField(1).toString(),"Stone");
				assertEquals(record.getField(2).toString(),"101");
				assertEquals(record.getField(3).toString(),"01/11/93");
			} else if(recCount==1) {
				assertEquals(record.getField(0).toString(),"-15.5");
				assertEquals(record.getField(1).toString(),"Brook");
				assertEquals(record.getField(2).toString(),"112");
				assertEquals(record.getField(3).toString(),"11/03/02");
			} else if(recCount==2) {
				assertEquals(record.getField(0).toString(),"-0.7");
				assertEquals(record.getField(1).toString(),"Bone Broo");
				assertEquals(record.getField(2).toString(),"99");
				assertEquals(record.getField(3).toString(),"01/01/03");
			}
			recCount++;
		}
	} catch (BadDataFormatException e){	
		fail("Should not raise an BadDataFormatException");
		e.printStackTrace();
	} catch (Exception ee){
		fail("Should not throw Exception");
		ee.printStackTrace();
	}
   assertEquals(3,recCount);
}
	
/**
 * Tests parsing a file that has one record per one line. Some data is correct and not null.
 *
 */
public void test_parsing_NL_good() {
// the content of the test file
//	1.0Stone    101   01/11/93-15.5  Brook   112  11/03/02 -0.7Bone Broo    9901/01/03 //
	int recCount = 0;
   
	try{
		while((record=aParser3.getNext(record))!=null){
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

   
   
	 try{
		 FileInputStream fis = new FileInputStream(testFile1);
		 testParser.open(fis,metadata);
		record = new DataRecord(metadata);
		record.init();

		 while((record=testParser.getNext(record))!=null){
			if(recCount==0) {
				assertEquals(record.getField(0).toString(),"1.0");
				assertEquals(record.getField(1).toString(),"Stone");
				assertEquals(record.getField(2).toString(),"101");
				assertEquals(record.getField(3).toString(),"01/11/93");
			} else if(recCount==1) {
				assertEquals(record.getField(0).toString(),"-15.5");
				assertEquals(record.getField(1).toString(),"Brook");
				assertEquals(record.getField(2).toString(),"112");
				assertEquals(record.getField(3).toString(),"11/03/02");
			} else if(recCount==2) {
				assertEquals(record.getField(0).toString(),"-0.7");
				assertEquals(record.getField(1).toString(),"Bone Broo");
				assertEquals(record.getField(2).toString(),"99");
				assertEquals(record.getField(3).toString(),"01/01/03");
			}
			recCount++;
		}
	} catch (BadDataFormatException e){	
		fail("Should not raise an BadDataFormatException");
		e.printStackTrace();
	} catch (Exception ee){
		fail("Should not throw Exception");
		ee.printStackTrace();
	}
   assertEquals(3,recCount);
}


}
