/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 24, 2003
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

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.jetel.data.DataRecord;
import org.jetel.data.FixLenDataParser2;
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
public class FixLenDataParser2Test  extends TestCase {
 private FixLenDataParser2 aParser2 = null;
 private DataRecord record;
	
 protected void setUp() { 
	 FileInputStream in = null;
	 FileInputStream in2 = null;
	 DataRecordMetadata metadata = null;
	 DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
			
	 try {
		 metadata = xmlReader.read(new FileInputStream("config\\test\\rec_def\\FL28_null_def_rec.xml"));
		 in2 = new FileInputStream("data\\in\\bad\\FL28_NL_nulls.txt");
	 } catch(FileNotFoundException e){
		 e.printStackTrace();
	 }
	
	 aParser2 = new FixLenDataParser2();
	 BadDataFormatExceptionHandler aHandler =  
	 	BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.STRICT);
	 aParser2.addBDFHandler(aHandler);
	 aParser2.open(in2,metadata);
	 record = new DataRecord(metadata);
	 record.init();
 }
	
	protected void tearDown() {
		aParser2.close();
		aParser2 = null;
		record  = null;
	}

 public void test_parsing() {
//	the content of the test file
//	   N/AStone          01/11/93
//	 -15.5          112  11/03/02
//	  -0.7Bone Broo    99        
	 int recCount = 0;
	try{
		record=aParser2.getNext(record);
		fail("Should raise an BadDataFormatException");
	} catch (BadDataFormatException e){	
	} catch (Exception ee){
		fail("Should not throw Exception");
		ee.printStackTrace();
	}
	 try{
		 while((record=aParser2.getNext(record))!=null){
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
	
}
