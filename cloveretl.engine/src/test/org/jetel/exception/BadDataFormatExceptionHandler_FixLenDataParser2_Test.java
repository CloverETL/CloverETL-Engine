/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 23, 2003
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

package test.org.jetel.exception;

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
public class BadDataFormatExceptionHandler_FixLenDataParser2_Test extends TestCase {
	private FixLenDataParser2 aFixLenDataParser2 = null;
	private FixLenDataParser2 aParser2 = null;
	private DataRecord record;
	
	protected void setUp() { 
		FileInputStream in = null;
		FileInputStream in2 = null;
		DataRecordMetadata metadata = null;
		DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
			
		try {
			//metadata = xmlReader.read(new FileInputStream("config\\test\\rec_def\\FL28_rec.xml"));
			metadata = xmlReader.read(new FileInputStream("config\\test\\rec_def\\FL28_null_def_rec.xml"));
			in = new FileInputStream("data\\in\\good\\FL28_NL.txt");
			in2 = new FileInputStream("data\\in\\bad\\FL28_NL_nulls.txt");
		} catch(FileNotFoundException e){
			e.printStackTrace();
		}
	
		aParser2 = new FixLenDataParser2();
		aParser2.open(in2,metadata);

		aFixLenDataParser2 = new FixLenDataParser2();
		aFixLenDataParser2.open(in,metadata);
		record = new DataRecord(metadata);
		record.init();
	}
	
	protected void tearDown() {
		aFixLenDataParser2.close();
		aFixLenDataParser2 = null;
		record  = null;
	}

	/**
	 *  Test for @link FixLenDataParser2 configured with strict
	 *  BadDataFormatExceptionHandler for a well formatted data source.
	 *
	 */
	
	public void test_FixLenDataParser2_strict_good() {
		BadDataFormatExceptionHandler aHandler =  
				BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.STRICT);
		aFixLenDataParser2.addBDFHandler(aHandler);
		try{
			while((record=aFixLenDataParser2.getNext(record))!=null){}
		} catch (BadDataFormatException e){	
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
	}
	
	/**
	 *  Test for @link FixLenDataParser2 configured with controlled
	 *  BadDataFormatExceptionHandler for a well formatted data source.
	 *
	 */
	
	public void test_FixLenDataParser2_controlled_good() {
		BadDataFormatExceptionHandler aHandler =  
				BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.CONTROLLED);
		aFixLenDataParser2.addBDFHandler(aHandler);
		
//		1.0Stone    101   01/11/93
//	  -15.5  Brook   112  11/03/02
//	   -0.7Bone Broo    9901/01/03
		int recCount = 0;
		try{
			while((record=aFixLenDataParser2.getNext(record))!=null){
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
			assertEquals(3,recCount);		} catch (BadDataFormatException e){	
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
	}

	/**
	 *  Test for @link FixLenDataParser2 configured with lenient
	 *  BadDataFormatExceptionHandler for a well formatted data source.
	 *
	 */
	
	public void test_FixLenDataParser2_lenient_good() {
		BadDataFormatExceptionHandler aHandler =  
				BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.LENIENT);
		aFixLenDataParser2.addBDFHandler(aHandler);
		
//		1.0Stone    101   01/11/93
//	  -15.5  Brook   112  11/03/02
//	   -0.7Bone Broo    9901/01/03
 		int recCount = 0;
		try{
			while((record=aFixLenDataParser2.getNext(record))!=null){
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
			assertEquals(3,recCount);
		} catch (BadDataFormatException e){	
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
	}
	

	/**
	 *  Test for @link FixLenDataParser2 configured with strict
	 *  BadDataFormatExceptionHandler for a data source with poorly formatted fields.
	 *
	 */
	
	public void test_FixLenDataParser2_strict_bad() {
		boolean failed = false;
		
		BadDataFormatExceptionHandler aHandler =  
				BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.STRICT);
		aParser2.addBDFHandler(aHandler);
		int recCount = 0;
		try{
			while((record=aParser2.getNext(record))!=null){
				recCount++;
			}
		} catch (BadDataFormatException e){	
			failed = true;
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
		if(!failed)
			fail("Should raise an BadDataFormatException");
		assertEquals(0,recCount);
	}
	
	/**
	 *  Test for @link FixLenDataParser2 configured with controlled
	 *  BadDataFormatExceptionHandler for  a data source with poorly formatted fields.
	 *
	 */
	
	public void test_FixLenDataParser2_controlled_bad() {
		BadDataFormatExceptionHandler aHandler =  
				BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.CONTROLLED);
		aParser2.addBDFHandler(aHandler);
		int recCount = 0;
		try{
			while((record=aParser2.getNext(record))!=null){
				recCount++;
			}
		} catch (BadDataFormatException e){	
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
		assertEquals(1,recCount);  //may need to be revised
		//depending how we implement nullable property
	}

	/**
	 *  Test for @link FixLenDataParser2 configured with lenient
	 *  BadDataFormatExceptionHandler for a data source with poorly formatted fields.
	 *
	 */
	
	public void test_FixLenDataParser2_lenient_bad() {
		BadDataFormatExceptionHandler aHandler =  
				BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.LENIENT);
		aParser2.addBDFHandler(aHandler);
		int recCount = 0;
		
// the content of the test file
//		N/AStone          01/11/93
//	  -15.5          112  11/03/02
//	   -0.7Bone Broo    99        
		try{
			while((record=aParser2.getNext(record))!=null){
				if(recCount==0) {
					assertEquals(record.getField(0).toString(),"0.0");
					assertEquals(record.getField(2).toString(),"5");
				} else if(recCount==1) {
					assertEquals(record.getField(1).toString(),"No Name");
				} else if(recCount==2) {
					assertEquals(record.getField(3).toString(),"01/01/00");
				}
				recCount++;
			}
			assertEquals(3,recCount);
		} catch (BadDataFormatException e){	
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
	}

}