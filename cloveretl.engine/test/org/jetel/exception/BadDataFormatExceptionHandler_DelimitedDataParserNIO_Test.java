/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 25, 2003
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

import junit.framework.TestCase;

import org.jetel.data.DataRecord;
import org.jetel.data.parser.DelimitedDataParserNIO;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.BadDataFormatExceptionHandlerFactory;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

/**
 * @author maciorowski
 *
 */
public class BadDataFormatExceptionHandler_DelimitedDataParserNIO_Test  extends TestCase {
	private DelimitedDataParserNIO aParser = null;
	private DelimitedDataParserNIO aParser2 = null;
	private DataRecord record;
	private FileInputStream in = null;
	private FileInputStream in2 = null;
	private DataRecordMetadata metadata = null;
	
	protected void setUp() { 
		DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
			
		try {
			metadata = xmlReader.read(new FileInputStream("config\\test\\rec_def\\DL_null_def_rec.xml"));
			in = new FileInputStream("data\\in\\good\\DL28_NL.txt");
			in2 = new FileInputStream("data\\in\\bad\\DL_NL_nulls.txt");
		} catch(FileNotFoundException e){
			e.printStackTrace();
		}
	
		aParser2 = new DelimitedDataParserNIO();

		aParser = new DelimitedDataParserNIO();
		record = new DataRecord(metadata);
		record.init();
	}
	
	protected void tearDown() {
		aParser.close();
		aParser = null;

		aParser2.close();
		aParser2 = null;
		record  = null;
		
		metadata = null;
		in = null;
		in2 = null;
	}
	
	/**
	 *  Test for @link for a well formatted data source.
	 *  No handler
	 */
	
	public void test_goodFile() {
		BadDataFormatExceptionHandler aHandler = null;

		// test no handler ------------------------------------
		aParser.open(in,metadata);
		try{
			while((record=aParser.getNext(record))!=null){}
		} catch (BadDataFormatException e){	
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
		aParser.close();

	}
	
	/**
	 *  Test for @link for a well formatted data source.
	 *  strict handler
	 */
	
	public void test_strict_goodFile() {
		BadDataFormatExceptionHandler aHandler = null;
		// test strict handler ------------------------------------
		aParser.open(in,metadata);
		aHandler = BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.STRICT);
		aParser.addBDFHandler(aHandler);
		try{
			while((record=aParser.getNext(record))!=null){}
		} catch (BadDataFormatException e){	
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
		aParser.close();
		 
	}
	
	/**
	 *  Test for @link for a well formatted data source.
	 *  controlled handler
	 */
	
	public void test_controlled_goodFile() {
		BadDataFormatExceptionHandler aHandler = null;
		// test controlled handler ------------------------------------
		aParser.open(in,metadata);
		aHandler = BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.CONTROLLED);
		aParser.addBDFHandler(aHandler);
		
//		1.0Stone    101   01/11/93
//	  -15.5  Brook   112  11/03/02
//	   -0.7Bone Broo    9901/01/03
		int recCount = 0;
		try{
			while((record=aParser.getNext(record))!=null){
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
		aParser.close();

	}
	
	/**
	 *  Test for @link for a well formatted data source.
	 *  lenient handler
	 */
	
	public void test_lenient_goodFile() {
		BadDataFormatExceptionHandler aHandler = null;
		// test lenient handler ------------------------------------
		aParser.open(in,metadata);
		aHandler = BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.LENIENT);
		aParser.addBDFHandler(aHandler);
		
//		1.0Stone    101   01/11/93
//	  -15.5  Brook   112  11/03/02
//	   -0.7Bone Broo    9901/01/03
		int recCount = 0;
		try{
			while((record=aParser.getNext(record))!=null){
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
	 *  Test for a data source with poorly formatted fields.
	 *  No handler.
	 */
	
	public void test_badFile() {
		BadDataFormatExceptionHandler aHandler = null;
		boolean failed = false;
		// test no handler ------------------------------------
		aParser2.open(in2,metadata);
		try{
			while((record=aParser2.getNext(record))!=null){
				fail("Should throw Exception");
			}
		} catch (BadDataFormatException e){	
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (RuntimeException re) {
			failed = true;
		} catch (Exception ee){
			ee.printStackTrace();
		}
		aParser2.close();
		if(!failed)
			fail("Should raise an RuntimeException");
	}
	

	/**
	 *  Test for a data source with poorly formatted fields.
	 *  No handler.
	 */
	
	public void test_strict_badFile() {
		BadDataFormatExceptionHandler aHandler = null;

		// test strict handler ------------------------------------
		aParser2.open(in2,metadata);
		aHandler = BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.STRICT);
		aParser2.addBDFHandler(aHandler);
		int recCount = 0;
		try{
			while((record=aParser2.getNext(record))!=null){
				recCount++;
			}
			fail("Should raise an BadDataFormatException");
		} catch (BadDataFormatException e){	
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
		assertEquals(0,recCount);
		aParser2.close();
	}
	

	/**
	 *  Test for a data source with poorly formatted fields.
	 *  controlled handler.
	 */
	
	public void test_controlled_badFile() {
		BadDataFormatExceptionHandler aHandler = null;

		// test controlled handler ------------------------------------
		aParser2.open(in2,metadata);
		aHandler = BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.CONTROLLED);
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
		assertEquals(2,recCount);  //may need to be revised
		//depending how we implement nullable property
		aParser2.close();

	}
	

	/**
	 *  Test for a data source with poorly formatted fields.
	 *  lenient handler.
	 */
	
	public void test_lenient_badFile() {
		BadDataFormatExceptionHandler aHandler = null;
		// test lenient handler ------------------------------------
		aParser2.open(in2,metadata);
		aHandler = BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.LENIENT);
		aParser2.addBDFHandler(aHandler);
		int recCount = 0;
		
// the content of the test file
//		N/AStone          01/11/93
//	  -15.5          112  11/03/02
//	   -0.7Bone Broo    99        
		try{
			while((record=aParser2.getNext(record))!=null){
				if(recCount==0) {
					assertEquals("0.0",record.getField(0).toString());
					assertEquals("",record.getField(2).toString());
					assertEquals(null,record.getField(2).getValue());
				} else if(recCount==1) {
					assertEquals("",record.getField(1).toString());
					assertEquals(null,record.getField(1).getValue());
				} else if(recCount==2) {
					assertEquals("",record.getField(3).toString());
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