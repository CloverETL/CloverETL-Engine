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

package org.jetel.exception;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.FixLenDataParser;
import org.jetel.exception.BadDataFormatException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

import junit.framework.TestCase;

/**
 * @author maciorowski
 *
 */
public class BadDataFormatExceptionHandler_FixLenDataParser2_Test extends TestCase {
	private FixLenDataParser aFixLenDataParser = null;
	private FixLenDataParser aParser2 = null;
	private DataRecord record;
	private FileInputStream in = null;
	private FileInputStream in2 = null;
	private DataRecordMetadata metadata = null;
	
	protected void setUp() { 
		DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
			
		try {
			//metadata = xmlReader.read(new FileInputStream("config/test/rec_def/FL28_rec.xml"));
			metadata = xmlReader.read(new FileInputStream("config/test/rec_def/FL28_null_def_rec.xml"));
			in = new FileInputStream("data/in/good/FL28_NL.txt");
			in2 = new FileInputStream("data/in/bad/FL28_NL_nulls.txt");
		} catch(FileNotFoundException e){
			e.printStackTrace();
		}
		
		Defaults.init();
	
		aParser2 = new FixLenCharDataParser();

		aFixLenDataParser = new FixLenCharDataParser();
		record = new DataRecord(metadata);
		record.init();
	}
	
	protected void tearDown() {
		aFixLenDataParser.close();
		aFixLenDataParser = null;

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
		IParserExceptionHandler aHandler = null;

		// test no handler ------------------------------------
		try {
			aFixLenDataParser.init(metadata);
		} catch (ComponentNotReadyException e1) {
		}
		aFixLenDataParser.setDataSource(in);
		try{
			while((record=aFixLenDataParser.getNext(record))!=null){}
		} catch (BadDataFormatException e){	
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee){
			ee.printStackTrace();
			fail("Should not throw Exception");
		}
		aFixLenDataParser.close();

	}
	
	/**
	 *  Test for @link for a well formatted data source.
	 *  strict handler
	 */
	
	public void test_strict_goodFile() {
		IParserExceptionHandler aHandler = null;
		// test strict handler ------------------------------------
				try {
			aFixLenDataParser.init(metadata);
		} catch (ComponentNotReadyException e1) {
		}
		aFixLenDataParser.setDataSource(in);

		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.STRICT);
		aFixLenDataParser.setExceptionHandler(aHandler);
		try{
			while((record=aFixLenDataParser.getNext(record))!=null){}
		} catch (BadDataFormatException e){	
			fail("Should not raise an BadDataFormatException");
			e.printStackTrace();
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
		aFixLenDataParser.close();
		 
	}
	
	/**
	 *  Test for @link for a well formatted data source.
	 *  controlled handler
	 */
	
	public void test_controlled_goodFile() {
		IParserExceptionHandler aHandler = null;
		// test controlled handler ------------------------------------
				try {
			aFixLenDataParser.init(metadata);
		} catch (ComponentNotReadyException e1) {
		}
		aFixLenDataParser.setDataSource(new ByteArrayInputStream(new String("" +
				"		1.0Stone    101   01/11/93" +
				"-15.5Brook    112   11/03/02" +
				"	-0.7Bone Broo99    01/01/03").getBytes()));

		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.CONTROLLED);
		aFixLenDataParser.setExceptionHandler(aHandler);
		
//		1.0Stone    101   01/11/93
//	  -15.5Brook   11211/03/02
//	   -0.7Bone Broo    9901/01/03
		int recCount = 0;
		try{
			while((record=aFixLenDataParser.getNext(record))!=null){
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
			e.printStackTrace();
			fail("Should not raise an BadDataFormatException");
		} catch (Exception ee){
			fail("Should not throw Exception");
			ee.printStackTrace();
		}
		aFixLenDataParser.close();

	}
	
	/**
	 *  Test for @link for a well formatted data source.
	 *  lenient handler
	 */
	
	public void test_lenient_goodFile() {
		IParserExceptionHandler aHandler = null;
		// test lenient handler ------------------------------------
				try {
			aFixLenDataParser.init(metadata);
		} catch (ComponentNotReadyException e1) {
		}
		aFixLenDataParser.setDataSource(new ByteArrayInputStream(new String(
				"		1.0Stone    101   01/11/93" +
				"-15.5Brook    112   11/03/02" +
				"	-0.7Bone Broo    9901/01/03").getBytes()));

		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.LENIENT);
		aFixLenDataParser.setExceptionHandler(aHandler);
		
//		1.0Stone    101   01/11/93
//	  -15.5  Brook   112  11/03/02
//	   -0.7Bone Broo    9901/01/03
 		int recCount = 0;
		try{
			while((record=aFixLenDataParser.getNext(record))!=null){
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
		IParserExceptionHandler aHandler = null;
		boolean failed = false;
		// test no handler ------------------------------------
		try{
			aParser2.init(metadata);
		} catch (ComponentNotReadyException e1) {
		}
	aParser2.setDataSource(in2);
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
		IParserExceptionHandler aHandler = null;

		// test strict handler ------------------------------------
		try{
			aParser2.init(metadata);
		} catch (ComponentNotReadyException e1) {
		}
		aParser2.setDataSource(in2);
		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.STRICT);
		aParser2.setExceptionHandler(aHandler);
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
		IParserExceptionHandler aHandler = null;

		// test controlled handler ------------------------------------
		try {
			aParser2.init(metadata);
		} catch (ComponentNotReadyException e1) {
		}
		aParser2.setDataSource(in2);
		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.CONTROLLED);
		aParser2.setExceptionHandler(aHandler);
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
		assertEquals(3,recCount);  //may need to be revised
		//depending how we implement nullable property
		aParser2.close();

	}
	

	/**
	 *  Test for a data source with poorly formatted fields.
	 *  lenient handler.
	 */
	
	public void test_lenient_badFile() {
		IParserExceptionHandler aHandler = null;
		// test lenient handler ------------------------------------
		try {
			aParser2.init(metadata);
		} catch (ComponentNotReadyException e1) {
		}
		aParser2.setDataSource(in2);
		aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.LENIENT);
		aParser2.setExceptionHandler(aHandler);
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