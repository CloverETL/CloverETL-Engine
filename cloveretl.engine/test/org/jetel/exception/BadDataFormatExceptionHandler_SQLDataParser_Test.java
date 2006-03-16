/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Created on Apr 28, 2003
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
import org.jetel.data.parser.SQLDataParser;
import org.jetel.database.DBConnection;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.BadDataFormatExceptionHandler;
import org.jetel.exception.BadDataFormatExceptionHandlerFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;

import junit.framework.TestCase;

/**
 * @author maciorowski
 *
 */
public class BadDataFormatExceptionHandler_SQLDataParser_Test  extends TestCase {
 private SQLDataParser aParser1 = null;
 private SQLDataParser aParser2 = null;
 private DataRecord record;
 private DataRecordMetadata metadata = null;
	
 protected void setUp() { 
	DBConnection aDBConnection = null;
	 DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
			
	 try {
		metadata = xmlReader.read(new FileInputStream("config\\test\\rec_def\\db_def_rec.xml"));
		aDBConnection = new DBConnection("config\\test\\msaccess.clover_test.txt");
	 } catch(FileNotFoundException e){
		 e.printStackTrace();
	 }
	
	aParser2 = new SQLDataParser("connection","SELECT * FROM bad");

	aParser1 = new SQLDataParser("connection","SELECT * FROM good");
	try {
		aParser1.open(aDBConnection,metadata);
		aParser2.open(aDBConnection,metadata);
	} catch (ComponentNotReadyException e1) {
		e1.printStackTrace();
	}
	 record = new DataRecord(metadata);
	 record.init();
	 aParser1.initSQLDataMap(record);
	 aParser2.initSQLDataMap(record);
 }
	
 protected void tearDown() {
	 aParser1.close();
	 aParser1 = null;

	 aParser2.close();
	 aParser2 = null;
	 record  = null;
		
	 metadata = null;
 }
	
 /**
  *  Test for @link for a well formatted data source.
  *  No handler
  */
	
 public void test_goodFile() {
	 BadDataFormatExceptionHandler aHandler = null;

	 // test no handler ------------------------------------
	 try{
		 while((record=aParser1.getNext(record))!=null){}
	 } catch (BadDataFormatException e){	
		 fail("Should not raise an BadDataFormatException");
		 e.printStackTrace();
	 } catch (Exception ee){
		 fail("Should not throw Exception");
		 ee.printStackTrace();
	 }

 }
	
 /**
  *  Test for @link for a well formatted data source.
  *  strict handler
  */
	
 public void test_strict_goodFile() {
	 BadDataFormatExceptionHandler aHandler = null;
	 // test strict handler ------------------------------------
	 aHandler = BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.STRICT);
	 aParser1.addBDFHandler(aHandler);
	 try{
		 while((record=aParser1.getNext(record))!=null){}
	 } catch (BadDataFormatException e){	
		 fail("Should not raise an BadDataFormatException");
		 e.printStackTrace();
	 } catch (Exception ee){
		 fail("Should not throw Exception");
		 ee.printStackTrace();
	 }
		 
 }
	
 /**
  *  Test for @link for a well formatted data source.
  *  controlled handler
  */
	
 public void test_controlled_goodFile() {
	 BadDataFormatExceptionHandler aHandler = null;
	 // test controlled handler ------------------------------------
	 aHandler = BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.CONTROLLED);
	 aParser1.addBDFHandler(aHandler);
		
	 int recCount = 0;
	 try{
		 while((record=aParser1.getNext(record))!=null){
			 if(recCount==0) {
				 assertEquals(record.getField(0).toString(),"1.0");
				 assertEquals(record.getField(1).toString(),"Stone");
				 assertEquals(record.getField(2).toString(),"101");
				 assertEquals(record.getField(3).toString(),"1993-01-11 00:00:00");
			 } else if(recCount==1) {
				 assertEquals(record.getField(0).toString(),"-15.5");
				 assertEquals(record.getField(1).toString(),"Brook");
				 assertEquals(record.getField(2).toString(),"112");
				 assertEquals(record.getField(3).toString(),"2002-11-03 00:00:00");
			 } else if(recCount==2) {
				 assertEquals(record.getField(0).toString(),"-0.7");
				 assertEquals(record.getField(1).toString(),"Bone Broo");
				 assertEquals(record.getField(2).toString(),"99");
				 assertEquals(record.getField(3).toString(),"2003-01-01 00:00:00");
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
  *  Test for @link for a well formatted data source.
  *  lenient handler
  */
	
 public void test_lenient_goodFile() {
	 BadDataFormatExceptionHandler aHandler = null;
	 // test lenient handler ------------------------------------
	 aHandler = BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.LENIENT);
	 aParser1.addBDFHandler(aHandler);
		
	 int recCount = 0;
	 try{
		 while((record=aParser1.getNext(record))!=null){
			 if(recCount==0) {
				 assertEquals(record.getField(0).toString(),"1.0");
				 assertEquals(record.getField(1).toString(),"Stone");
				 assertEquals(record.getField(2).toString(),"101");
				 assertEquals(record.getField(3).toString(),"1993-01-11 00:00:00");
			 } else if(recCount==1) {
				 assertEquals(record.getField(0).toString(),"-15.5");
				 assertEquals(record.getField(1).toString(),"Brook");
				 assertEquals(record.getField(2).toString(),"112");
				 assertEquals(record.getField(3).toString(),"2002-11-03 00:00:00");
			 } else if(recCount==2) {
				 assertEquals(record.getField(0).toString(),"-0.7");
				 assertEquals(record.getField(1).toString(),"Bone Broo");
				 assertEquals(record.getField(2).toString(),"99");
				 assertEquals(record.getField(3).toString(),"2003-01-01 00:00:00");
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
 }
	

 /**
  *  Test for a data source with poorly formatted fields.
  *  controlled handler.
  */
	
 public void test_controlled_badFile() {
	 BadDataFormatExceptionHandler aHandler = null;

	 // test controlled handler ------------------------------------
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
	 assertEquals(0,recCount);  //may need to be revised
	 //depending how we implement nullable property

 }
	

 /**
  *  Test for a data source with poorly formatted fields.
  *  lenient handler.
  */
	
 public void test_lenient_badFile() {
	 BadDataFormatExceptionHandler aHandler = null;
	 // test lenient handler ------------------------------------
	 aHandler = BadDataFormatExceptionHandlerFactory.getHandler(BadDataFormatExceptionHandler.LENIENT);
	 aParser2.addBDFHandler(aHandler);
	 int recCount = 0;
		
	 try{
		 while((record=aParser2.getNext(record))!=null){
			 if(recCount==0) {
				 assertEquals("No Name",record.getField(1).toString());
				 assertEquals("101",record.getField(2).toString());
			 } else if(recCount==1) {
				 assertEquals("Brook",record.getField(1).toString());
				 assertEquals("2000-01-01 00:00:00",record.getField(3).toString());
			 } else if(recCount==2) {
				assertEquals("0.0",record.getField(0).toString());
				assertEquals("5",record.getField(2).toString());
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
