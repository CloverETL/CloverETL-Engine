

import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.jetel.connection.DBConnection;
import org.jetel.connection.SQLDataParser;
import org.jetel.data.DataRecord;
import org.jetel.exception.BadDataFormatException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.IParserExceptionHandler;
import org.jetel.exception.ParserExceptionHandlerFactory;
import org.jetel.exception.PolicyType;
import org.jetel.main.runGraph;
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
	 
	 runGraph.initEngine("../cloveretl.engine/plugins", null);
	DBConnection aDBConnection = null;
	 DataRecordMetadataXMLReaderWriter xmlReader = new DataRecordMetadataXMLReaderWriter();
			
	 try {
//		metadata = xmlReader.read(new FileInputStream("config\\test\\rec_def\\db_def_rec.xml"));
//		aDBConnection = new DBConnection("", "config\\test\\msaccess.clover_test.txt");
		metadata = xmlReader.read(new FileInputStream("../cloveretl.engine/config/test/rec_def/db_def_rec.xml"));
		aDBConnection = new DBConnection("conn", "../cloveretl.engine/examples/koule_postgre.cfg");
	 } catch(FileNotFoundException e){
		 e.printStackTrace();
	 }
	 
	 try {
		aDBConnection.init();
	} catch (ComponentNotReadyException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	aParser2 = new SQLDataParser("connection","SELECT * FROM bad");

	aParser1 = new SQLDataParser("connection","SELECT * FROM good");
	try {
		aParser1.init(metadata);
		aParser1.setDataSource(aDBConnection);
		aParser2.init(metadata);
		aParser2.setDataSource(aDBConnection);
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
	  IParserExceptionHandler aHandler = null;

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
	 IParserExceptionHandler aHandler = null;
	 // test strict handler ------------------------------------
	 aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.STRICT);
	 aParser1.setExceptionHandler(aHandler);
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
	 IParserExceptionHandler aHandler = null;
	 // test controlled handler ------------------------------------
	 aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.CONTROLLED);
	 aParser1.setExceptionHandler(aHandler);
		
	 int recCount = 0;
	 try{
		 while((record=aParser1.getNext(record))!=null){
			 if(recCount==0 || recCount==1) {
				 assertEquals(record.getField(0).toString(),"1.0");
				 assertEquals(record.getField(1).toString(),"Stone");
				 assertEquals(record.getField(2).toString(),"101");
				 assertEquals(record.getField(3).toString(),"1993-01-11 00:00:00");
			 } else if(recCount==2) {
				 assertEquals(record.getField(0).toString(),"-15.5");
				 assertEquals(record.getField(1).toString(),"Brook");
				 assertEquals(record.getField(2).toString(),"112");
				 assertEquals(record.getField(3).toString(),"2002-11-03 00:00:00");
			 } else if(recCount==3) {
				 assertEquals(record.getField(0).toString(),"-0.7");
				 assertEquals(record.getField(1).toString(),"Bone Broo");
				 assertEquals(record.getField(2).toString(),"99");
				 assertEquals(record.getField(3).toString(),"1993-01-01 00:00:00");
			 }
			 recCount++;
		 }
		 assertEquals(4,recCount);		
			
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
	 IParserExceptionHandler aHandler = null;
	 // test lenient handler ------------------------------------
	 aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.LENIENT);
	 aParser1.setExceptionHandler(aHandler);
		
	 int recCount = 0;
	 try{
		 while((record=aParser1.getNext(record))!=null){
			 if(recCount==0 || recCount==1) {
				 assertEquals(record.getField(0).toString(),"1.0");
				 assertEquals(record.getField(1).toString(),"Stone");
				 assertEquals(record.getField(2).toString(),"101");
				 assertEquals(record.getField(3).toString(),"1993-01-11 00:00:00");
			 } else if(recCount==2) {
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
		 assertEquals(4,recCount);
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
		 while((record=aParser2.getNext(record))!=null){
			 fail("Should throw Exception");
		 }
	 } catch (BadDataFormatException e){	
		 failed = true;
		System.out.println(e.getMessage());
	 } catch (RuntimeException re) {
		 fail("Should throw RuntimeException");
	 } catch (Exception ee){
		 ee.printStackTrace();
	 }
	 if(!failed)
		 fail("Should raise an BadDataFormatException");
 }
	

 /**
  *  Test for a data source with poorly formatted fields.
  *  No handler.
  */
	
 public void test_strict_badFile() {
	 IParserExceptionHandler aHandler = null;

	 // test strict handler ------------------------------------
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
 }
	

 /**
  *  Test for a data source with poorly formatted fields.
  *  controlled handler.
  */
	
 public void test_controlled_badFile() {
	 IParserExceptionHandler aHandler = null;

	 // test controlled handler ------------------------------------
	 aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.CONTROLLED);
	 aParser2.setExceptionHandler(aHandler);
	 int recCount = 0;
	 try{
		 while((record=aParser2.getNext(record))!=null){
			 recCount++;
		 }
	 } catch (BadDataFormatException e){	
		 System.out.println(e.getMessage());
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
	 IParserExceptionHandler aHandler = null;
	 // test lenient handler ------------------------------------
	 aHandler = ParserExceptionHandlerFactory.getHandler(PolicyType.LENIENT);
	 aParser2.setExceptionHandler(aHandler);
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
		 assertEquals(0,recCount);
	 } catch (BadDataFormatException e){	
		 fail("Should not raise an BadDataFormatException");
		 e.printStackTrace();
	 } catch (Exception ee){
		 fail("Should not throw Exception");
		 ee.printStackTrace();
	 }
 }


}
