/*
*    jETeL/Clover.ETL - Java based ETL application framework.
*    Copyright (C) 2002-2004  David Pavlis <david_pavlis@hotmail.com>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package org.jetel.interpreter;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.ByteDataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.SetVal;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.lookup.LookupTableFactory;
import org.jetel.data.parser.Parser;
import org.jetel.data.primitive.ByteArray;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.data.sequence.Sequence;
import org.jetel.data.sequence.SequenceFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.TransformationGraph;
import org.jetel.interpreter.ASTnode.CLVFFunctionDeclaration;
import org.jetel.interpreter.ASTnode.CLVFStart;
import org.jetel.interpreter.ASTnode.CLVFStartExpression;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.interpreter.data.TLNullValue;
import org.jetel.interpreter.data.TLVariable;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.test.CloverTestCase;
import org.jetel.util.MiscUtils;
import org.jetel.util.string.StringAproxComparator;
import org.jetel.util.string.StringUtils;

/**
 * @author dpavlis
 * @since  10.8.2004
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
public class InterpreterTest extends CloverTestCase {
	
	DataRecordMetadata metadata,metadata1,metaOut,metaOut1,metadataBinary;
	DataRecord record,record1,out,out1,recBinary,recBinaryOut;
    TransformationGraph graph;
    LookupTable lkp;
	private GregorianCalendar today;
	static byte[] BYTEARRAY_INITVALUE = new byte[] {0x41,0x42,0x43,0x44,0x45,0x46,0x47,0x48};
	
	@Override
	protected void setUp() throws Exception {
		super.setUp();
	    
        graph=new TransformationGraph();
        
		metadata=new DataRecordMetadata("in",DataRecordMetadata.DELIMITED_RECORD);
		
		metadata.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metadata.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("BooleanValueF",DataFieldMetadata.BOOLEAN_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("BooleanValueT",DataFieldMetadata.BOOLEAN_FIELD, "\n"));
		
		metadata1=new DataRecordMetadata("in1",DataRecordMetadata.DELIMITED_RECORD);
		
		metadata1.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metadata1.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metadata1.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metadata1.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metadata1.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, "\n"));
		
		metaOut=new DataRecordMetadata("out",DataRecordMetadata.DELIMITED_RECORD);
		
		metaOut.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metaOut.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metaOut.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metaOut.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metaOut.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, "\n"));
				
		metaOut1=new DataRecordMetadata("out1",DataRecordMetadata.DELIMITED_RECORD);
		
		metaOut1.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metaOut1.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metaOut1.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metaOut1.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metaOut1.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, "\n"));
		
		metadataBinary=new DataRecordMetadata("inBinary",DataRecordMetadata.DELIMITED_RECORD);
		
		metadataBinary.addField(new DataFieldMetadata("Binary",DataFieldMetadata.BYTE_FIELD, ";"));
		metadataBinary.addField(new DataFieldMetadata("Compressed",DataFieldMetadata.BYTE_FIELD_COMPRESSED, "\n"));

		record = DataRecordFactory.newRecord(metadata);
		record.init();
		record1 = DataRecordFactory.newRecord(metadata1);
		record1.init();
		out = DataRecordFactory.newRecord(metaOut);
		out.init();
		out1 = DataRecordFactory.newRecord(metaOut1);
		out1.init();
		
		recBinary= DataRecordFactory.newRecord(metadataBinary);
		recBinary.init();
		recBinaryOut=DataRecordFactory.newRecord(metadataBinary);
		recBinaryOut.init();
		
		SetVal.setString(record,0,"  HELLO ");
		SetVal.setString(record1,0,"  My name ");
		SetVal.setInt(record,1,135);
		SetVal.setDouble(record1,1,13.5);
		SetVal.setString(record,2,"Some silly longer string.");
		SetVal.setString(record1,2,"Prague");
	    today = (GregorianCalendar)Calendar.getInstance();
		SetVal.setValue(record1,3,today.getTime());
		record.getField("Born").setNull(true);
		SetVal.setInt(record,4,-999);
		record1.getField("Value").setNull(true);
		record.getField("BooleanValueT").setValue(Boolean.TRUE);
		recBinary.getField(0).setValue(BYTEARRAY_INITVALUE);
        
        Sequence seq = SequenceFactory.createSequence(graph, "PRIMITIVE_SEQUENCE", 
        		new Object[]{"test",graph,"test"}, new Class[]{String.class,TransformationGraph.class,String.class});
        graph.addSequence(seq);
        try {
			seq.init();
		} catch (ComponentNotReadyException e) {
            throw new RuntimeException(e);
		}
        
//        LookupTable lkp=new SimpleLookupTable("LKP", metadata, new String[] {"Name"}, null);
        lkp = LookupTableFactory.createLookupTable(graph, "simpleLookup", 
        		new Object[]{"LKP" , metadata ,new String[] {"Name"} , null}, new Class[]{String.class, 
        		DataRecordMetadata.class, String[].class, Parser.class});
        try {
        lkp.init();
        graph.addLookupTable(lkp);
        }catch(Exception ex) {
            throw new RuntimeException(ex);
        }
        lkp.put(record);
        record.getField("Name").setValue("xxxx");
        lkp.put(record);
  
//        RecordKey key = new RecordKey(new int[]{0}, metadata);
//        key.init();
//        lkp.setLookupKey(key);
//        DataRecord keyRecord = new DataRecord(metadata);
//        keyRecord.init();
//        keyRecord.getField(0).setValue("one");
//        lkp.setLookupKey("nesmysl");
	}
	
	@Override
	protected void tearDown() {
		metadata= null;
		record=null;
		out=null;
	}
	
	public void testA_lexer(){
		//new ByteArrayInputStream("\"([^\\\\|]*\\\\|){3}\"".getBytes())
		/*System.out.print("enter token string:");
		JavaCharStream cs=new JavaCharStream(System.in);
		TransformLangParserTokenManager ltm;
		ltm=new TransformLangParserTokenManager(cs);
		Token t = ltm.getNextToken();
		System.out.println(t.image);*/
		//assertEquals(t.kind,TransformLangParserConstants.STRING_LITERAL);
		
		// test expression
		System.out.print("enter exp string: ");
		
		   String strin="$Name~=\"([^\\\\|]*\\\\|){3}\"";
		   System.out.println(strin);
		   TransformLangParser parser = new TransformLangParser(metadata,strin);
		    
		   CLVFStartExpression parseTree=null;
		   try{
			   parseTree = parser.StartExpression();
			   System.out.println("Initializing parse tree..");
			      parseTree.init();
			   System.out.println("Parse tree:");
			      parseTree.dump("");

			      
			      
			      /*
			      System.out.println("Interpreting parse tree..");
			      TransformLangExecutor executor=new TransformLangExecutor();
			      executor.setInputRecords(new DataRecord[] {record});
			      executor.visit(parseTree,null);
			      */
			      System.out.println("Finished interpreting.");
		   }catch(ParseException ex){
			   ex.printStackTrace();
		   }
	}
	
	public void test_int(){
		System.out.println("int test:");
		String expStr = "int i; i=0; print_err(i); \n"+
						"int j; j=-1; print_err(j);\n"+
						"int h1; h1=0x10; print_err(h1);\n"+
						"int h2; h2=-0x11011; print_err(h2);\n"+
						"int h3; h3=0x1f1; print_err(h3);\n"+
						"int o1; o1=010; print_err(o1);\n"+
						"int o2; o2=-011111; print_err(o2);\n"+
						"int minInt; minInt="+Integer.MIN_VALUE+"; print_err(minInt, true);\n"+
						"int maxInt; maxInt="+Integer.MAX_VALUE+"; print_err(maxInt, true);\n"+
						"int field; field=$Value; print_err(field);\n" +
						"$Value := 1234;\n";

		try {
		      print_code(expStr);

		      TransformLangParser parser = new TransformLangParser(new DataRecordMetadata[]{record.getMetadata()},new DataRecordMetadata[]{record.getMetadata()}, expStr);
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.setOutputRecords(new DataRecord[]{record1});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals(0,executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getInt());
		      assertEquals(-1,executor.getGlobalVariable(parser.getGlobalVariableSlot("j")).getTLValue().getNumeric().getInt());
		      assertEquals(0x10,executor.getGlobalVariable(parser.getGlobalVariableSlot("h1")).getTLValue().getNumeric().getInt());
		      assertEquals(-0x11011,executor.getGlobalVariable(parser.getGlobalVariableSlot("h2")).getTLValue().getNumeric().getInt());
		      assertEquals(0x1f1,executor.getGlobalVariable(parser.getGlobalVariableSlot("h3")).getTLValue().getNumeric().getInt());
		      assertEquals(010,executor.getGlobalVariable(parser.getGlobalVariableSlot("o1")).getTLValue().getNumeric().getInt());
		      assertEquals(-011111,executor.getGlobalVariable(parser.getGlobalVariableSlot("o2")).getTLValue().getNumeric().getInt());
		      assertEquals(Integer.MIN_VALUE,executor.getGlobalVariable(parser.getGlobalVariableSlot("minInt")).getTLValue().getNumeric().getInt());
		      assertEquals(Integer.MAX_VALUE,executor.getGlobalVariable(parser.getGlobalVariableSlot("maxInt")).getTLValue().getNumeric().getInt());
		      assertEquals(((Integer)record.getField("Value").getValue()).intValue(),executor.getGlobalVariable(parser.getGlobalVariableSlot("field")).getTLValue().getNumeric().getInt());
	    } catch (ParseException e) {
	    	System.err.println(e.getMessage());
	    	e.printStackTrace();
	    	throw new RuntimeException("Parse exception",e);
	    }
		      
	}
	
	public void test_long(){
		System.out.println("\nlong test:");
		String expStr = "long i; i=0; print_err(i); \n"+
						"long j; j=-1; print_err(j);\n"+
						"long minLong; minLong="+(Long.MIN_VALUE+1)+"; print_err(minLong);\n"+
						"long maxLong; maxLong="+(Long.MAX_VALUE)+"; print_err(maxLong);\n"+
						"long field; field=$Value; print_err(field);\n"+
						"long wrong;wrong="+Long.MAX_VALUE+"; print_err(wrong);\n";
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals(0,executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getLong());
		      assertEquals(-1,executor.getGlobalVariable(parser.getGlobalVariableSlot("j")).getTLValue().getNumeric().getLong());
		      assertEquals(Long.MIN_VALUE+1,executor.getGlobalVariable(parser.getGlobalVariableSlot("minLong")).getTLValue().getNumeric().getLong());
		      assertEquals(Long.MAX_VALUE,executor.getGlobalVariable(parser.getGlobalVariableSlot("maxLong")).getTLValue().getNumeric().getLong());
		      assertEquals(((Integer)record.getField("Value").getValue()).longValue(),executor.getGlobalVariable(parser.getGlobalVariableSlot("field")).getTLValue().getNumeric().getLong());
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    }
	}

	public void test_decimal(){
		System.out.println("\ndecimal test:");
		String expStr = "decimal i; i=0; print_err(i); \n"+
						"decimal j; j=-1.0; print_err(j);\n"+
						"decimal(18,3) minLong; minLong=999999.999d; print_err(minLong);\n"+
						"decimal maxLong; maxLong=0000000.0000000; print_err(maxLong);\n"+
						"decimal fieldValue; fieldValue=$Value; print_err(fieldValue);\n"+
						"decimal fieldAge; fieldAge=$Age; print_err(fieldAge);\n"+
						"decimal(400,350) minDouble; minDouble="+Double.MIN_VALUE+"d; print_err(minDouble);\n" +
						"decimal def;print_err(def);\n" +
						"print_err('the end');\n";
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      assertEquals(DecimalFactory.getDecimal(0),executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric());
		      assertEquals(DecimalFactory.getDecimal(-1),executor.getGlobalVariable(parser.getGlobalVariableSlot("j")).getTLValue().getNumeric());
		      assertEquals(DecimalFactory.getDecimal(999999.999),executor.getGlobalVariable(parser.getGlobalVariableSlot("minLong")).getTLValue().getNumeric());
		      assertEquals(DecimalFactory.getDecimal(0),executor.getGlobalVariable(parser.getGlobalVariableSlot("maxLong")).getTLValue().getNumeric());
		      assertEquals(((Integer)record.getField("Value").getValue()).intValue(),executor.getGlobalVariable(parser.getGlobalVariableSlot("fieldValue")).getTLValue().getNumeric().getInt());
		      assertEquals((Double)record.getField("Age").getValue(),executor.getGlobalVariable(parser.getGlobalVariableSlot("fieldAge")).getTLValue().getNumeric().getDouble());
		      assertEquals(Double.valueOf(Double.MIN_VALUE),executor.getGlobalVariable(parser.getGlobalVariableSlot("minDouble")).getTLValue().getNumeric().getDouble());
		      assertEquals(Double.valueOf(0),executor.getGlobalVariable(parser.getGlobalVariableSlot("def")).getTLValue().getNumeric().getDouble());

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }
		      
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    }
	}

	public void test_number(){
		System.out.println("\nnumber test:");
		String expStr = "number i; i=0; print_err(i); \n"+
						"number j; j=-1.0; print_err(j);\n"+
						"number minLong; minLong=999999.99911; print_err(minLong);  \n"+
						"number fieldValue; fieldValue=$Value; print_err(fieldValue);\n"+
						"number fieldAge; fieldAge=$Age; print_err(fieldAge);\n"+
						"number minDouble; minDouble="+Double.MIN_VALUE+"; print_err(minDouble);\n" +
						"number def;print_err(def);\n";
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(), expStr);
			  CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals(Double.valueOf(0),executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getDouble());
		      assertEquals(Double.valueOf(-1),executor.getGlobalVariable(parser.getGlobalVariableSlot("j")).getTLValue().getNumeric().getDouble());
		      assertEquals(Double.valueOf(999999.99911),executor.getGlobalVariable(parser.getGlobalVariableSlot("minLong")).getTLValue().getNumeric().getDouble());
		      assertEquals(Double.valueOf(((Integer)record.getField("Value").getValue())),executor.getGlobalVariable(parser.getGlobalVariableSlot("fieldValue")).getTLValue().getNumeric().getDouble());
		      assertEquals(Double.valueOf((Double)record.getField("Age").getValue()),executor.getGlobalVariable(parser.getGlobalVariableSlot("fieldAge")).getTLValue().getNumeric().getDouble());
		      assertEquals(Double.valueOf(Double.MIN_VALUE),executor.getGlobalVariable(parser.getGlobalVariableSlot("minDouble")).getTLValue().getNumeric().getDouble());
		      assertEquals(Double.valueOf(0),executor.getGlobalVariable(parser.getGlobalVariableSlot("def")).getTLValue().getNumeric().getDouble());
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    }
	}

	public void test_string(){
		System.out.println("\nstring test:");
		int lenght=1000;
        StringBuilder tmp = new StringBuilder(lenght);
		for (int i=0;i<lenght;i++){
			tmp.append(i%10);
		}
		String expStr = "string i; i=\"0\"; print_err(i); \n"+
						"string hello; hello='hello\\nworld'; print_err(hello);\n"+
						"string fieldName; fieldName=$Name; print_err(fieldName);\n"+
						"string fieldCity; fieldCity=$City; print_err(fieldCity);\n"+
						"string longString; longString=\""+tmp+"\"; print_err(longString);\n"+
						"string specialChars; specialChars='a\u0101\u0102A'; print_err(specialChars);\n" +
						"string empty=\"\";print_err(empty+specialChars);\n" +
						"print_err(\"\"+specialChars);\n" +
						"print_err(concat('', specialChars));\n";
	      print_code(expStr);
		
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes("UTF-8")));
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals("0",executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().toString());
		      assertEquals("hello\\nworld",executor.getGlobalVariable(parser.getGlobalVariableSlot("hello")).getTLValue().toString());
		      assertEquals(record.getField("Name").getValue().toString(),executor.getGlobalVariable(parser.getGlobalVariableSlot("fieldName")).getTLValue().toString());
		      assertEquals(record.getField("City").getValue().toString(),executor.getGlobalVariable(parser.getGlobalVariableSlot("fieldCity")).getTLValue().toString());
		      assertEquals(tmp.toString(),executor.getGlobalVariable(parser.getGlobalVariableSlot("longString")).getTLValue().toString());
		      assertEquals("a\u0101\u0102A",executor.getGlobalVariable(parser.getGlobalVariableSlot("specialChars")).getTLValue().toString());
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    } catch (UnsupportedEncodingException ex){
		        ex.printStackTrace();
            }
	}

	public void test_date(){
		System.out.println("\ndate test:");
		String expStr = "date d3; d3=2006-08-01; print_err(d3);\n"+
						"date d2; d2=2006-08-02 15:15:00 ; print_err(d2);\n"+
						"date d1; d1=2006-1-1 1:2:3; print_err(d1);\n"+
						"date born; born=$0.Born; print_err(born);\n" +
						"date dnull = null; print_err(dnull);\n";
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
		
	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

  		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals(new GregorianCalendar(2006,7,01).getTime(),executor.getGlobalVariable(parser.getGlobalVariableSlot("d3")).getTLValue().getDate());
		      assertEquals(new GregorianCalendar(2006,7,02,15,15).getTime(),executor.getGlobalVariable(parser.getGlobalVariableSlot("d2")).getTLValue().getDate());
		      assertEquals(new GregorianCalendar(2006,0,01,01,02,03).getTime(),executor.getGlobalVariable(parser.getGlobalVariableSlot("d1")).getTLValue().getDate());
		      assertEquals((Date)record.getField("Born").getValue(),executor.getGlobalVariable(parser.getGlobalVariableSlot("born")).getTLValue().getDate());


		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    }
	}

	public void test_boolean(){
		System.out.println("\nboolean test:");
		String expStr = "boolean b1; b1=true; print_err(b1);\n"+
						"boolean b2; b2=false ; print_err(b2);\n"+
						"boolean b4; print_err(b4);\n"+
						"print_err( iif($BooleanValueF == true,'true','false')); print_err( iif($BooleanValueT == true,'true','false'));";
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);
		
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("b1")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(false,executor.getGlobalVariable(parser.getGlobalVariableSlot("b2")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(false,executor.getGlobalVariable(parser.getGlobalVariableSlot("b4")).getTLValue()==TLBooleanValue.TRUE);
		      assertTrue(executor.getGlobalVariable(parser.getGlobalVariableSlot("b4")).isNullable());
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    }
	}

	public void test_variables(){
		System.out.println("\nvariable test:");
		String expStr = "boolean b1; boolean b2; b1=true; print_err(b1);\n"+
						"b2=false ; print_err(b2);\n"+
						"string b4; b4=\"hello\"; print_err(b4);\n"+
						"b2 = true; print_err(b2);\n" +
						"int in;\n" +
						"if (b2) {in=2;print_err('in');}\n"+
						"print_err(b2);\n" +
						"b4=null; print_err(b4);\n"+
						"b4='hi'; print_err(b4);";
	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();
		      
		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("b1")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("b2")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals("hi",executor.getGlobalVariable(parser.getGlobalVariableSlot("b4")).getTLValue().toString());
//		      assertEquals(2,executor.getGlobalVariable(parser.getGlobalVariableSlot("in")).getValue().getNumeric().getInt());
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    }
	}

	public void test_plus(){
		System.out.println("\nplus test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=100;\n" +
						"int iplusj;iplusj=i+j; print_err(\"plus int:\"+iplusj);\n" +
						"long l;l="+Integer.MAX_VALUE/10+"l;print_err(l);\n" +
						"long m;m="+(Integer.MAX_VALUE)+"l;print_err(m);\n" +
						"long lplusm;lplusm=l+m;print_err(\"plus long:\"+lplusm);\n" +
						"number n; n=0;print_err(n);\n" +
						"number m1; m1=0.001;print_err(m1);\n" +
						"number nplusm1; nplusm1=n+m1;print_err(\"plus number:\"+nplusm1);\n" +
						"number nplusj;nplusj=n+j;print_err(\"number plus int:\"+nplusj);\n"+
						"decimal d; d=0.1;print_err(d);\n" +
						"decimal(10,4) d1; d1=0.0001;print_err(d1);\n" +
						"decimal(10,4) dplusd1; dplusd1=d+d1;print_err(\"plus decimal:\"+dplusd1);\n" +
						"decimal dplusj;dplusj=d+j;print_err(\"decimal plus int:\"+dplusj);\n" +
						"decimal(10,4) dplusn;dplusn=d+m1;print_err(\"decimal plus number:\"+dplusn);\n" +
						"dplusn=dplusn+10;\n" +
						"string s; s=\"hello\"; print_err(s);\n" +
						"string s1;s1=\" world\";print_err(s1);\n " +
						"string spluss1;spluss1=s+s1;print_err(\"adding strings:\"+spluss1);\n" +
						"string splusm1;splusm1=s+m1;print_err(\"string plus decimal:\"+splusm1);\n" +
						"date mydate; mydate=2004-01-30 15:00:30;print_err(mydate);\n" +
						"date dateplus;dateplus=mydate+i;print_err(dateplus);\n";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals("iplusj",110,(executor.getGlobalVariable(parser.getGlobalVariableSlot("iplusj")).getTLValue().getNumeric().getInt()));
		      assertEquals("lplusm",(long)Integer.MAX_VALUE+(long)Integer.MAX_VALUE/10,executor.getGlobalVariable(parser.getGlobalVariableSlot("lplusm")).getTLValue().getNumeric().getLong());
		      assertEquals("nplusm1",Double.valueOf(0.001),executor.getGlobalVariable(parser.getGlobalVariableSlot("nplusm1")).getTLValue().getNumeric().getDouble());
		      assertEquals("nplusj",Double.valueOf(100),executor.getGlobalVariable(parser.getGlobalVariableSlot("nplusj")).getTLValue().getNumeric().getDouble());
		      assertEquals("dplusd1",Double.valueOf(0.1000),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusd1")).getTLValue().getNumeric().getDouble());
		      assertEquals("dplusj",Double.valueOf(100.1),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusj")).getTLValue().getNumeric().getDouble());
		      assertEquals("dplusn",Double.valueOf(10.1),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusn")).getTLValue().getNumeric().getDouble());
		      assertEquals("spluss1","hello world",executor.getGlobalVariable(parser.getGlobalVariableSlot("spluss1")).getTLValue().toString());
		      assertEquals("splusm1","hello0.001",executor.getGlobalVariable(parser.getGlobalVariableSlot("splusm1")).getTLValue().toString());
		      assertEquals("dateplus",new GregorianCalendar(2004,01,9,15,00,30).getTime(),executor.getGlobalVariable(parser.getGlobalVariableSlot("dateplus")).getTLValue().getDate());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_minus(){
		System.out.println("\nminus test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=100;\n" +
						"int iplusj;iplusj=i-j; print_err(\"minus int:\"+iplusj);\n" +
						"long l;l="+((long)Integer.MAX_VALUE+10)+";print_err(l);\n" +
						"long m;m=1;print_err(m);\n" +
						"long lplusm;lplusm=l-m;print_err(\"minus long:\"+lplusm);\n" +
						"number n; n=0;print_err(n);\n" +
						"number m1; m1=0.001;print_err(m1);\n" +
						"number nplusm1; nplusm1=n-m1;print_err(\"minus number:\"+nplusm1);\n" +
						"number nplusj;nplusj=n-j;print_err(\"number minus int:\"+nplusj);\n"+
						"decimal d; d=0.1;print_err(d);\n" +
						"decimal(10,4) d1; d1=0.0001d;print_err(d1);\n" +
						"decimal(10,4) dplusd1; dplusd1=d-d1;print_err(\"minus decimal:\"+dplusd1);\n" +
						"decimal dplusj;dplusj=d-j;print_err(\"decimal minus int:\"+dplusj);\n" +
						"decimal(10,4) dplusn;dplusn=d-m1;print_err(\"decimal minus number:\"+dplusn);\n" +
						"number d1minusm1;d1minusm1=d1-m1;print_err('decimal minus number = number:'+d1minusm1);\n" +
						"date mydate; mydate=2004-01-30 15:00:30;print_err(mydate);\n" +
						"date dateplus;dateplus=mydate-i;print_err(dateplus);\n";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      assertEquals("iplusj",-90,(executor.getGlobalVariable(parser.getGlobalVariableSlot("iplusj")).getTLValue().getNumeric().getInt()));
		      assertEquals("lplusm",(long)Integer.MAX_VALUE+9,executor.getGlobalVariable(parser.getGlobalVariableSlot("lplusm")).getTLValue().getNumeric().getLong());
		      assertEquals("nplusm1",Double.valueOf(-0.001),executor.getGlobalVariable(parser.getGlobalVariableSlot("nplusm1")).getTLValue().getNumeric().getDouble());
		      assertEquals("nplusj",Double.valueOf(-100),executor.getGlobalVariable(parser.getGlobalVariableSlot("nplusj")).getTLValue().getNumeric().getDouble());
//		      Decimal tmp = DecimalFactory.getDecimal(0.1);
//		      tmp.sub(DecimalFactory.getDecimal(0.0001,10,4));
//		      assertEquals("dplusd1",tmp, executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusd1")).getValue().getNumeric());
		      assertEquals("dplusd1",DecimalFactory.getDecimal(0.09), executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusd1")).getTLValue().getNumeric());
		      assertEquals("dplusj",Double.valueOf(-99.9),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusj")).getTLValue().getNumeric().getDouble());
		      assertEquals("dplusn",Double.valueOf(0.1),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusn")).getTLValue().getNumeric().getDouble());
		      assertEquals("d1minusm1",Double.valueOf(-0.0009),executor.getGlobalVariable(parser.getGlobalVariableSlot("d1minusm1")).getTLValue().getNumeric().getDouble());
		      assertEquals("dateplus",new GregorianCalendar(2004,0,20,15,00,30).getTime(),executor.getGlobalVariable(parser.getGlobalVariableSlot("dateplus")).getTLValue().getDate());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_multiply(){
		System.out.println("\nmultiply test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=100;\n" +
						"int iplusj;iplusj=i*j; print_err(\"multiply int:\"+iplusj);\n" +
						"long l;l="+((long)Integer.MAX_VALUE+10)+";print_err(l);\n" +
						"long m;m=1;print_err(m);\n" +
						"long lplusm;lplusm=l*m;print_err(\"multiply long:\"+lplusm);\n" +
						"number n; n=0.1;print_err(n);\n" +
						"number m1; m1=-0.01;print_err(m1);\n" +
						"number nplusm1; nplusm1=n*m1;print_err(\"multiply number:\"+nplusm1);\n" +
						"number m1plusj;m1plusj=m1*j;print_err(\"number multiply int:\"+m1plusj);\n"+
						"decimal(8,4) d; d=0.1; print_err(d);\n" +
						"decimal(10,4) d1; d1=10.01d;print_err(d1);\n" +
						"decimal(10,4) dplusd1; dplusd1=d*d1;print_err(\"multiply decimal:\"+dplusd1);\n" +
						"decimal(10,4) dplusj;dplusj=d*j;print_err(\"decimal multiply int:\"+dplusj);\n"+
						"decimal(10,4) dplusn;dplusn=d*n;print_err(\"decimal multiply number:\"+dplusn);\n";
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals("i*j",1000,(executor.getGlobalVariable(parser.getGlobalVariableSlot("iplusj")).getTLValue().getNumeric().getInt()));
		      assertEquals("l*m",(long)Integer.MAX_VALUE+10,executor.getGlobalVariable(parser.getGlobalVariableSlot("lplusm")).getTLValue().getNumeric().getLong());
		      assertEquals("n*m1",Double.valueOf(-0.001),executor.getGlobalVariable(parser.getGlobalVariableSlot("nplusm1")).getTLValue().getNumeric().getDouble());
		      assertEquals("m1*j",Double.valueOf(-1),executor.getGlobalVariable(parser.getGlobalVariableSlot("m1plusj")).getTLValue().getNumeric().getDouble());
		      assertEquals("d*d1",DecimalFactory.getDecimal(1.001,10,4),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusd1")).getTLValue().getNumeric());
		      assertEquals("d*j",DecimalFactory.getDecimal(10,10,4),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusj")).getTLValue().getNumeric());
		      assertEquals("d*n",DecimalFactory.getDecimal(0.01, 10, 4),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusn")).getTLValue().getNumeric());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_division(){
		System.out.println("\ndivision test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=100;\n" +
						"int iplusj;iplusj=i/j; print_err(\"div int:\"+iplusj);\n" +
						"int jdivi;jdivi=j/i; print_err(\"div int:\"+jdivi);\n" +
						"long l;l="+((long)Integer.MAX_VALUE+10)+";print_err(l);\n" +
						"long m;m=1;print_err(m);\n" +
						"long lplusm;lplusm=l/m;print_err(\"div long:\"+lplusm);\n" +
						"number n; n=0;print_err(n);\n" +
						"number m1; m1=0.01;print_err(m1);\n" +
						"number n1; n1=10;print_err(n1);\n" +
						"number nplusm1; nplusm1=n/m1;print_err(\"0/0.01:\"+nplusm1);\n" +
						"number m1divn; m1divn=m1/n;print_err(\"deleni nulou:\"+m1divn);\n" +
						"number m1divn1; m1divn1=m1/n1;print_err(\"deleni numbers:\"+m1divn1);\n" +
						"number m1plusj;m1plusj=j/n1;print_err(\"number division int:\"+m1plusj);\n"+
						"decimal d; d=0.1;print_err(d);\n" +
						"decimal(10,4) d1; d1=0.01;print_err(d1);\n" +
						"decimal(10,4)  dplusd1; dplusd1=d/d1;print_err(\"div decimal:\"+dplusd1);\n" +
						"decimal(10,4)  dplusj;dplusj=d/j;print_err(\"decimal div int:\"+dplusj);\n"+
						"decimal(10,4)  dplusn;dplusn=n1/d;print_err(\"decimal div number:\"+dplusn);\n";
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

  		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals("i/j",0,(executor.getGlobalVariable(parser.getGlobalVariableSlot("iplusj")).getTLValue().getNumeric().getInt()));
		      assertEquals("j/i",10,(executor.getGlobalVariable(parser.getGlobalVariableSlot("jdivi")).getTLValue().getNumeric().getInt()));
		      assertEquals("l/m",(long)Integer.MAX_VALUE+10,executor.getGlobalVariable(parser.getGlobalVariableSlot("lplusm")).getTLValue().getNumeric().getLong());
		      assertEquals("n/m1",Double.valueOf(0),executor.getGlobalVariable(parser.getGlobalVariableSlot("nplusm1")).getTLValue().getNumeric().getDouble());
		      assertEquals("m1/n",Double.valueOf(Double.POSITIVE_INFINITY),executor.getGlobalVariable(parser.getGlobalVariableSlot("m1divn")).getTLValue().getNumeric().getDouble());
		      assertEquals("m1/n1",Double.valueOf(0.001),executor.getGlobalVariable(parser.getGlobalVariableSlot("m1divn1")).getTLValue().getNumeric().getDouble());
		      assertEquals("j/n1",Double.valueOf(10),executor.getGlobalVariable(parser.getGlobalVariableSlot("m1plusj")).getTLValue().getNumeric().getDouble());
		      assertEquals("d/d1",DecimalFactory.getDecimal(0.1/0.01),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusd1")).getTLValue().getNumeric());
		      assertEquals("d/j",DecimalFactory.getDecimal(0.0000),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusj")).getTLValue().getNumeric());
		      assertEquals("n1/d",DecimalFactory.getDecimal(100.0000),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusn")).getTLValue().getNumeric());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_modulus(){
		System.out.println("\nmodulus test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=103;\n" +
						"int iplusj;iplusj=j%i; print_err(\"mod int:\"+iplusj);\n" +
						"long l;l="+((long)Integer.MAX_VALUE+10)+";print_err(l);\n" +
						"long m;m=2;print_err(m);\n" +
						"long lplusm;lplusm=l%m;print_err(\"mod long:\"+lplusm);\n" +
						"number n; n=10.2;print_err(n);\n" +
						"number m1; m1=2;print_err(m1);\n" +
						"number nplusm1; nplusm1=n%m1;print_err(\"mod number:\"+nplusm1);\n" +
						"number m1plusj;m1plusj=n%i;print_err(\"number mod int:\"+m1plusj);\n"+
						"decimal d; d=10.1;print_err(d);\n" +
						"decimal(10,4) d1; d1=10;print_err(d1);\n" +
						"decimal dplusd1; dplusd1=d%d1;print_err(\"mod decimal:\"+dplusd1);\n" +
						"decimal(10,4) dplusj;dplusj=d1%j;print_err(\"decimal mod int:\"+dplusj);\n"+
						"decimal dplusn;dplusn=d%m1;print_err(\"decimal mod number:\"+dplusn);\n";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals(3,(executor.getGlobalVariable(parser.getGlobalVariableSlot("iplusj")).getTLValue().getNumeric().getInt()));
		      assertEquals(((long)Integer.MAX_VALUE+10)%2,executor.getGlobalVariable(parser.getGlobalVariableSlot("lplusm")).getTLValue().getNumeric().getLong());
		      assertEquals(Double.valueOf(10.2%2),executor.getGlobalVariable(parser.getGlobalVariableSlot("nplusm1")).getTLValue().getNumeric().getDouble());
		      assertEquals(Double.valueOf(10.2%10),executor.getGlobalVariable(parser.getGlobalVariableSlot("m1plusj")).getTLValue().getNumeric().getDouble());
		      assertEquals(DecimalFactory.getDecimal(0.1),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusd1")).getTLValue().getNumeric());
		      assertEquals(DecimalFactory.getDecimal(10),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusj")).getTLValue().getNumeric());
		      assertEquals(DecimalFactory.getDecimal(0.1),executor.getGlobalVariable(parser.getGlobalVariableSlot("dplusn")).getTLValue().getNumeric());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_increment_decrement(){
		System.out.println("\nincrement-decrement test:");
		String expStr = "int i; i=10;print_err(++i);\n" +
						"i--;" +
						"print_err(--i);\n"+
						"long j;j="+(Long.MAX_VALUE-10)+"l;print_err(++j);\n" +
						"print_err(--j);\n"+
						"decimal d;d=2;d++;\n" +
						"print_err(--d);\n;" +
						"number n;n=3.5;print_err(++n);\n" +
						"n--;\n" +
						"{print_err(++n);}\n" +
						"print_err(++n);\n";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }
		      
		      assertEquals(9,(executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getInt()));
		      assertEquals(new CloverLong(Long.MAX_VALUE-10).
		    		  getLong(),executor.getGlobalVariable(parser.getGlobalVariableSlot("j")).getTLValue().getNumeric().getLong());
		      assertEquals(DecimalFactory.getDecimal(2),executor.getGlobalVariable(parser.getGlobalVariableSlot("d")).getTLValue().getNumeric());
		      assertEquals(Double.valueOf(5.5),executor.getGlobalVariable(parser.getGlobalVariableSlot("n")).getTLValue().getNumeric().getDouble());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_equal(){
		System.out.println("\nequal test:");
		String expStr = "int i; i=10;print_err(\"i=\"+i);\n" +
						"int j;j=9;print_err(\"j=\"+j);\n" +
						"boolean eq1; eq1=(i==j+1);print_err(\"eq1=\"+eq1);\n" +
//						"boolean eq1;eq1=(i.eq.(j+1));print_err(\"eq1=\"+eq1);\n" +
						"long l;l=10;print_err(\"l=\"+l);\n" +
						"boolean eq2;eq2=(l==j);print_err(\"eq2=\"+eq2);\n" +
						"eq2=(l.eq.i);print_err(\"eq2=\");print_err(eq2);\n" +
						"decimal d;d=10;print_err(\"d=\"+d);\n" +
						"boolean eq3;eq3=d==i;print_err(\"eq3=\"+eq3);\n" +
						"number n;n=10;print_err(\"n=\"+n);\n" +
						"boolean eq4;eq4=n.eq.l;print_err(\"eq4=\"+eq4);\n" +
						"boolean eq5;eq5=n==d;print_err(\"eq5=\"+eq5);\n" +
						"string s;s='hello';print_err(\"s=\"+s);\n" +
						"string s1;s1=\"hello \";print_err(\"s1=\"+s1);\n" +
						"boolean eq6;eq6=s.eq.s1;print_err(\"eq6=\"+eq6);\n" +
						"boolean eq7;eq7=s==trim(s1);print_err(\"eq7=\"+eq7);\n" +
						"date mydate;mydate=2006-01-01;print_err(\"mydate=\"+mydate);\n" +
						"date anothermydate;print_err(\"anothermydate=\"+anothermydate);\n" +
						"boolean eq8;eq8=mydate.eq.anothermydate;print_err(\"eq8=\"+eq8);\n" +
						"anothermydate=2006-1-1 0:0:0;print_err(\"anothermydate=\"+anothermydate);\n" +
						"boolean eq9;eq9=mydate==anothermydate;print_err(\"eq9=\"+eq9);\n" +
						"boolean eq10;eq10=eq9.eq.eq8;print_err(\"eq10=\"+eq10);\n";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq1")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq2")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq3")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq4")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq5")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(false,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq6")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq7")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(false,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq8")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq9")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(false,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq10")).getTLValue()==TLBooleanValue.TRUE);

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_non_equal(){
		System.out.println("\nNon equal test:");
		String expStr = "int i; i=10;print_err(\"i=\"+i);\n" +
						"int j;j=9;print_err(\"j=\"+j);\n" +
						"boolean eq1; eq1=(i!=j);print_err(\"eq1=\");print_err(eq1);\n" +
						"long l;l=10;print_err(\"l=\"+l);\n" +
						"boolean eq2;eq2=(l<>j);print_err(\"eq2=\");print_err(eq2);\n" +
						"decimal d;d=10;print_err(\"d=\"+d);\n" +
						"boolean eq3;eq3=d.ne.i;print_err(\"eq3=\");print_err(eq3);\n";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq1")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq2")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals(false,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq3")).getTLValue()==TLBooleanValue.TRUE);

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_greater_less(){
		System.out.println("\nGreater and less test:");
		String expStr = "int i; i=10;print_err(\"i=\"+i);\n" +
						"int j;j=9;print_err(\"j=\"+j);\n" +
						"boolean eq1; eq1=(i>j);print_err(\"eq1=\"+eq1);\n" +
						"long l;l=10;print_err(\"l=\"+l);\n" +
						"boolean eq2;eq2=(l>=j);print_err(\"eq2=\"+eq2);\n" +
						"decimal d;d=10;print_err(\"d=\"+d);\n" +
						"boolean eq3;eq3=d=>i;print_err(\"eq3=\"+eq3);\n" +
						"number n;n=10;print_err(\"n=\"+n);\n" +
						"boolean eq4;eq4=n.gt.l;print_err(\"eq4=\"+eq4);\n" +
						"boolean eq5;eq5=n.ge.d;print_err(\"eq5=\"+eq5);\n" +
						"string s;s='hello';print_err(\"s=\"+s);\n" +
						"string s1;s1=\"hello\";print_err(\"s1=\"+s1);\n" +
						"boolean eq6;eq6=s<s1;print_err(\"eq6=\"+eq6);\n" +
						"date mydate;mydate=2006-01-01;print_err(\"mydate=\"+mydate);\n" +
						"date anothermydate;print_err(\"anothermydate=\"+anothermydate);\n" +
						"boolean eq7;eq7=mydate.lt.anothermydate;print_err(\"eq7=\"+eq7);\n" +
						"anothermydate=2006-1-1 0:0:0;print_err(\"anothermydate=\"+anothermydate);\n" +
						"boolean eq8;eq8=mydate<=anothermydate;print_err(\"eq8=\"+eq8);\n" ;

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals("eq1",true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq1")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals("eq2",true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq2")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals("eq3",true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq3")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals("eq4",false,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq4")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals("eq5",true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq5")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals("eq6",false,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq6")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals("eq7",true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq7")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals("eq8",true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq8")).getTLValue()==TLBooleanValue.TRUE);

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_regex(){
		System.out.println("\nRegex test:");
		String expStr = "string s;s='Hej';print_err(s);\n" +
						"boolean eq2;eq2=(s~=\"[A-Za-z]{3}\");\n" +
						"print_err(\"eq2=\"+eq2);\n";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }
		     

		      
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("eq2")).getTLValue()==TLBooleanValue.TRUE);

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_1_expression() {
		String expStr="$Age>=135 or 200>$Age and not $Age<=0 and 1==999999999999999 or $Name==\"HELLO\"";
		
	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStartExpression parseTree = parser.StartExpression();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      assertEquals(true, executor.getResult()==TLBooleanValue.TRUE);
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    }
		}
	
	public void test_2_expression() {
		String expStr="int months=datediff(nvl($Born,2005-2-1),2005-1-1,month); \n"+
					  "int days=datediff(2399-01-01, 2007-01-01, day); int years=datediff(2399-01-01, 2007-01-01, year);";
	      print_code(expStr);
		try {
            TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStart parseTree = parser.Start();
              
 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
      
              assertEquals(1,executor.getGlobalVariable(parser.getGlobalVariableSlot("months")).getTLValue().getNumeric().getInt());
              assertEquals(392,executor.getGlobalVariable(parser.getGlobalVariableSlot("years")).getTLValue().getNumeric().getInt());
              assertEquals(143175,executor.getGlobalVariable(parser.getGlobalVariableSlot("days")).getTLValue().getNumeric().getInt());

              
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    }
		
	}
		
	public void test_3_expression() {
//		String expStr="not (trim($Name) .ne. \"HELLO\") || replace($Name,\".\" ,\"a\")=='aaaaaaa'";
		String expStr="print_err(trim($Name)); print_err(replace('xyyxyyxzz',\"x\" ,\"ab\")); print_err('aaaaaaa'); print_err(split('abcdef','c'));";
	      print_code(expStr);
		try {
			System.out.println("in Test3expression");
            TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));           
            
            CLVFStart parseTree = parser.Start();
//            CLVFStartExpression parseTree = parser.StartExpression();

            parseTree.dump("ccc");
              
              for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
            	  System.err.println(it.next());
              }
              
              
 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
		      
//              assertEquals(false, executor.getResult()==TLBooleanValue.TRUE);
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    }
		
	}
	
	public void test_if(){
		System.out.println("\nIf statement test:");
		String expStr = "int i; i=10;print_err(\"i=\"+i);\n" +
						"int j;j=9;print_err(\"j=\"+j);\n" +
						"long l;" +
						"if (i>j) l=1; else l=0;\n" +
						"print_err(l);\n" +
						"decimal d;" +
						"if (i.gt.j and l.eq.1) {d=0;print_err('d rovne 0');}\n" +
						"else d=0.1;\n" +
						"number n;\n" +
						"if (d==0.1) n=0;\n" +
						"if (d==0.1 || l<=1) n=0;\n" +
						"else {n=-1;print_err('n rovne -1');}\n" +
						"date date1; date1=2006-01-01;print_err(date1);\n" +
						"date date2; date2=2006-02-01;print_err(date2);\n" +
						"boolean result;result=false;\n" +
						"boolean compareDates;compareDates=date1<=date2;print_err(compareDates);\n" +
						"if (date1<=date2) \n" +
						"{  print_err('before if (i<j)');\n" +
						"	if (i<j) print_err('date1<today and i<j'); else print_err('date1<date2 only');\n" +
						"	result=true;}\n" +
						"result=false;" +
						"if (i<j) result=true;\n" +
						"else if (not result) result=true;\n" +
						"else print_err('last else');\n";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }
		     
		      assertEquals(1,executor.getGlobalVariable(parser.getGlobalVariableSlot("l")).getTLValue().getNumeric().getLong());
		      assertEquals(DecimalFactory.getDecimal(0),executor.getGlobalVariable(parser.getGlobalVariableSlot("d")).getTLValue().getNumeric());
		      assertEquals(Double.valueOf(0),executor.getGlobalVariable(parser.getGlobalVariableSlot("n")).getTLValue().getNumeric().getDouble());
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("result")).getTLValue()==TLBooleanValue.TRUE);

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
                
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_switch(){
		System.out.println("\nSwitch test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"int n;n=date2num(born,month);print_err(n);\n" +
						"string mont;\n" +
						"decimal april;april=4;\n" +
						"switch (n) {\n" +
						"	case 0.0:mont='january';\n" +
						"	case 1.0:mont='february';\n" +
						"	case 2.0:mont='march';\n" +
						"	case 3:mont='april';\n" +
						"	case april:mont='may';\n" +
						"	case 5.0:mont='june';\n" +
						"	case 6.0:mont='july';\n" +
						"	case 7.0:mont='august';\n" +
						"	case 3:print_err('4th month');\n" +
						"	case 8.0:mont='september';\n" +
						"	case 9.0:mont='october';\n" +
						"	case 10.0:mont='november';\n" +
						"	case 11.0:mont='december';\n" +
						"	default: mont='unknown';};\n"+
						"print_err('month:'+mont);\n" +
						"boolean ok;ok=(n.ge.0)and(n.lt.12);\n" +
						"switch (ok) {\n" +
						"	case true:print_err('OK');\n" +
						"	case false:print_err('WRONG');};\n" +
						"switch (born) {\n" +
						"	case 2006-01-01:{mont='January';print_err('january');}\n" +
						"	case 1973-04-23:{mont='April';print_err('april');}}\n" +
						"//	default:print_err('other')};\n"+
						"switch (born<1996-08-01) {\n" +
						"	case true:{print_err('older then ten');}\n" +
						"	default:print_err('younger then ten');};\n";
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals(3,executor.getGlobalVariable(parser.getGlobalVariableSlot("n")).getTLValue().getNumeric().getInt());
		      assertEquals("April",executor.getGlobalVariable(parser.getGlobalVariableSlot("mont")).getTLValue().toString());
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("ok")).getTLValue()==TLBooleanValue.TRUE);
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}
	
	public void test_while(){
		System.out.println("\nWhile test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"date now;now=today();\n" +
						"int yer;yer=0;\n" +
						"while (born<now) {\n" +
						"	born=dateadd(born,1,year);\n " +
						"	while (yer<5) yer=yer+1;\n" +
						"	yer=yer+1;}\n" +
						"print_err('years:'+yer);\n";
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      GregorianCalendar b,n;
		      b= new GregorianCalendar();
		      b.setTime(((Date)record.getField("Born").getValue()));
		      n = new GregorianCalendar();
		      int diff = n.get(Calendar.YEAR) - b.get(Calendar.YEAR) + 5;
		      b.set(Calendar.YEAR, 0);
		      n.set(Calendar.YEAR, 0);
		      if (n.after(b)) {diff++;}
		      assertEquals(diff, 
		    		  (executor.getGlobalVariable(parser.getGlobalVariableSlot("yer")).getTLValue().getNumeric().getInt()));
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_do_while(){
		System.out.println("\nDo-while test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"date now;now=today();\n" +
						"int yer;yer=0;\n" +
						"do {\n" +
						"	born=dateadd(born,1,year);\n " +
						"	print_err('years:'+yer);\n" +
						"	print_err(born);\n" +
						"	do yer=yer+1; while (yer<5);\n" +
						"	print_err('years:'+yer);\n" +
						"	print_err(born);\n" +
						"	yer=yer+1;}\n" +
						"while (born<now)\n" +
						"print_err('years on the end:'+yer);\n";
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      GregorianCalendar b,n;
		      b= new GregorianCalendar();
		      b.setTime(((Date)record.getField("Born").getValue()));
		      n = new GregorianCalendar();
		      int diff = n.get(Calendar.YEAR) - b.get(Calendar.YEAR);
		      b.set(Calendar.YEAR, 0);
		      n.set(Calendar.YEAR, 0);
		      if (n.after(b)) {diff++;}
		      assertEquals(2 * (diff) + 4,
		    		  (executor.getGlobalVariable(parser.getGlobalVariableSlot("yer")).getTLValue().getNumeric().getInt()));
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_for(){
		System.out.println("\nFor test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"date now;now=today();\n" +
						"int yer;yer=0;\n" +
						"for (born;born<now;born=dateadd(born,1,year)) yer++;\n" +
						"print_err('years on the end:'+yer);\n" +
						"boolean b;\n" +
						"for (born;!b;++yer) \n" +
						"	if (yer==100) b=true;\n" +
						"print_err(born);\n" +
						"print_err('years on the end:'+yer);\n" +
						"print_err('born:'+born);\n"+
						"int i;\n" +
						"for (i=0;i.le.10;++i) ;\n" +
						"print_err('on the end i='+i);\n";
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
              int iVarSlot=parser.getGlobalVariableSlot("i");
              int yerVarSlot=parser.getGlobalVariableSlot("yer");
		      TLVariable[] result = executor.stack.globalVarSlot;
		      assertEquals(101,result[yerVarSlot].getTLValue().getNumeric().getInt());
		      assertEquals(11,result[iVarSlot].getTLValue().getNumeric().getInt());
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_for2(){
		System.out.println("\nFor test:");
		String expStr ="int i;int yer;yer=0;\n" +
						"for (i=0;i.le.10;++i) ;\n" +
						"print_err('on the end i='+i);\n" +
						"int j=1;long l=123456789012345678L;\n" +
						"for (j=5;j<i;++j){\n" +
						"	l=l-i;}";
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
              int iVarSlot=parser.getGlobalVariableSlot("i");
              int jVarSlot=parser.getGlobalVariableSlot("j");
		      TLVariable[] result = executor.stack.globalVarSlot;
		      assertEquals(11,result[jVarSlot].getTLValue().getNumeric().getInt());
		      assertEquals(11,result[iVarSlot].getTLValue().getNumeric().getInt());
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_break(){
		System.out.println("\nBreak test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"date now;now=today();\n" +
						"int yer;yer=0;\n" +
						"int i;\n" +
						"while (born<now) {\n" +
						"	yer++;\n" +
						"	born=dateadd(born,1,year);\n" +
						"	for (i=0;i<20;++i) \n" +
						"		if (i==10) break;\n" +
						"}\n" +
						"print_err('years on the end:'+yer);\n"+
						"print_err('i after while:'+i);\n" ;
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      GregorianCalendar b,n;
		      b= new GregorianCalendar();
		      b.setTime(((Date)record.getField("Born").getValue()));
		      n = new GregorianCalendar();
		      int diff = n.get(Calendar.YEAR) - b.get(Calendar.YEAR);
		      b.set(Calendar.YEAR, 0);
		      n.set(Calendar.YEAR, 0);
		      if (n.after(b)) {diff++;}
		      assertEquals(diff,
		    		  (executor.getGlobalVariable(parser.getGlobalVariableSlot("yer")).getTLValue().getNumeric().getInt()));
		      assertEquals(10,(executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getInt()));
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_break2(){
		System.out.println("\nBreak test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"date now; now=today(); print_err(now);\n" +
						"int yer;yer=0;\n" +
						"int i;\n" +
						"while (yer<date2num(now,year)) {\n" +
						"	yer++;\n" +
						"	for (i=0;i<20;++i) \n" +
						"		if (i==10) break;\n" +
						"}\n" +
						"print_err('years on the end:'+yer);\n"+
						"print_err('i after while:'+i);\n" ;
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals((new GregorianCalendar()).get(Calendar.YEAR),(executor.getGlobalVariable(parser.getGlobalVariableSlot("yer")).getTLValue().getNumeric().getInt()));
		      assertEquals(10,(executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getInt()));
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_continue(){
		System.out.println("\nContinue test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"date now;now=today();\n" +
						"int yer;yer=0;\n" +
						"int i;\n" +
						"for (i=0;i<10;i=i+1) {\n" +
						"	print_err('i='+i);\n" +
						"	if (i>5) continue;\n" +
						"	print_err('After if');" +
						"}\n" +
						"print_err('new loop starting');\n" +
						"print_err('born '+born+' now '+now);\n"+
						"while (born<now) {\n" +
						"	print_err('i='+i);i=0;\n" +
						"	print_err(yer);\n" +
						"	yer=yer+1;\n" +
						"	born=dateadd(born,1,year);\n" +
						"	if (yer>30) continue\n" +
						"	for (i=0;i<20;++i) \n" +
						"		if (i==10) break;\n" +
						"}\n" +
						"print_err('years on the end:'+yer);\n"+
						"print_err('i after while:'+i);\n" ;
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      GregorianCalendar b,n;
		      b= new GregorianCalendar();
		      b.setTime(((Date)record.getField("Born").getValue()));
		      n = new GregorianCalendar();
		      int diff = n.get(Calendar.YEAR) - b.get(Calendar.YEAR);
		      b.set(Calendar.YEAR, 0);
		      n.set(Calendar.YEAR, 0);
		      if (n.after(b)) {diff++;}
		      
		      assertEquals(diff,(executor.getGlobalVariable(parser.getGlobalVariableSlot("yer")).getTLValue().getNumeric().getInt()));
		      assertEquals(0,(executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getInt()));
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_continue2(){
		System.out.println("\nContinue test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"date now;now=today();\n" +
						"int yer;yer=0;\n" +
						"int i;\n" +
						"for (i=0;i<10;i=i+1) {\n" +
						"	print_err('i='+i);\n" +
						"	if (i>5) continue;\n" +
						"	print_err('After if');" +
						"}\n" +
						"print_err('i after f:'+i);\n" ;
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      
		      assertEquals(10,(executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getInt()));
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_return(){
		System.out.println("\nReturn test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"function year_before(now) {\n" +
						"	return dateadd(now,-1,year);" +
						"}\n" +
						"function age(born){\n" +
						"	date now;int yer;\n" +
						"	now=today();yer=0;\n" +
						"	for (born;born<now;born=dateadd(born,1,year)) yer++;\n" +
						"	if (yer>0) return yer else return -1;" +
						"}\n" +
						"print_err('years born'+age(born));\n" +
						"print_err(\"year before:\"+year_before(born));\n" +
						" while (true) {print_err('pred return');" +
						"return;\n" +
						"print_err('po return');}\n" +
						"print_err('za blokem');\n";
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
	      print_code(expStr);

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
              
            for(Iterator iter=parser.getParseExceptions().iterator();iter.hasNext();){
                System.err.println(iter.next());
            }
              
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      //TODO
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	public void test_list_map(){
        System.out.println("\nList/Map test:");
        String expStr = "list seznam; list seznam2; list fields;\n"+
        				 "map mapa; mapa['f1']=10; mapa['f2']='hello'; map mapa2; mapa2[]=mapa; map mapa3; \n"+
        				 "int i; for(i=0;i<20;i++) { seznam[]=i; if (i>10) seznam2[]=seznam;  }\n"+
        				 "seznam[1]=999; seznam2[3]='hello'; \n"+
        				 "list negative=[-1,-2,-3];\n" +
        				 "print_err(negative);\n" +
        				 "for(i=0;i<3;i++) { negative[i]=negative[i]+1;}\n"+
        				 "print_err(negative);\n" +
        				 "fields=split('a,b,c,d,e,f,g,h',','); fields[]=null;"+
        				 "int length=length(seznam); print_err('length: '+length);\n print_err(seznam);\n print_err(seznam2); print_err(fields);\n"+
        				 "list novy; novy[]=mapa; print_err('novy1:'+novy); mapa2['f2']='xxx'; novy[]=mapa2; mapa['f1']=99; novy[]=mapa; \n" +
        				 "print_err('novy='+novy); print_err(novy[1]); \n" +
        				 "print_err('novy[2]:'+novy[2]); mapa3=novy[2]; print_err(mapa2['f2']); print_err(mapa3); \n" +
        				 "fields=seznam2; print_err(fields);\n" +
        				 "print_err(join(':del:',seznam,mapa,novy[1]));\n";
        print_code(expStr);

       Log logger = LogFactory.getLog(this.getClass());
       TransformLangParser parser=null;
        
        try {
              parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStart parseTree = parser.Start();

              System.out.println("Initializing parse tree..");
              parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
              System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.setRuntimeLogger(logger);
              executor.setGraph(graph);
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
              
              assertEquals("lengh",20,executor.getGlobalVariable(parser.getGlobalVariableSlot("length")).getTLValue().getNumeric().getInt());
              
              
        } catch (ParseException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
             
                Iterator it=parser.getParseExceptions().iterator();
                while(it.hasNext()){
                	System.err.println(((Throwable)it.next()).getMessage());
                }
                throw new RuntimeException("Parse exception",e);
        }
    }
    
	
	public void test_list_map2(){
        System.out.println("\nList/Map test2:");
        String expStr = "list novy=[1,2,3,4,5,6,7,8];\n" +
        				"print_err('novy: ' + novy);\n" +
        				"novy[]=[9,8,7,6,5];\n"+
        				"print_err('novy: ' + novy);\n" +
        				 "int index=1; print_err('novy with index '+index);\n"+
        				 "print_err(' content: '+novy[index]);\n";
        print_code(expStr);

       Log logger = LogFactory.getLog(this.getClass());
       TransformLangParser parser=null;
        
        try {
              parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStart parseTree = parser.Start();

              System.out.println("Initializing parse tree..");
              parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
              System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.setRuntimeLogger(logger);
              executor.setGraph(graph);
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
               
              
        } catch (ParseException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
             
                Iterator it=parser.getParseExceptions().iterator();
                while(it.hasNext()){
                	System.err.println(((Throwable)it.next()).getMessage());
                }
                throw new RuntimeException("Parse exception",e);
        }
    }
	
	
	public void test_buildInFunctions(){
		System.out.println("\nBuild-in functions test:");
		String expStr = 
			"string s;s='hello world';\n" +
						"number lenght;lenght=5.5;\n" +
						"string subs;subs=substring(s,1,lenght);\n" +
						"print_err('original string:'+s );\n" +
						"print_err('substring:'+subs );\n" +
						"string upper;upper=uppercase(subs);\n" +
						"print_err('to upper case:'+upper );\n"+
						"string lower;lower=lowercase(subs+'hI   ');\n" +
						"print_err('to lower case:'+lower );\n"+
						"string t;t=trim('\t  im  '+lower);\n" +
						"print_err('after trim:'+t );\n" +
						"breakpoint();\n" +
						"//print_stack();\n"+
						"decimal l;l=length(upper);\n" +
						"print_err('length of '+upper+':'+l );\n"+
						"string c;c=concat(lower,upper,2,',today is ',today());\n" +
						"print_err('concatenation \"'+lower+'\"+\"'+upper+'\"+2+\",today is \"+today():'+c );\n"+
						"date datum; date born;born=nvl($Born,today()-365);\n" +
						"print_err('born=' + born);\n" +
						"datum=dateadd(born,100,millisec);\n" +
						"print_err('dataum = ' + datum );\n"+
						"long ddiff;date otherdate;otherdate=today();\n" +
						"ddiff=datediff(born,otherdate,year);\n" +
						"print_err('date diffrence:'+ddiff );\n" +
						"print_err('born: '+born+' otherdate: '+otherdate);\n" +
						"boolean isn;isn=isnull(ddiff);\n" +
						"print_err(isn );\n" +
						"number s1;s1=nvl(l+1,1);\n" +
						"print_err(s1 );\n" +
						"string rep;rep=replace(c,'[lL]','t');\n" +
						"print_err(rep );\n" +
						"decimal(10,5) stn;stn=str2num('2.5125e-1',decimal);\n" +
						"print_err(stn );\n" +
						"int i = str2num('1234');\n" +
						"string nts;nts=num2str(10,4);\n" +
						"print_err(nts );\n" +
						"string currstr; currstr = num2str(1234.56,'\u00A4#,###.##','en.US');\n" +
						"print_err('currency: ' + currstr);\n" +
						"date newdate;newdate=2001-12-20 16:30:04;\n" +
						"decimal dtn;dtn=date2num(newdate,month);\n" +
						"print_err(dtn );\n" +
						"int ii;ii=iif(newdate<2000-01-01,20,21);\n" +
						"print_err('ii:'+ii);\n" +
						"print_stack();\n" +
						"date ndate;ndate=2002-12-24;\n" +
						"string dts;dts=date2str(ndate,'yy.MM.dd');\n" +
						"print_err('date to string:'+dts);\n" +
						"print_err(str2date(dts,'yy.MM.dd'));\n" +
						"string lef=left(dts,5);\n" +
						"string righ=right(dts,5);\n" +
						"print_err('s=word, soundex='+soundex('word'));\n" +
						"print_err('s=world, soundex='+soundex('world'));\n" +
						"int j;for (j=0;j<length(s);j++){print_err(char_at(s,j));};\n" +
						"int charCount = count_char('mimimichal','i');\n" +
						"print_err(charCount);\n" +
						"int debug_print = 1;\n" +
						"print_err('debug print is on: ',debug_print == 1);\n" + // second argument can be any boolean expression
						"print_err('debug print is off: ',debug_print == 0);\n" +
						"print_err('debug print is off by default:');\n";  // do not print position by default

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }


 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals("subs","ello ",executor.getGlobalVariable(parser.getGlobalVariableSlot("subs")).getTLValue().toString());
		      assertEquals("upper","ELLO ",executor.getGlobalVariable(parser.getGlobalVariableSlot("upper")).getTLValue().toString());
		      assertEquals("lower","ello hi   ",executor.getGlobalVariable(parser.getGlobalVariableSlot("lower")).getTLValue().toString());
		      assertEquals("t(=trim)","im  ello hi",executor.getGlobalVariable(parser.getGlobalVariableSlot("t")).getTLValue().toString());
		      assertEquals("l(=length)",5,executor.getGlobalVariable(parser.getGlobalVariableSlot("l")).getTLValue().getNumeric().getInt());
		      assertEquals("c(=concat)","ello hi   ELLO 2,today is "+new Date(),executor.getGlobalVariable(parser.getGlobalVariableSlot("c")).getTLValue().toString());
//		      assertEquals("datum",record.getField("Born").getValue(),executor.getGlobalVariable(parser.getGlobalVariableSlot("datum")).getValue().getDate());
		      assertEquals("ddiff",-1,executor.getGlobalVariable(parser.getGlobalVariableSlot("ddiff")).getTLValue().getNumeric().getLong());
		      assertEquals("isn",false,executor.getGlobalVariable(parser.getGlobalVariableSlot("isn")).getTLValue()==TLBooleanValue.TRUE);
		      assertEquals("s1",Double.valueOf(6),executor.getGlobalVariable(parser.getGlobalVariableSlot("s1")).getTLValue().getNumeric().getDouble());
		      assertEquals("rep",("etto hi   EttO 2,today is "+new Date()).replaceAll("[lL]", "t"),executor.getGlobalVariable(parser.getGlobalVariableSlot("rep")).getTLValue().toString());
		      assertEquals("stn",0.25125,executor.getGlobalVariable(parser.getGlobalVariableSlot("stn")).getTLValue().getNumeric().getDouble());
		      assertEquals("i",1234,executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getInt());
		      assertEquals("nts","22",executor.getGlobalVariable(parser.getGlobalVariableSlot("nts")).getTLValue().toString());
		      assertEquals("currency","$1,234.56",executor.getGlobalVariable(parser.getGlobalVariableSlot("currstr")).getTLValue().toString());
		      assertEquals("dtn",11.0,executor.getGlobalVariable(parser.getGlobalVariableSlot("dtn")).getTLValue().getNumeric().getDouble());
		      assertEquals("ii",21,executor.getGlobalVariable(parser.getGlobalVariableSlot("ii")).getTLValue().getNumeric().getInt());
		      assertEquals("dts","02.12.24",executor.getGlobalVariable(parser.getGlobalVariableSlot("dts")).getTLValue().toString());
		      assertEquals("lef","02.12",executor.getGlobalVariable(parser.getGlobalVariableSlot("lef")).getTLValue().toString());
		      assertEquals("righ","12.24",executor.getGlobalVariable(parser.getGlobalVariableSlot("righ")).getTLValue().toString());
		      assertEquals("charCount",3,executor.getGlobalVariable(parser.getGlobalVariableSlot("charCount")).getTLValue().getNumeric().getInt());
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

    public void test_functions2(){
        System.out.println("\nFunctions test:");
        String expStr = "string test='test';\n" +
        		"boolean isBlank=is_blank(test);\n" +
        		"boolean isBlank1=is_blank('');\n" +
        		"test=null; boolean isBlank2=is_blank(test);\n" +
        		"boolean isAscii1=is_ascii('test');\n" +
        		"boolean isAscii2=is_ascii('aęř');\n" +
        		"boolean isNumber=is_number('t1');\n" +
        		"boolean isNumber1=is_number('1g');\n" +
        		"boolean isNumber2=is_number('1'); print_err(str2num('1'));\n" +
        		"boolean isNumber3=is_number('-382.334'); print_err(str2num('-382.334',decimal));\n" +
        		"boolean isNumber4=is_number('+332e2');\n" +
        		"boolean isNumber5=is_number('8982.8992e-2');print_err(str2num('8982.8992e-2',double));\n" +
        		"boolean isNumber6=is_number('-7888873.2E3');print_err(str2num('-7888873.2E3',number));\n" +
        		"boolean isInteger=is_integer('h3');\n" +
        		"boolean isInteger1=is_integer('78gd');\n" +
        		"boolean isInteger2=is_integer('8982.8992');\n" +
        		"boolean isInteger3=is_integer('-766542378');print_err(str2num('-766542378'));\n" +
        		"boolean isLong=is_long('7864232568822234');\n" +
        		"boolean isDate5=is_date('20Jul2000','ddMMMyyyy','en.GB');print_err(str2date('20Jul2000','ddMMMyyyy','en.GB'));\n" +
        		"boolean isDate6=is_date('20July    2000','java:ddMMMMMMMMyyyy','en.GB');print_err(str2date('20July    2000','java:ddMMMMMMMMyyyy','en.GB'));\n" +
        		"boolean isDate3=is_date('4:42','HH:mm');print_err(str2date('4:42','HH:mm'));\n" +
        		"boolean isDate=is_date('20.11.2007','dd.MM.yyyy');print_err(str2date('20.11.2007','dd.MM.yyyy'));\n" +
        		"boolean isDate1=is_date('20.11.2007','dd-MM-yyyy');\n" +
        		"boolean isDate2=is_date('24:00 20.11.2007','kk:mm dd.MM.yyyy');print_err(str2date('24:00 20.11.2007','kk:mm dd.MM.yyyy'));\n" +
        		"boolean isDate4=is_date('test 20.11.2007','hhmm dd.MM.yyyy');\n" +
        		"boolean isDate7=is_date('                ','HH:mm dd.MM.yyyy',true);\n"+
        		"boolean isDate8=is_date('                ','HH:mm dd.MM.yyyy');\n"+
        		"boolean isDate9=is_date('20-15-2007','dd-MM-yyyy');\n" +
        		"if (isDate9)print_err(str2date('20-15-2007','dd-MM-yyyy'));\n" +
        		"boolean isDate10=is_date('20-15-2007','dd-MM-yyyy');\n" +
        		"boolean isDate13=is_date('942-12-1996','dd-MM-yyyy','en.US');\n" +
        		"boolean isDate14=is_date('12-prosinec-1996','dd-MMMMM-yyyy','cs.CZ');\n" +
        		"boolean isDate15=is_date('12-prosinec-1996','dd-MMMMM-yyyy','en.US');\n" + 
        		"boolean isDate16=is_date('24:00 20.11.2007','HH:mm dd.MM.yyyy');\n" +
        		"boolean isDate17=is_date('','HH:mm dd.MM.yyyy');\n";
        
        print_code(expStr);

       Log logger = LogFactory.getLog(this.getClass());
       
        
        try {
              TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStart parseTree = parser.Start();

              System.out.println("Initializing parse tree..");
              parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
              System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.setRuntimeLogger(logger);
              executor.setGraph(graph);
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
              
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isBlank")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isBlank1")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isBlank2")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isAscii1")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isAscii2")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isNumber")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isNumber1")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isNumber2")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isNumber3")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isNumber4")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isNumber5")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isNumber6")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isInteger")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isInteger1")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isInteger2")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isInteger3")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isLong")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate1")).getTLValue()==TLBooleanValue.TRUE));
		      // "kk" allows hour to be 1-24 (as opposed to HH allowing hour to be 0-23) 
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate2")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate3")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate4")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate5")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate6")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate7")).getTLValue()==TLBooleanValue.TRUE));
		      // illegal month: 15
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate9")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate10")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate13")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(true,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate14")).getTLValue()==TLBooleanValue.TRUE));
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate15")).getTLValue()==TLBooleanValue.TRUE));
		      // 24 is an illegal value for pattern HH (it allows only 0-23)
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate16")).getTLValue()==TLBooleanValue.TRUE));
		      // empty string in strict mode: invalid
		      assertEquals(false,(executor.getGlobalVariable(parser.getGlobalVariableSlot("isDate17")).getTLValue()==TLBooleanValue.TRUE));
		      
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Parse exception",e);
        }
              
    }

    public void test_functions3(){
        System.out.println("\nFunctions test:");
        String expStr = "string test=remove_diacritic('teścik');\n" +
        				"string test1=remove_diacritic('žabička');\n" +
        				"string r1=remove_blank_space(\"" + 
        				StringUtils.specCharToString(" a	b\nc\rd   e \u000Cf\r\n") + 
        				"\");\n" +
        				"string an1 = get_alphanumeric_chars(\"" +
           				StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + 
        				"\");\n" +
           				"string an2 = get_alphanumeric_chars(\"" +
           				StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + 
        				"\",true,true);\n" +
           				"string an3 = get_alphanumeric_chars(\"" +
           				StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + 
        				"\",true,false);\n" +
           				"string an4 = get_alphanumeric_chars(\"" +
           				StringUtils.specCharToString(" a	1b\nc\rd \b  e \u000C2f\r\n") + 
           				"\",false,true);\n" +
           				"string an5 = remove_nonprintable(\"" +
           				new StringBuffer("separator").append((char)0x07).append("is").append((char)0x1B).append("special").append((char)0x0C).append("character").toString() + 
           				"\");\n" +
           				"string an6 = remove_nonascii(\"" +
           				new StringBuffer("separator").append((char)0xA9).append("is").append((char)0xB4).append("nonascii").append((char)0xC2).append("character").toString() + 
           				"\");\n" +
           				"string t=translate('hello','leo','pii');\n" +
        				"string t1=translate('hello','leo','pi');\n" +
        				"string t2=translate('hello','leo','piims');\n" +
        				"string t3=translate('hello','leo',null); print_err(t3);\n" +
        				"string t4=translate('my language needs the letter e', 'egms', 'X');\n" +
        				"string input='hello world';\n" +
        				"int index=index_of(input,'l');\n" +
        				"int index1=index_of(input,'l',5);\n" +
        				"int index2=index_of(input,'hello');\n" +
        				"int index3=index_of(input,'hello',1);\n" +
        				"int index4=index_of(input,'world',1);\n";
        print_code(expStr);

       Log logger = LogFactory.getLog(this.getClass());
       
        
        try {
              TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStart parseTree = parser.Start();

              System.out.println("Initializing parse tree..");
              parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
              System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.setRuntimeLogger(logger);
              executor.setGraph(graph);
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
              
		  //    assertEquals("tescik",(executor.getGlobalVariable(parser.getGlobalVariableSlot("test")).getTLValue().toString()));
		  //    assertEquals("zabicka",(executor.getGlobalVariable(parser.getGlobalVariableSlot("test1")).getTLValue().toString()));
		      assertEquals("abcdef",(executor.getGlobalVariable(parser.getGlobalVariableSlot("r1")).getTLValue().toString()));
		      assertEquals("a1bcde2f",(executor.getGlobalVariable(parser.getGlobalVariableSlot("an1")).getTLValue().toString()));
		      assertEquals("a1bcde2f",(executor.getGlobalVariable(parser.getGlobalVariableSlot("an2")).getTLValue().toString()));
		      assertEquals("abcdef",(executor.getGlobalVariable(parser.getGlobalVariableSlot("an3")).getTLValue().toString()));
		      assertEquals("12",(executor.getGlobalVariable(parser.getGlobalVariableSlot("an4")).getTLValue().toString()));
		      assertEquals("separatorisspecialcharacter",(executor.getGlobalVariable(parser.getGlobalVariableSlot("an5")).getTLValue().toString()));
		      assertEquals("separatorisnonasciicharacter",(executor.getGlobalVariable(parser.getGlobalVariableSlot("an6")).getTLValue().toString()));
		      assertEquals("hippi",(executor.getGlobalVariable(parser.getGlobalVariableSlot("t")).getTLValue().toString()));
		      assertEquals("hipp",(executor.getGlobalVariable(parser.getGlobalVariableSlot("t1")).getTLValue().toString()));
		      assertEquals("hippi",(executor.getGlobalVariable(parser.getGlobalVariableSlot("t2")).getTLValue().toString()));
		    //  assertTrue((executor.getGlobalVariable(parser.getGlobalVariableSlot("t3")).isNULL()));
		      assertEquals("y lanuaX nXXd thX lXttXr X",(executor.getGlobalVariable(parser.getGlobalVariableSlot("t4")).getTLValue().toString()));
		      assertEquals(2,(executor.getGlobalVariable(parser.getGlobalVariableSlot("index")).getTLValue().getNumeric().getInt()));
		      assertEquals(9,(executor.getGlobalVariable(parser.getGlobalVariableSlot("index1")).getTLValue().getNumeric().getInt()));
		      assertEquals(0,(executor.getGlobalVariable(parser.getGlobalVariableSlot("index2")).getTLValue().getNumeric().getInt()));
		      assertEquals(-1,(executor.getGlobalVariable(parser.getGlobalVariableSlot("index3")).getTLValue().getNumeric().getInt()));
		      assertEquals(6,(executor.getGlobalVariable(parser.getGlobalVariableSlot("index4")).getTLValue().getNumeric().getInt()));

        } catch (ParseException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Parse exception",e);
        }
              
    }

    public void test_functions4(){
        System.out.println("\nFunctions test:");
        DecimalFormat format = (DecimalFormat)NumberFormat.getInstance(MiscUtils.createLocale("en.US"));
        String expStr = 
	    		"string stringNo='12';\n" +
	    		"int No;\n" +
        		"No = try_convert(stringNo,int);\n"+
        		"stringNo='128a';\n" +
        		"int intNo;\n" + 
        		"intNo = try_convert(stringNo,int);\n" +
        		"stringNo='" + format.format(1285.455) + "';\n" +
        		"double no1=1.34;\n" +
        		"no1 = try_convert(stringNo,double,'" + format.toPattern() + "','en.US');\n"  +
        		"decimal(10,3) no21;\n" +
        		"decimal(10,3) no22;\n" +
        		"no21= try_convert(no1,decimal);\n" +
        		"no22 = try_convert(34542.3,decimal);\n" +
        		"int no31;\n" +
        		"int no32;\n" +
        		"int no33;\n" +
        		"no31 = try_convert(34542.7,int);\n" +
        		"no32 = try_convert(345427,int);\n" +
        		"no33 = try_convert(3454876434468927,int);\n" +
        		"date date1 = $Born;\n" +
        		"long no4;\n" +
        		"no4 = try_convert(date1,long,null);\n" +
        		"no4 = no4 + 1000*60*60*24;\n" +
        		"date1 = try_convert(no4,date,null);\n" +
        		"date date2;\n" +
        		"date2 = try_convert('20.9.2007',date,'dd.MM.yyyy');\n" +
        		"decimal(6,4) d1=73.8474;\n" +
        		"decimal(4,2) d2;\n" +
        		"d2 = try_convert(d1,decimal);\n" +
        		"d2 = 75.32;\n" +
        		"d1 = try_convert(d2,decimal);\n" +
        		"boolean b=true;\n" +
        		"d2 = try_convert(b, decimal);\n" +
        		"string curr; curr = try_convert(1234.56,string,'\u00A4#,###.##','en.US');\n" +
        		"print_err(curr);\n" + 
        		"string ns;\n" +
        		"ns = try_convert(1247,string);"+
        		"boolean bol=num2bool(1);";
        print_code(expStr);

       Log logger = LogFactory.getLog(this.getClass());
       
        
        try {
              TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
              CLVFStart parseTree = parser.Start();

              System.out.println("Initializing parse tree..");
              parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
              System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record1});
              executor.setRuntimeLogger(logger);
              executor.setGraph(graph);
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
              
		      assertEquals(12,(executor.getGlobalVariable(parser.getGlobalVariableSlot("No")).getTLValue().getNumeric().getInt()));
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("intNo")).getTLValue() == TLNullValue.getInstance());
		      assertEquals(1285.455,(executor.getGlobalVariable(parser.getGlobalVariableSlot("no1")).getTLValue().getNumeric().getDouble()));
		      assertEquals(DecimalFactory.getDecimal(1285.450, 10, 3),(executor.getGlobalVariable(parser.getGlobalVariableSlot("no21")).getTLValue().getNumeric()));
		      assertEquals(DecimalFactory.getDecimal(34542.3, 10, 3),(executor.getGlobalVariable(parser.getGlobalVariableSlot("no22")).getTLValue().getNumeric()));
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("no31")).getTLValue() == TLNullValue.getInstance());
		      assertEquals(345427,(executor.getGlobalVariable(parser.getGlobalVariableSlot("no32")).getTLValue().getNumeric().getInt()));
		      assertEquals(true,executor.getGlobalVariable(parser.getGlobalVariableSlot("no33")).getTLValue() == TLNullValue.getInstance());
		      today.add(Calendar.DATE, 1);
		      assertEquals(today.getTime(),(executor.getGlobalVariable(parser.getGlobalVariableSlot("date1")).getTLValue().getDate()));
		      assertEquals(today.getTimeInMillis(),(executor.getGlobalVariable(parser.getGlobalVariableSlot("no4")).getTLValue().getNumeric().getLong()));
		      assertEquals(new GregorianCalendar(2007,8,20).getTime(),(executor.getGlobalVariable(parser.getGlobalVariableSlot("date2")).getTLValue().getDate()));
		      assertEquals(DecimalFactory.getDecimal(75.32, 6, 4),(executor.getGlobalVariable(parser.getGlobalVariableSlot("d1")).getTLValue().getNumeric()));
		      assertEquals(DecimalFactory.getDecimal(75.32, 6, 4),(executor.getGlobalVariable(parser.getGlobalVariableSlot("d1")).getTLValue().getNumeric()));
		      assertEquals(DecimalFactory.getDecimal(1), executor.getGlobalVariable(parser.getGlobalVariableSlot("d2")).getTLValue().getNumeric());
		      assertEquals("$1,234.56", executor.getGlobalVariable(parser.getGlobalVariableSlot("curr")).getTLValue().toString());
		      assertEquals("1247", executor.getGlobalVariable(parser.getGlobalVariableSlot("ns")).getTLValue().toString());
		      
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Parse exception",e);
        }
              
    }

    public void test_string_functions(){
		System.out.println("\nString functions test:");
		final String expStr = 
						"string s1=chop(\"hello\\n\");\n" +
						"string s2=chop(\"hello\\r\\n\");\n" +
						"string s3=chop(\"hello world\",'world');\n" +
						"string s4=chop(\"hello world\",' world');\n" +
						"string s5=chop(\"hello\\n\\n\");\n" +
						"string s6=chop(\"hello\\r\");\n" +
						"string s7=chop(\"hello\\nworld\\r\\n\");\n" +
						"string s8=chop(\"hello world\",'wo');\n" +
						"string s9=chop(\"\");\n" + // empty string
						"string s10=chop(\"\\n\");\n" + // UNIX
						"string s11=chop(\"\\r\");\n" + // MAC
						"string s12=chop(\"\\r\\n\");\n" +  // WINDOWS
						"string s13=chop(\"\",'wo');\n" + 
						"string s14=chop(\"hello world\",'hello world');\n" +
						"string s15=chop(\"hello\",'hello world');\n" +
						"int dist = edit_distance('agata','ahata');\n"+
						"int dist1 = edit_distance('agata','agatą');\n"+ 
						"int dist2 = edit_distance('agata','agatą'," + StringAproxComparator.SECONDARY + ");\n"+ 
						"int dist5 = edit_distance('agata','agatą'," + StringAproxComparator.SECONDARY + ",'CZ.cz');\n"+ 
						"int dist3 = edit_distance('agata','agatą'," + StringAproxComparator.TERTIARY + ");\n"+
						"int dist4 = edit_distance('agata','Agata'," + StringAproxComparator.TERTIARY + ");\n"+
						"int dist6 = edit_distance('hello','vitej'," + StringAproxComparator.TERTIARY + ");\n"+
						"int dist7 = edit_distance('hello','vitej'," + StringAproxComparator.TERTIARY + ",10);\n"+ 
						"int dist8 = edit_distance('aAeEiIoOuUÚnN','áÁéÉíÍóÓúüÜñÑ'," + StringAproxComparator.SECONDARY + ",'ES.es');\n"+
						"int dist9 = edit_distance('aAAaaeeeeEEEEcC','àâAÀÂéêëEÈÉÊËçÇ'," + StringAproxComparator.SECONDARY + ",'FR.fr');\n";
   	
	      print_code(expStr);
			try {
				  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
			      CLVFStart parseTree = parser.Start();

	 		      System.out.println("Initializing parse tree..");
			      parseTree.init();
			      System.out.println("Parse tree:");
			      parseTree.dump("");
			      
			      System.out.println("Interpreting parse tree..");
			      TransformLangExecutor executor=new TransformLangExecutor();
			      executor.setInputRecords(new DataRecord[] {record});
			      executor.visit(parseTree,null);
			      System.out.println("Finished interpreting.");

			      if (parser.getParseExceptions().size()>0){
			    	  //report error
			    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
				    	  System.out.println(it.next());
				      }
			    	  throw new RuntimeException("Parse exception");
			      }

			      
			      assertEquals("s1","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s1")).getTLValue().getValue().toString());
			      assertEquals("s6","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s6")).getTLValue().getValue().toString());
			      assertEquals("s5","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s5")).getTLValue().getValue().toString());
			      assertEquals("s2","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s2")).getTLValue().getValue().toString());
			      assertEquals("s7","hello\nworld",executor.getGlobalVariable(parser.getGlobalVariableSlot("s7")).getTLValue().getValue().toString());
			      assertEquals("s3","hello ",executor.getGlobalVariable(parser.getGlobalVariableSlot("s3")).getTLValue().getValue().toString());
			      assertEquals("s4","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s4")).getTLValue().getValue().toString());
			      assertEquals("s8","hello world",executor.getGlobalVariable(parser.getGlobalVariableSlot("s8")).getTLValue().getValue().toString());
			      assertEquals("s9","",executor.getGlobalVariable(parser.getGlobalVariableSlot("s9")).getTLValue().getValue().toString());
			      assertEquals("s10","",executor.getGlobalVariable(parser.getGlobalVariableSlot("s10")).getTLValue().getValue().toString());
			      assertEquals("s11","",executor.getGlobalVariable(parser.getGlobalVariableSlot("s11")).getTLValue().getValue().toString());
			      assertEquals("s12","",executor.getGlobalVariable(parser.getGlobalVariableSlot("s12")).getTLValue().getValue().toString());
			      assertEquals("s13","",executor.getGlobalVariable(parser.getGlobalVariableSlot("s13")).getTLValue().getValue().toString());
			      assertEquals("s14","",executor.getGlobalVariable(parser.getGlobalVariableSlot("s14")).getTLValue().getValue().toString());
			      assertEquals("s15","hello",executor.getGlobalVariable(parser.getGlobalVariableSlot("s15")).getTLValue().getValue().toString());
			      assertEquals("dist",1,executor.getGlobalVariable(parser.getGlobalVariableSlot("dist")).getTLValue().getNumeric().getInt());
			      assertEquals("dist1",1,executor.getGlobalVariable(parser.getGlobalVariableSlot("dist1")).getTLValue().getNumeric().getInt());
			      assertEquals("dist2",0,executor.getGlobalVariable(parser.getGlobalVariableSlot("dist2")).getTLValue().getNumeric().getInt());
			      assertEquals("dist5",1,executor.getGlobalVariable(parser.getGlobalVariableSlot("dist5")).getTLValue().getNumeric().getInt());
			      assertEquals("dist3",1,executor.getGlobalVariable(parser.getGlobalVariableSlot("dist3")).getTLValue().getNumeric().getInt());
			      assertEquals("dist4",0,executor.getGlobalVariable(parser.getGlobalVariableSlot("dist4")).getTLValue().getNumeric().getInt());
			      assertEquals("dist6",4,executor.getGlobalVariable(parser.getGlobalVariableSlot("dist6")).getTLValue().getNumeric().getInt());
			      assertEquals("dist7",5,executor.getGlobalVariable(parser.getGlobalVariableSlot("dist7")).getTLValue().getNumeric().getInt());
			      assertEquals("dist8",0,executor.getGlobalVariable(parser.getGlobalVariableSlot("dist8")).getTLValue().getNumeric().getInt());
			      assertEquals("dist9",0,executor.getGlobalVariable(parser.getGlobalVariableSlot("dist9")).getTLValue().getNumeric().getInt());
			} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
    }
    
    public void test_math_functions(){
		System.out.println("\nMath functions test:");
		String expStr = "number original;original=pi();\n" +
						"print_err('pi='+original);\n" +
						"number ee=e();\n" +
						"number result;result=sqrt(original);\n" +
						"print_err('sqrt='+result);\n" +
						"int i;i=9;\n" +
						"number p9;p9=sqrt(i);\n" +
						"number ln;ln=log(p9);\n" +
						"print_err('sqrt(-1)='+sqrt(-1));\n" +
						"decimal d;d=0;"+
						"print_err('log(0)='+log(d));\n" +
						"number l10;l10=log10(p9);\n" +
						"number ex;ex =exp(l10);\n" +
						"number po;po=pow(p9,1.2);\n" +
						"number p;p=pow(-10,-0.3);\n" +
						"print_err('power(-10,-0.3)='+p);\n" +
						"int r;r=round(-po);\n" +
						"print_err('round of '+(-po)+'='+r);"+
						"int t;t=trunc(-po);\n" +
						"print_err('truncation of '+(-po)+'='+t);\n" +
						"date date1;date1=2004-01-02 17:13:20;\n" +
						"date tdate1; tdate1=trunc(date1);\n" +
						"print_err('truncation of '+date1+'='+tdate1);\n" +
						"print_err('Random number: '+random());\n";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }

		      
		      assertEquals("pi",Double.valueOf(Math.PI),executor.getGlobalVariable(parser.getGlobalVariableSlot("original")).getTLValue().getNumeric().getDouble());
		      assertEquals("e",Double.valueOf(Math.E),executor.getGlobalVariable(parser.getGlobalVariableSlot("ee")).getTLValue().getNumeric().getDouble());
		      assertEquals("sqrt",Double.valueOf(Math.sqrt(Math.PI)),executor.getGlobalVariable(parser.getGlobalVariableSlot("result")).getTLValue().getNumeric().getDouble());
		      assertEquals("sqrt(9)",Double.valueOf(3),executor.getGlobalVariable(parser.getGlobalVariableSlot("p9")).getTLValue().getNumeric().getDouble());
		      assertEquals("ln",Double.valueOf(Math.log(3)),executor.getGlobalVariable(parser.getGlobalVariableSlot("ln")).getTLValue().getNumeric().getDouble());
		      assertEquals("log10",Double.valueOf(Math.log10(3)),executor.getGlobalVariable(parser.getGlobalVariableSlot("l10")).getTLValue().getNumeric().getDouble());
		      assertEquals("exp",Double.valueOf(Math.exp(Math.log10(3))),executor.getGlobalVariable(parser.getGlobalVariableSlot("ex")).getTLValue().getNumeric().getDouble());
		      assertEquals("power",Double.valueOf(Math.pow(3,1.2)),executor.getGlobalVariable(parser.getGlobalVariableSlot("po")).getTLValue().getNumeric().getDouble());
		      assertEquals("power--",Double.valueOf(Math.pow(-10,-0.3)),executor.getGlobalVariable(parser.getGlobalVariableSlot("p")).getTLValue().getNumeric().getDouble());
		      assertEquals("round",Integer.parseInt("-4"),executor.getGlobalVariable(parser.getGlobalVariableSlot("r")).getTLValue().getNumeric().getInt());
		      assertEquals("truncation",Integer.parseInt("-3"),executor.getGlobalVariable(parser.getGlobalVariableSlot("t")).getTLValue().getNumeric().getInt());
		      assertEquals("date truncation",new GregorianCalendar(2004,00,02).getTime(),executor.getGlobalVariable(parser.getGlobalVariableSlot("tdate1")).getTLValue().getDate());
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

//	public void test_global_parameters(){
//		System.out.println("\nGlobal parameters test:");
//		String expStr = "string original;original=${G1};\n" +
//						"int num; num=str2num(original); \n"+
//						"print_err(original);\n"+
//						"print_err(num);\n";
//
//	      print_code(expStr);
//		try {
//			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
//			  		new ByteArrayInputStream(expStr.getBytes()));
//		      CLVFStart parseTree = parser.Start();
//
// 		      System.out.println("Initializing parse tree..");
//		      parseTree.init();
//		      System.out.println("Parse tree:");
//		      parseTree.dump("");
//		      
//		      System.out.println("Interpreting parse tree..");
//		      TransformLangExecutor executor=new TransformLangExecutor();
//		      executor.setInputRecords(new DataRecord[] {record});
//		      Properties globalParameters = new Properties();
//		      globalParameters.setProperty("G1","10");
//		      executor.setGlobalParameters(globalParameters);
//		      executor.visit(parseTree,null);
//		      System.out.println("Finished interpreting.");
//		      
//		      assertEquals("num",10,executor.getGlobalVariable(parser.getGlobalVariableSlot("num")).getValue().getNumeric().getInt());
//		      
//		} catch (ParseException e) {
//		    	System.err.println(e.getMessage());
//		    	e.printStackTrace();
//		    	throw new RuntimeException("Parse exception",e);
//	    }
//	}
//
	public void test_mapping(){
		System.out.println("\nMapping test:");
		String expStr = "print_err($1.City); "+
						"function test(){\n" +
						"	string result;\n" +
						"	print_err('function');\n" +
						"	result='result';\n" +
						"	//return result;\n" +
						"	$Name:=result;\n" +
						"	$0.Age:=$Age;\n" +
						"	$out.City:=concat(\"My City \",$City);\n" +
						"	$Born:=$1.Born;\n" +
						"	$0.Value:=nvl(0,$in1.Value);\n" +
						"	}\n" +
						"test();\n" +
						"print_err('Age='+ $0.Age);\n "+
						"if (isnull($0.Age)) {print_err('!!!! Age is null!!!');}\n" +
						"print_err($1.City); "+
						"//print_err($out.City); " +
						"print_err($1.City); "+
						"$1.Name:=test();\n" +
						"$out1.Age:=$Age;\n" +
						"$1.City:=$1.City;\n" +
						"$out1.Value:=$in.Value;\n";

	      print_code(expStr);
		try {
		      DataRecordMetadata[] recordMetadata=new DataRecordMetadata[] {metadata,metadata1};
		      DataRecordMetadata[] outMetadata=new DataRecordMetadata[] {metaOut,metaOut1};
			  TransformLangParser parser = new TransformLangParser(recordMetadata,
			  		outMetadata,new ByteArrayInputStream(expStr.getBytes()),"UTF-8");
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record,record1});
		      executor.setOutputRecords(new DataRecord[]{out,out1});
		      SetVal.setString(record1,2,"Prague");
		      record.getField("Age").setNull(true);
		      
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals("result",out.getField("Name").getValue().toString());
		      assertEquals(record.getField("Age").getValue(),out.getField("Age").getValue());
		      assertEquals("My City "+record.getField("City").getValue().toString(), out.getField("City").getValue().toString());
		      assertEquals(record1.getField("Born").getValue(), out.getField("Born").getValue());
		      assertEquals(0,out.getField("Value").getValue());
		      assertEquals("",out1.getField("Name").getValue().toString());
		      assertEquals(record.getField("Age").getValue(), out1.getField("Age").getValue());
		      assertEquals(record1.getField("City").getValue().toString(), out1.getField("City").getValue().toString());
		      assertNull(out1.getField("Born").getValue());
		      assertEquals(record.getField("Value").getValue(), out1.getField("Value").getValue());
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}
    
	/*
	 * TODO(issue #1433) replace this with a valid test case for wildcard
	 * mapping when activated (presum v2.7)
	 */
    
    public void test_logger(){
        System.out.println("\nLogger test:");
        String expStr = "/*raise_error(\"my testing error\") ;*/ " +
        				"print_log(fatal,10 * 15);";
        print_code(expStr);

       Log logger = LogFactory.getLog(this.getClass());
        
        try {
              TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStart parseTree = parser.Start();

              System.out.println("Initializing parse tree..");
              parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
              System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.setRuntimeLogger(logger);
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
              
        } catch (ParseException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Parse exception",e);
        }
    }

    public void test_sequence(){
        System.out.println("\nSequence test:");
        String expStr = "print_err(sequence(test).next);\n"+
                        "print_err(sequence(test).next);\n"+
                        "int i; for(i=0;i<10;++i) print_err(sequence(test).next);\n"+
                        "i=sequence(test).current; print_err(i,true); i=sequence(test).reset; \n" +
                        "print_err('i after reset='+i);\n" +
                        "int current;string next;"+
                        "for(i=0;i<50;++i) { current=sequence(test).current;\n" +
                        "print_err('current='+current);\n" +
                        "next=sequence(test).next;\n" +
                        " print_err('next='+next); }\n";
        print_code(expStr);

       Log logger = LogFactory.getLog(this.getClass());
        
        try {
              TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStart parseTree = parser.Start();

              System.out.println("Initializing parse tree..");
              parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
             System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.setRuntimeLogger(logger);
              executor.setGraph(graph);
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
              
        } catch (ParseException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Parse exception",e);
        }
    }

    public void test_lookup(){
        System.out.println("\nLookup test:");
        String expStr = "print_err(lookup(LKP,'  HELLO ').Age); \n"+
                        "print_err(lookup_found(LKP));\n"+
                        "print_err(lookup_next(LKP).Age);\n"+
                        "print_err(lookup(LKP,'xxxx').Name);\n";
        print_code(expStr);

       Log logger = LogFactory.getLog(this.getClass());
       
        
        try {
              TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStart parseTree = parser.Start();

              System.out.println("Initializing parse tree..");
              parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
              System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.setRuntimeLogger(logger);
              executor.setGraph(graph);
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
              
        } catch (ParseException e) {
                System.err.println(e.getMessage());
                e.printStackTrace();
                throw new RuntimeException("Parse exception",e);
        }
    }
    
    
    public void test_function(){
        System.out.println("\nFunction test:");
        String expStr = "function myFunction(idx){\n" +
        		"if (idx==1) print_err('idx equals 1');\n" +
        		" else {\n" +
        		"	print_err('idx does not equal 1');\n" +
        		"	idx=1;\n" +
        		"	}\n" +
        		"}\n" +
        		"myFunction(1);\n" +
        		"int in;\n" +
        		"print_err('in='+in);\n"+
        		"myFunction(in);\n" +
        		"print_err('in='+in);\n" +
        		"function init(){" +
        		"print_err('in init');\n" +
        		"};\n" +
        		"init();";
        print_code(expStr);

       Log logger = LogFactory.getLog(this.getClass());
       
        
        try {
              TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStart parseTree = parser.Start();

              System.out.println("Initializing parse tree..");
              parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
              System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.setRuntimeLogger(logger);
              executor.setGraph(graph);
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
              
//              CLVFFunctionDeclaration function = (CLVFFunctionDeclaration)parser.getFunctions().get("myFunction");
//              executor.executeFunction(function, new TLValue[]{new TLValue(TLValueType.INTEGER,new CloverInteger(1))});
//              executor.executeFunction(function, new TLValue[]{new TLValue(TLValueType.INTEGER,new CloverInteger(10))});
    
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Parse exception",e);
        }
              
    }

    public void test_import(){
        System.out.println("\nImport test:");
        String expStr = "import 'data/tlExample.ctl';";
        print_code(expStr);

       Log logger = LogFactory.getLog(this.getClass());
       
        
        try {
              TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStart parseTree = parser.Start();

              System.out.println("Initializing parse tree..");
              parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
              System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.setRuntimeLogger(logger);
              executor.setGraph(graph);
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
              
		      assertEquals(10,(executor.getGlobalVariable(parser.getGlobalVariableSlot("i")).getTLValue().getNumeric().getInt()));

        } catch (ParseException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Parse exception",e);
        }
              
    }

    public void test_eval(){
		System.out.println("\neval test:");
		String expStr = "string str='print_err(\"eval test OK\");';\n eval(str); print_err(eval_exp('\"ahoj\"'));\n";
		
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      print_code(expStr);
		      
 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.setParser(parser);
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
		    }
	}

    
    public void test_containerFunctions(){
		System.out.println("\nBuild-in container functions test:");
		String expStr = "list seznam=[1,2,3,4,5,6,7,8,9,10]; \n" +
						"int pop1=pop(seznam); remove(seznam,1); print_err(seznam); \n" +
						"int poll1=poll(seznam); \n" +
						"push(seznam,33); print_err(seznam); \n"+
						"insert(seznam,3,12,13,14,15,16,17); print_err(seznam); \n"+
						"sort(seznam); print_err(seznam); reverse(seznam); print_err(seznam); \n"+
						"copy(seznam,[ 1, 2, 3, 4, 5]); print_err(seznam); remove_all(seznam); print_err(seznam); int last=pop(seznam); boolean isNull=isnull(last); \n ";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }


 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals("pop",10,executor.getGlobalVariable(parser.getGlobalVariableSlot("pop1")).getTLValue().getNumeric().getInt());
		      assertEquals("poll",1,executor.getGlobalVariable(parser.getGlobalVariableSlot("poll1")).getTLValue().getNumeric().getInt());
		      assertEquals("isNull",true,executor.getGlobalVariable(parser.getGlobalVariableSlot("isNull")).getTLValue()==TLBooleanValue.TRUE);
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}
    
    public void test_dateFunctions(){
		System.out.println("\nBuild-in date functions test:");
		String expStr = "date first=2008-01-01; date second2=2008-02-01 12:12:12; \n" +
						"print_err(first); print_err(second2); print_err(trunc(second2)); print_err(trunc_date(second2)); \n";

	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }


 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		    //  assertEquals("pop",10,executor.getGlobalVariable(parser.getGlobalVariableSlot("pop1")).getTLValue().getNumeric().getInt());
		    //  assertEquals("poll",1,executor.getGlobalVariable(parser.getGlobalVariableSlot("poll1")).getTLValue().getNumeric().getInt());
		    //  assertEquals("isNull",true,executor.getGlobalVariable(parser.getGlobalVariableSlot("isNull")).getTLValue()==TLBooleanValue.TRUE);
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}
    
    
    public void test_stringFunctions(){
		System.out.println("\nBuild-in string functions test:");
		String expStr = "string numbers='abc1edf2geh3ijk10lmn999opq'; \n" +
						"list seznam=find(numbers,'\\d{2,}'); \n" +
						"print_err(seznam); print_err(cut(numbers,[0,1,3,4,10,5]));\n";
	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }


 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      //assertEquals("pop",10,executor.getGlobalVariable(parser.getGlobalVariableSlot("pop1")).getTLValue().getNumeric().getInt());
		      //assertEquals("poll",1,executor.getGlobalVariable(parser.getGlobalVariableSlot("poll1")).getTLValue().getNumeric().getInt());
		      //assertEquals("isNull",true,executor.getGlobalVariable(parser.getGlobalVariableSlot("isNull")).getTLValue()==TLBooleanValue.TRUE);
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}
    
    public void test_wildcardMapping(){
		System.out.println("\nBuild-in string functions test:");
		String expStr = "$0.* := $0.*; \n" +
						"$metaPort1.* := $0.*; \n" +
						"$2.* := $in.*;\n" +
						"$metaPort3.* := $in.*;\n";
	      print_code(expStr);
		try {
			

			// We use for different metadata with the same structure but different names
			// so that they resolve to different ports
			DataRecordMetadata[] outputMetadata = new DataRecordMetadata[4];
			DataRecord[] outputRecords = new DataRecord[4];
			for (int i=0; i<4; i++) {
				outputMetadata[i] = metaOut.duplicate();
				outputMetadata[i].setName("metaPort" + i);
				outputRecords[i] = DataRecordFactory.newRecord(outputMetadata[i]);
				outputRecords[i].init();
			}

			
			
			// We create input field with identical structure
			DataRecordMetadata[] inputMetadata = new DataRecordMetadata[1];
			inputMetadata[0] = metaOut.duplicate();
			inputMetadata[0].setName("in");
			
			DataRecord[] inputRecords = new DataRecord[1];
			inputRecords[0] = DataRecordFactory.newRecord(inputMetadata[0]);
			inputRecords[0].init();
			
			final String NAME_CONTENTS = "  My name ";
			final double AGE_CONTENTS = 13.5;
			final String CITY_CONTENTS = "Prague";
			final Date BORN_CONTENTS = ((GregorianCalendar)Calendar.getInstance()).getTime();
			
			SetVal.setString(inputRecords[0],"Name",NAME_CONTENTS);
			SetVal.setDouble(inputRecords[0],"Age",AGE_CONTENTS);
			SetVal.setString(inputRecords[0],"City",CITY_CONTENTS);
			SetVal.setValue(inputRecords[0],"Born",BORN_CONTENTS);
			inputRecords[0].getField("Value").setNull(true);
			
			TransformLangParser parser = new TransformLangParser(inputMetadata,outputMetadata,
					  new ByteArrayInputStream(expStr.getBytes()),"UTF-8");
			  
			CLVFStart parseTree = parser.Start();

			if (parser.getParseExceptions().size()>0){
				  // report error
				for(Throwable t : parser.getParseExceptions()) {
					System.out.println(t);
				}
				throw new RuntimeException("Parse exception");
			}


 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setParser(parser);
		      executor.setInputRecords(inputRecords);
		      executor.setOutputRecords(outputRecords);
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      // Compare for each output records if the original values were copied successfully
		      for (int i=0; i < outputRecords.length; i++) {
		    	  assertEquals(((CharSequence)outputRecords[i].getField("Name").getValue()).toString(),NAME_CONTENTS);
		    	  assertEquals(outputRecords[i].getField("Age").getValue(),AGE_CONTENTS);
		    	  assertEquals(((CharSequence)outputRecords[i].getField("City").getValue()).toString(),CITY_CONTENTS);
		    	  assertEquals(outputRecords[i].getField("Born").getValue(),BORN_CONTENTS);
		    	  assertTrue(outputRecords[i].getField("Value").isNull());
		      }
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}
    
    public void test_binaryMapping(){
		System.out.println("\nBuild-in string functions test:");
		String expStr = "print_err(byte2hex($0.Binary)); bytearray data=$0.Binary; print_err(byte2hex(data)); \n"+
						"$0.Binary:= data; \n" +
						"$0.Compressed := $0.Binary; \n";
	      print_code(expStr);
		try {
			

			// We use for different metadata with the same structure but different names
			// so that they resolve to different ports
			DataRecordMetadata[] outputMetadata = new DataRecordMetadata[] {metadataBinary};
			DataRecord[] outputRecords = new DataRecord[] {recBinaryOut};
			
			// We create input field with identical structure
			DataRecordMetadata[] inputMetadata = new DataRecordMetadata[] {metadataBinary};
			DataRecord[] inputRecords = new DataRecord[] {recBinary};
			
			TransformLangParser parser = new TransformLangParser(inputMetadata,outputMetadata,
					  new ByteArrayInputStream(expStr.getBytes()),"UTF-8");
			  
			CLVFStart parseTree = parser.Start();

			if (parser.getParseExceptions().size()>0){
				  // report error
				for(Throwable t : parser.getParseExceptions()) {
					System.out.println(t);
				}
				throw new RuntimeException("Parse exception");
			}


 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setParser(parser);
		      executor.setInputRecords(inputRecords);
		      executor.setOutputRecords(outputRecords);
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      // Compare for each output records if the original values were copied successfully
		    	 System.out.println(((ByteDataField)outputRecords[0].getField(0)).toString());
		      		assertEquals(((ByteDataField)outputRecords[0].getField(0)).toString(),(new ByteArray(BYTEARRAY_INITVALUE)).toString());
		    	    assertEquals(((ByteDataField)outputRecords[0].getField(1)).toString(),(new ByteArray(BYTEARRAY_INITVALUE)).toString());
		  		
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}
    
    public void test_bitFunctions(){
		System.out.println("\nBuild-in bits functions test:");
		String expStr = "int number1=0; int number2; number2=bit_set(number2,7,true); \n" +
						"int number3=bit_set(number1,4,true); print_err(number3); \n" +
						"int number4=bit_set(31,4,false); print_err(number4); \n" +
						"boolean isN2 = bit_is_set(number2,7); print_err(isN2);\n" +
						"int shiftL = bit_lshift(127,2); print_err(shiftL);\n" +
						"int shiftR = bit_rshift(shiftL,2); print_err(shiftR);\n" +
						"int and1 = bit_and(0x07,0x070); print_err(and1);\n" +
						"int xor1 = bit_xor(0x07,0x070); print_err(xor1);\n" +
						"int or1 = bit_or(0x07,0x070); print_err(or1);\n" +
						"int and2 = bit_and(127,87); print_err(and2);\n" +
						"int xor2 = bit_xor(127,87); print_err(xor2);\n" +
						"int or2 = bit_or(127,87); print_err(or2);\n" +
						"int invert1 = bit_invert(127); print_err(invert1);\n" +
						"int invert2 = bit_invert(-1); print_err(invert2);\n" +
						" print_err(bit_invert(0x0000)); \n";
	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }


 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      assertEquals("number3",16,executor.getGlobalVariable(parser.getGlobalVariableSlot("number3")).getTLValue().getNumeric().getInt());
		      assertEquals("number2",128,executor.getGlobalVariable(parser.getGlobalVariableSlot("number2")).getTLValue().getNumeric().getInt());
		      assertEquals("number4",15,executor.getGlobalVariable(parser.getGlobalVariableSlot("number4")).getTLValue().getNumeric().getInt());
		      assertTrue("isN2", (Boolean)executor.getGlobalVariable(parser.getGlobalVariableSlot("isN2")).getTLValue().getValue());
		      assertEquals("shiftL",127<<2,executor.getGlobalVariable(parser.getGlobalVariableSlot("shiftL")).getTLValue().getNumeric().getInt());
		      assertEquals("shiftR",(127<<2)>>2,executor.getGlobalVariable(parser.getGlobalVariableSlot("shiftR")).getTLValue().getNumeric().getInt());
		      assertEquals("invert1",~127,executor.getGlobalVariable(parser.getGlobalVariableSlot("invert1")).getTLValue().getNumeric().getInt());
		      assertEquals("invert2",~-1,executor.getGlobalVariable(parser.getGlobalVariableSlot("invert2")).getTLValue().getNumeric().getInt());
		      assertEquals("and1",0x07&0x070,executor.getGlobalVariable(parser.getGlobalVariableSlot("and1")).getTLValue().getNumeric().getInt());
		      assertEquals("and2",127&87,executor.getGlobalVariable(parser.getGlobalVariableSlot("and2")).getTLValue().getNumeric().getInt());
		      assertEquals("xor1",0x07^0x070,executor.getGlobalVariable(parser.getGlobalVariableSlot("xor1")).getTLValue().getNumeric().getInt());
		      assertEquals("xor2",127^87,executor.getGlobalVariable(parser.getGlobalVariableSlot("xor2")).getTLValue().getNumeric().getInt());
		      assertEquals("or1",0x07|0x070,executor.getGlobalVariable(parser.getGlobalVariableSlot("or1")).getTLValue().getNumeric().getInt());
		      assertEquals("or2",127|87,executor.getGlobalVariable(parser.getGlobalVariableSlot("or2")).getTLValue().getNumeric().getInt());
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}
    
    public void test_dictionaryFunctions(){
		System.out.println("\nBuild-in dictionary functions test:");
		String expStr = "dict_put_str('mykey','Hello string');\n" +
						"print_err(dict_get_str('mykey')); print_err(dict_get_str('non-existent-key')); print_err(dict_get_str('outside_key')); \n";
	      print_code(expStr);
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),expStr);
		      CLVFStart parseTree = parser.Start();

		      if (parser.getParseExceptions().size()>0){
		    	  //report error
		    	  for(Iterator it=parser.getParseExceptions().iterator();it.hasNext();){
			    	  System.out.println(it.next());
			      }
		    	  throw new RuntimeException("Parse exception");
		      }


 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setGraph(graph);
		      try{
		    	  graph.getDictionary().init();
		      }catch(Exception ex){
		    	  //DO nothing
		      }
		      graph.getDictionary().setValue("outside_key", "Outside value");
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    } catch (ComponentNotReadyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
    
    public void test_RecordManipulationOperators(){
		System.out.println("\nRecord manipulation operators:");
		String expStr = "record (@in) myrec; \n" +
						"int i; \n"+
						"print_err(myrec); print_err(@in[1]); print_err(@0[length(@in)-1]); myrec=@in; print_err(myrec); /*print_err(get_field_name(myrec,1); */\n" +
						"for (i=0;i< length(myrec); i++) { print_err(@0[i]); }\n" +
						"\n";
	      print_code(expStr);
		try {
			

			// We use for different metadata with the same structure but different names
			// so that they resolve to different ports
			DataRecordMetadata[] outputMetadata = new DataRecordMetadata[4];
			DataRecord[] outputRecords = new DataRecord[4];
			for (int i=0; i<4; i++) {
				outputMetadata[i] = metaOut.duplicate();
				outputMetadata[i].setName("metaPort" + i);
				outputRecords[i] = DataRecordFactory.newRecord(outputMetadata[i]);
				outputRecords[i].init();
			}

			
			
			// We create input field with identical structure
			DataRecordMetadata[] inputMetadata = new DataRecordMetadata[1];
			inputMetadata[0] = metaOut.duplicate();
			inputMetadata[0].setName("in");
			
			DataRecord[] inputRecords = new DataRecord[1];
			inputRecords[0] = DataRecordFactory.newRecord(inputMetadata[0]);
			inputRecords[0].init();
			
			final String NAME_CONTENTS = "  My name ";
			final double AGE_CONTENTS = 13.5;
			final String CITY_CONTENTS = "Prague";
			final Date BORN_CONTENTS = ((GregorianCalendar)Calendar.getInstance()).getTime();
			
			SetVal.setString(inputRecords[0],"Name",NAME_CONTENTS);
			SetVal.setDouble(inputRecords[0],"Age",AGE_CONTENTS);
			SetVal.setString(inputRecords[0],"City",CITY_CONTENTS);
			SetVal.setValue(inputRecords[0],"Born",BORN_CONTENTS);
			inputRecords[0].getField("Value").setNull(true);
			
			TransformLangParser parser = new TransformLangParser(inputMetadata,outputMetadata,
					  new ByteArrayInputStream(expStr.getBytes()),"UTF-8");
			  
			CLVFStart parseTree = parser.Start();

			if (parser.getParseExceptions().size()>0){
				  // report error
				for(Throwable t : parser.getParseExceptions()) {
					System.out.println(t);
				}
				throw new RuntimeException("Parse exception");
			}


 		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setParser(parser);
		      executor.setInputRecords(inputRecords);
		      executor.setOutputRecords(outputRecords);
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
//		      // Compare for each output records if the original values were copied successfully
//		      for (int i=0; i < outputRecords.length; i++) {
//		    	  assertEquals(((StringBuilder)outputRecords[i].getField("Name").getValue()).toString(),NAME_CONTENTS);
//		    	  assertEquals(outputRecords[i].getField("Age").getValue(),AGE_CONTENTS);
//		    	  assertEquals(((StringBuilder)outputRecords[i].getField("City").getValue()).toString(),CITY_CONTENTS);
//		    	  assertEquals(outputRecords[i].getField("Born").getValue(),BORN_CONTENTS);
//		    	  assertTrue(outputRecords[i].getField("Value").isNull());
//		      }
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}

	// This method tests fix of issue 4140. Copying failed in case the input data records were swapped.
    public void test_dataFieldCopy() {
		String expStr = "function test() { $0.Name := $0.Name; }\n";

		DataRecordMetadata inputMetadata[] = { metaOut.duplicate() };
		DataRecordMetadata outputMetadata[] = { metaOut.duplicate() };

		TransformLangParser parser = new TransformLangParser(inputMetadata, outputMetadata,
				new ByteArrayInputStream(expStr.getBytes()), "UTF-8");
		CLVFStart parseTree = null;

		try {
			parseTree = parser.Start();
			parseTree.init();
		} catch (ParseException exception) {
			exception.printStackTrace();
			fail();
		}

		// prepare multiple input and output data records to simulate swapping
		DataRecord[] inputRecords = new DataRecord[2];
		DataRecord[] outputRecords = new DataRecord[inputRecords.length];

		for (int i = 0; i < inputRecords.length; i++) {
			// init input records with their ordinal
			inputRecords[i] = DataRecordFactory.newRecord(inputMetadata[0]);
			inputRecords[i].init();
			inputRecords[i].getField("Name").setValue(Integer.toString(i + 1));

			// keep output records blank
			outputRecords[i] = DataRecordFactory.newRecord(outputMetadata[0]);
			outputRecords[i].init();
		}

		CLVFFunctionDeclaration testFunction = (CLVFFunctionDeclaration) parser.getFunctions().get("test");

		// execute the test() function multiple times for different input data records
		for (int i = 0; i < inputRecords.length; i++) {
			TransformLangExecutor executor = new TransformLangExecutor();
			executor.setParser(parser);
			executor.setInputRecords(new DataRecord[] { inputRecords[i] });
			executor.setOutputRecords(new DataRecord[] { outputRecords[i] });
			executor.executeFunction(testFunction, null);

			if (!inputRecords[i].getField("Name").equals(outputRecords[i].getField("Name"))) {
				fail("Field copy failed!");
			}
		}
    }

    public void test_TryCatch(){
		System.out.println("\nDate Error:");
		String expStr = "string exception;\n"+
						"date datum;\n"+
						"try \n" +
						"  datum = 123;\n" +
						"catch (exception) " +
						"   print_err(exception); \n"+
						"try \n" +
						"  datum = 123;\n" +
						"catch () " +
						"   print_err('error when assigning 123 to date variable'); \n";
	      print_code(expStr);
		try {
		      DataRecordMetadata[] recordMetadata=new DataRecordMetadata[] {metadata,metadata1};
		      DataRecordMetadata[] outMetadata=new DataRecordMetadata[] {metaOut,metaOut1};
			  TransformLangParser parser = new TransformLangParser(recordMetadata,
			  		outMetadata,new ByteArrayInputStream(expStr.getBytes()),"UTF-8");
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Parse tree:");
		      parseTree.dump("");
		      
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record,record1});
		      executor.setOutputRecords(new DataRecord[]{out,out1});
		      SetVal.setString(record1,2,"Prague");
		      record.getField("Age").setNull(true);
		      
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");
		      
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception",e);
	    }
	}
    
    
    
    
    public void print_code(String text){
        String[] lines=text.split("\n");
        System.out.println("\t:         1         2         3         4         5         ");
        System.out.println("\t:12345678901234567890123456789012345678901234567890123456789");
        for(int i=0;i<lines.length;i++){
            System.out.println((i+1)+"\t:"+lines[i]);
        }
    }
}
