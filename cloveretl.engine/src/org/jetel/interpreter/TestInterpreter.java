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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import junit.framework.TestCase;

import org.jetel.data.*;
import org.jetel.data.primitive.CloverDouble;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.interpreter.*;
import org.jetel.interpreter.node.CLVFStart;
import org.jetel.interpreter.node.CLVFStartExpression;
import org.jetel.metadata.*;

/**
 * @author dpavlis
 * @since  10.8.2004
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
public class TestInterpreter extends TestCase {

	DataRecordMetadata metadata;
	DataRecord record;
	
	protected void setUp() {
	    Defaults.init();
	    
		metadata=new DataRecordMetadata("TestInput",DataRecordMetadata.DELIMITED_RECORD);
		
		metadata.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metadata.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metadata.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		metadata.addField(new DataFieldMetadata("Value",DataFieldMetadata.INTEGER_FIELD, "\n"));
		
		record = new DataRecord(metadata);
		record.init();
		
		SetVal.setString(record,0,"  HELLO ");
		SetVal.setInt(record,1,135);
		SetVal.setString(record,2,"Some silly longer string.");
		SetVal.setValue(record,3,Calendar.getInstance().getTime());
		record.getField("Born").setNull(true);
		SetVal.setInt(record,4,-999);
	}
	
	protected void tearDown() {
		metadata= null;
		record=null;
	}
	
	public void test_int(){
		System.out.println("int test:");
		String expStr = "int i; i=0; print_err(i); \n"+
						"int j; j=-1; print_err(j);\n"+
						"int minInt; minInt="+Integer.MIN_VALUE+"; print_err(minInt);\n"+
						"int maxInt; maxInt="+Integer.MAX_VALUE+"; print_err(maxInt)"+
						"int field; field=$Value; print_err(field)";

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

            System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      
		      Object[] result = executor.stack.globalVarSlot;
		      assertEquals(0,((CloverInteger)result[0]).intValue());
		      assertEquals(-1,((CloverInteger)result[1]).intValue());
		      assertEquals(Integer.MIN_VALUE,((CloverInteger)result[2]).intValue());
		      assertEquals(Integer.MAX_VALUE,((CloverInteger)result[3]).intValue());
		      assertEquals(((Integer)record.getField("Value").getValue()).intValue(),((CloverInteger)result[4]).intValue());
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
	}
	
	public void test_long(){
		System.out.println("\nlong test:");
		String expStr = "long i; i=0; print_err(i); \n"+
						"long j; j=-1; print_err(j);\n"+
						"long minLong; minLong="+(Long.MIN_VALUE+1)+"; print_err(minLong);\n"+
						"long maxLong; maxLong="+(Long.MAX_VALUE)+"; print_err(maxLong);\n"+
						"long field; field=$Value; print_err(field);\n"+
						"wrong="+Long.MAX_VALUE+"; print_err(wrong);\n";

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

            System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      
		      Object[] result = executor.stack.globalVarSlot;
		      assertEquals(0,((CloverLong)result[0]).longValue());
		      assertEquals(-1,((CloverLong)result[1]).longValue());
		      assertEquals(Long.MIN_VALUE+1,((CloverLong)result[2]).longValue());
		      assertEquals(Long.MAX_VALUE,((CloverLong)result[3]).longValue());
		      assertEquals(((Integer)record.getField("Value").getValue()).longValue(),((CloverLong)result[4]).longValue());
//		      assertEquals(Integer.MAX_VALUE,((CloverInteger)result[5]).intValue());
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
	}

	public void test_decimal(){
		System.out.println("\ndecimal test:");
		String expStr = "decimal i; i=0; print_err(i); \n"+
						"decimal j; j=-1.0; print_err(j);\n"+
						"decimal minLong; minLong=999999.999; print_err(minLong);\n"+
						"decimal maxLong; maxLong=0000000.0000000; print_err(maxLong);\n"+
						"decimal fieldValue; fieldValue=$Value; print_err(fieldValue);\n"+
						"decimal fieldAge; fieldAge=$Age; print_err(fieldAge);\n"+
						"decimal minDouble; minDouble="+Double.MIN_VALUE+"; print_err(minDouble)";

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

            System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      
		      Object[] result = executor.stack.globalVarSlot;
		      assertEquals(DecimalFactory.getDecimal(0),((Decimal)result[0]));
		      assertEquals(DecimalFactory.getDecimal(-1),((Decimal)result[1]));
		      assertEquals(DecimalFactory.getDecimal(999999.999),((Decimal)result[2]));
		      assertEquals(DecimalFactory.getDecimal(0),((Decimal)result[3]));
		      assertEquals(((Integer)record.getField("Value").getValue()).intValue(),((Decimal)result[4]).getInt());
		      assertEquals((Double)record.getField("Age").getValue(),new Double(((Decimal)result[5]).getDouble()));
		      assertEquals(new Double(Double.MIN_VALUE),new Double(((Decimal)result[6]).getDouble()));
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
	}

	public void test_number(){
		System.out.println("\nnumber test:");
		String expStr = "number i; i=0; print_err(i); \n"+
						"number j; j=-1.0; print_err(j);\n"+
						"number minLong; minLong=999999.999; print_err(minLong);\n"+
						"number fieldValue; fieldValue=$Value; print_err(fieldValue);\n"+
						"number fieldAge; fieldAge=$Age; print_err(fieldAge);\n"+
						"number minDouble; minDouble="+Double.MIN_VALUE+"; print_err(minDouble)";

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

            System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      
		      Object[] result = executor.stack.globalVarSlot;
		      assertEquals(DecimalFactory.getDecimal(0),((Decimal)result[0]));
		      assertEquals(DecimalFactory.getDecimal(-1),((Decimal)result[1]));
		      assertEquals(DecimalFactory.getDecimal(999999.999),((Decimal)result[2]));
		      assertEquals(((Integer)record.getField("Value").getValue()).intValue(),((Decimal)result[3]).getInt());
		      assertEquals((Double)record.getField("Age").getValue(),new Double(((Decimal)result[4]).getDouble()));
		      assertEquals(new Double(Double.MIN_VALUE),new Double(((Decimal)result[5]).getDouble()));
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
	}

	public void test_string(){
		System.out.println("\nstring test:");
		int lenght=1000;
		StringBuffer tmp = new StringBuffer(lenght);
		for (int i=0;i<lenght;i++){
			tmp.append(i%10);
		}
		String expStr = "string i; i=\"0\"; print_err(i); \n"+
						"string hello; hello='hello'; print_err(hello);\n"+
						"string fieldName; fieldName=$Name; print_err(fieldName);\n"+
						"string fieldCity; fieldCity=$City; print_err(fieldCity);\n"+
						"string longString; longString=\""+tmp+"\"; print_err(longString);\n"+
						"string specialChars; specialChars=\"\"\"; print_err(specialChars);";
		
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

            System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      
		      
		      Object[] result = executor.stack.globalVarSlot;
		      assertEquals("0",((StringBuffer)result[0]).toString());
		      assertEquals("hello",((StringBuffer)result[1]).toString());
		      assertEquals(record.getField("Name").getValue().toString(),((StringBuffer)result[2]).toString());
		      assertEquals(record.getField("City").getValue().toString(),((StringBuffer)result[3]).toString());
		      assertEquals(tmp.toString(),((StringBuffer)result[4]).toString());
		      assertEquals("\"",((StringBuffer)result[5]).toString());
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
	}

	public void test_date(){
		System.out.println("\ndate test:");
		String expStr = "date d3; d3=2006-08-01; print_err(d3);\n"+
						"date d2; d2=2006-08-02 15:15:00 ; print_err(d2);\n"+
						"date d1; d1=2006-1-1 1:2:3; print_err(d1);\n"+
						"date born; born=$Born; print_err(born);";
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
		
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

            System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      
		      Object[] result = executor.stack.globalVarSlot;
		      assertEquals(new GregorianCalendar(2006,7,01).getTime(),((Date)result[0]));
		      assertEquals(new GregorianCalendar(2006,7,02,15,15).getTime(),((Date)result[1]));
		      assertEquals(new GregorianCalendar(2006,0,01,01,02,03),((Date)result[2]));
		      assertEquals((Date)record.getField("Born").getValue(),((Date)result[3]));
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
	}

	public void test_boolean(){
		System.out.println("\nboolean test:");
		String expStr = "boolean b1; b1=true; print_err(b1);\n"+
						"boolean b2; b2=false ; print_err(b2);\n"+
						"boolean b4; print_err(b4);";
		GregorianCalendar born = new GregorianCalendar(1973,03,23);
		record.getField("Born").setValue(born.getTime());
		
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

            System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      
		      Object[] result = executor.stack.globalVarSlot;
		      assertEquals(true,((Boolean)result[0]).booleanValue());
		      assertEquals(false,((Boolean)result[1]).booleanValue());
		      assertEquals(false,((Boolean)result[2]).booleanValue());
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
	}

	public void test_variables(){
		System.out.println("\nvariable test:");
		String expStr = "boolean b1; boolean b2; b1=true; print_err(b1);\n"+
						"b2=false ; print_err(b2);\n"+
						"string b4; b4=\"hello\"; print_err(b4);\n"+
						"b2 = true; print_err(b2);\n"+
						"{int in; in=2; print_err(in)};\n";
//						"print_err(in)";
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

            System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      
		      Object[] result = executor.stack.globalVarSlot;
		      assertEquals(true,((Boolean)result[0]).booleanValue());
		      assertEquals(true,((Boolean)result[1]).booleanValue());
		      assertEquals("hello",((StringBuffer)result[2]).toString());
//		      assertEquals(2,((CloverInteger)result[3]).getInt());
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
	}

	public void test_operators(){
		System.out.println("\noperators test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=100;\n" +
						"int iplusj;iplusj=i+j; print_err(\"plus int:\"+iplusj);\n" +
						"long l;l="+((long)Integer.MAX_VALUE+10)+";print_err(l);\n" +
						"long m;m=1;print_err(m)\n" +
						"long lplusm;lplusm=l+m;print_err(\"plus long:\"+lplusm);\n" +
						"number n; n=0;print_err(n);\n" +
						"number m1; m1=0.001;print_err(m1);\n" +
						"number nplusm1; nplusm1=n+m1;print_err(\"plus number:\"+nplusm1);\n" +
						"number nplusj;nplusj=n+j;print_err(\"number plus int:\"+nplusj);\n";

		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

            System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
		      executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      
		      parseTree.dump("");
		      
		      Object[] result = executor.stack.globalVarSlot;
		      assertEquals(110,((CloverInteger)result[2]).getInt());
		      assertEquals((long)Integer.MAX_VALUE+11,((CloverLong)result[5]).getLong());
		      assertEquals(new Double(0.001),new Double(((Decimal)result[8]).getDouble()));
		      assertEquals(new Double(100),new Double(((Decimal)result[9]).getDouble()));
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
	}

	public void test_1_expression() {
		String expStr="$Age>=135 or 200>$Age and $Age>0 and 1==999999999999999 or $Name==\"HELLO\"";
		
		try {
			  TransformLangParser parser = new TransformLangParser(record.getMetadata(),
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStartExpression parseTree = parser.StartExpression();

              System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      assertEquals(true, ((Boolean)executor.getResult()).booleanValue() );
		      
		      parseTree.dump("");
		      
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
		}
	
	public void test_2_expression() {
		String expStr="datediff(nvl($Born,2005-2-1),2005-1-1,month)";
		try {
            TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStartExpression parseTree = parser.StartExpression();
              
              System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      parseTree.dump("");
		      System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
      
              assertEquals(1,((CloverInteger)executor.getResult()).intValue());
		      
		      
		      parseTree.dump("");
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
		
	}
		
	public void test_3_expression() {
		String expStr="trim($Name)==\"HELLO\" or replace($Name,\".\" ,\"a\")";
		try {
            TransformLangParser parser = new TransformLangParser(record.getMetadata(),
                    new ByteArrayInputStream(expStr.getBytes()));
              CLVFStartExpression parseTree = parser.StartExpression();

              System.out.println(expStr);
		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      parseTree.dump("");
		      System.out.println("Interpreting parse tree..");
              TransformLangExecutor executor=new TransformLangExecutor();
              executor.setInputRecords(new DataRecord[] {record});
              executor.visit(parseTree,null);
              System.out.println("Finished interpreting.");
		      
              assertEquals(true,((Boolean)executor.getResult()).booleanValue());
		      
		      
		      parseTree.dump("");
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
		
	}
	
}
