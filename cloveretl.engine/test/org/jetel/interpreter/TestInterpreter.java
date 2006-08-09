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
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import junit.framework.TestCase;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.SetVal;
import org.jetel.data.primitive.CloverInteger;
import org.jetel.data.primitive.CloverLong;
import org.jetel.data.primitive.Decimal;
import org.jetel.data.primitive.DecimalFactory;
import org.jetel.interpreter.node.CLVFStart;
import org.jetel.interpreter.node.CLVFStartExpression;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

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
	    } catch (ParseException e) {
	    	System.err.println(e.getMessage());
	    	e.printStackTrace();
	    	throw new RuntimeException("Parse exception");
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
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
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
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
		    }
	}

	public void test_number(){
		System.out.println("\nnumber test:");
		String expStr = "number i; i=0; print_err(i); \n"+
						"number j; j=-1.0; print_err(j);\n"+
						"number minLong; minLong=999999.999; print_err(minLong); print_err(999999.999); \n"+
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
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
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
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
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
		      assertEquals(new GregorianCalendar(2006,0,01,01,02,03).getTime(),((Date)result[2]));
		      assertEquals((Date)record.getField("Born").getValue(),((Date)result[3]));
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
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
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
		    }
	}

	public void test_variables(){
		System.out.println("\nvariable test:");
		String expStr = "boolean b1; boolean b2; b1=true; print_err(b1);\n"+
						"b2=false ; print_err(b2);\n"+
						"string b4; b4=\"hello\"; print_err(b4);\n"+
						"b2 = true; print_err(b2);\n"+
						"{int in; in=2; print_err(in);}\n";
//						"print_err(b2)";
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
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
		    }
	}

	public void test_plus(){
		System.out.println("\nplus test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=100;\n" +
						"int iplusj;iplusj=i+j; print_err(\"plus int:\"+iplusj);\n" +
						"long l;l="+Integer.MAX_VALUE/10+"l;print_err(l);\n" +
						"long m;m="+(Integer.MAX_VALUE)+"l;print_err(m)\n" +
						"long lplusm;lplusm=l+m;print_err(\"plus long:\"+lplusm);\n" +
//						"number m; m=0.001;print_err(m);\n" +
//						"number nplusm; nplusm=n+m;print_err(\"plus number:\"+nplusm);\n" +
						"number n; n=0;print_err(n);\n" +
						"number m1; m1=0.01;print_err(m1);\n" +
						"number nplusm1; nplusm1=n+m1;print_err(\"plus number:\"+nplusm1);\n" +
						"number nplusj;nplusj=n+j;print_err(\"number plus int:\"+nplusj);\n"+
						"decimal d; d=0.1;print_err(d);\n" +
						"decimal d1; d1=0.01;print_err(d1);\n" +
						"decimal dplusd1; dplusd1=d+d1;print_err(\"plus decimal:\"+dplusd1);\n" +
						"decimal dplusj;dplusj=d+j;print_err(\"decimal plus int:\"+dplusj);\n" +
						"decimal dplusn;dplusn=d+n;print_err(\"decimal plus number:\"+dplusn);\n" +
						"string s; s=\"hello\"; print_err(s);\n" +
						"string s1;s1=\" world\";print_err(s1);\n " +
						"string spluss1;spluss1=s+s1;print_err(\"adding strings:\"+spluss1);\n" +
						"string splusd;splusd=s+m1;print_err(\"string plus decimal:\"+splusb);\n" +
						"date mydate; mydate=2004-01-30 15:00:30;print_err(mydate);\n" +
						"date dateplus;dateplus=mydate+i;print_err(dateplus);\n";

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
		      assertEquals((long)Integer.MAX_VALUE+(long)Integer.MAX_VALUE/10,((CloverLong)result[5]).getLong());
		      assertEquals(new Double(0.01),new Double(((Decimal)result[8]).getDouble()));
		      assertEquals(new Double(100),new Double(((Decimal)result[9]).getDouble()));
		      assertEquals(new Double(0.11),new Double(((Decimal)result[12]).getDouble()));
		      assertEquals(new Double(100.1),new Double(((Decimal)result[13]).getDouble()));
		      assertEquals(new Double(0.1),new Double(((Decimal)result[14]).getDouble()));
		      assertEquals("hello world",(((StringBuffer)result[17]).toString()));
		      assertEquals("hello0.01",(((StringBuffer)result[18]).toString()));
		      assertEquals(new GregorianCalendar(2004,01,9,15,00,30).getTime(),(Date)result[20]);

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_minus(){
		System.out.println("\nplus test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=100;\n" +
						"int iplusj;iplusj=i-j; print_err(\"plus int:\"+iplusj);\n" +
						"long l;l="+((long)Integer.MAX_VALUE+10)+";print_err(l);\n" +
						"long m;m=1;print_err(m)\n" +
						"long lplusm;lplusm=l-m;print_err(\"plus long:\"+lplusm);\n" +
						"number n; n=0;print_err(n);\n" +
						"number m1; m1=0.01;print_err(m1);\n" +
						"number nplusm1; nplusm1=n-m1;print_err(\"plus number:\"+nplusm1);\n" +
						"number nplusj;nplusj=n-j;print_err(\"number plus int:\"+nplusj);\n"+
						"decimal d; d=0.1;print_err(d);\n" +
						"decimal d1; d1=0.01;print_err(d1);\n" +
						"decimal dplusd1; dplusd1=d-d1;print_err(\"plus decimal:\"+dplusd1);\n" +
						"decimal dplusj;dplusj=d-j;print_err(\"decimal plus int:\"+dplusj);\n" +
						"decimal dplusn;dplusn=d-n;print_err(\"decimal plus number:\"+dplusn);\n" +
						"date mydate; mydate=2004-01-30 15:00:30;print_err(mydate);\n" +
						"date dateplus;dateplus=mydate-i;print_err(dateplus);\n";

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
		      assertEquals(-90,((CloverInteger)result[2]).getInt());
		      assertEquals((long)Integer.MAX_VALUE+9,((CloverLong)result[5]).getLong());
		      assertEquals(new Double(-0.01),new Double(((Decimal)result[8]).getDouble()));
		      assertEquals(new Double(-100),new Double(((Decimal)result[8]).getDouble()));
		      assertEquals(new Double(0.09),new Double(((Decimal)result[11]).getDouble()));
		      assertEquals(new Double(-99.9),new Double(((Decimal)result[11]).getDouble()));
		      assertEquals(new Double(0.1),new Double(((Decimal)result[12]).getDouble()));
		      assertEquals(new GregorianCalendar(2004,0,20,15,00,30).getTime(),(Date)result[13]);

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_multiply(){
		System.out.println("\nmultiply test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=100;\n" +
						"int iplusj;iplusj=i*j; print_err(\"plus int:\"+iplusj);\n" +
						"long l;l="+((long)Integer.MAX_VALUE+10)+";print_err(l);\n" +
						"long m;m=1;print_err(m)\n" +
						"long lplusm;lplusm=l*m;print_err(\"plus long:\"+lplusm);\n" +
						"number n; n=0;print_err(n);\n" +
						"number m1; m1=0.01;print_err(m1);\n" +
						"number nplusm1; nplusm1=n*m1;print_err(\"plus number:\"+nplusm1);\n" +
						"number m1plusj;m1plusj=m1*j;print_err(\"number plus int:\"+m1plusj);\n"+
						"decimal d; d=0.1;print_err(d);\n" +
						"decimal d1; d1=0.01;print_err(d1);\n" +
						"decimal dplusd1; dplusd1=d*d1;print_err(\"plus decimal:\"+dplusd1);\n" +
						"decimal dplusj;dplusj=d*j;print_err(\"decimal plus int:\"+dplusj);\n"+
						"decimal dplusn;dplusn=d*n;print_err(\"decimal plus number:\"+dplusn);\n";

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
		      assertEquals(1000,((CloverInteger)result[2]).getInt());
		      assertEquals((long)Integer.MAX_VALUE+10,((CloverLong)result[5]).getLong());
		      assertEquals(new Double(1),new Double(((Decimal)result[8]).getDouble()));
		      assertEquals(new Double(-100),new Double(((Decimal)result[8]).getDouble()));
		      assertEquals(new Double(0.001),new Double(((Decimal)result[11]).getDouble()));
		      assertEquals(new Double(10),new Double(((Decimal)result[11]).getDouble()));
		      assertEquals(new Double(0),new Double(((Decimal)result[12]).getDouble()));

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_division(){
		System.out.println("\ndivision test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=100;\n" +
						"int iplusj;iplusj=i/j; print_err(\"plus int:\"+iplusj);\n" +
						"int jdivi;jdivi=j/i; print_err(\"plus int:\"+jdivi);\n" +
						"long l;l="+((long)Integer.MAX_VALUE+10)+";print_err(l);\n" +
						"long m;m=1;print_err(m)\n" +
						"long lplusm;lplusm=l/m;print_err(\"plus long:\"+lplusm);\n" +
						"number n; n=0;print_err(n);\n" +
						"number m1; m1=0.01;print_err(m1);\n" +
						"number n1; n1=10;print_err(n1);\n" +
						"number nplusm1; nplusm1=n/m1;print_err(\"0/0.01:\"+nplusm1);\n" +
						"number m1divn; m1divn=m1/n;print_err(\"deleni nulou:\"+m1divn);\n" +
						"number m1divn1; m1divn1=m1/n1;print_err(\"deleni numbers:\"+m1divn1);\n" +
						"number m1plusj;m1plusj=m1/j;print_err(\"number division int:\"+m1plusj);\n"+
						"decimal d; d=0.1;print_err(d);\n" +
						"decimal d1; d1=0.01;print_err(d1);\n" +
						"decimal dplusd1; dplusd1=d/d1;print_err(\"plus decimal:\"+dplusd1);\n" +
						"decimal dplusj;dplusj=d/j;print_err(\"decimal plus int:\"+dplusj);\n"+
						"decimal dplusn;dplusn=d/n1;print_err(\"decimal plus number:\"+dplusn);\n";

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
		      assertEquals(0,((CloverInteger)result[2]).getInt());
		      assertEquals(10,((CloverInteger)result[3]).getInt());
		      assertEquals((long)Integer.MAX_VALUE+10,((CloverLong)result[6]).getLong());
		      assertEquals(new Double(0),new Double(((Decimal)result[10]).getDouble()));
		      assertEquals("nan",new Double(((Decimal)result[11]).getDouble()));
		      assertEquals(new Double(0.001),new Double(((Decimal)result[12]).getDouble()));
		      assertEquals(new Double(0.0001),new Double(((Decimal)result[13]).getDouble()));
		      assertEquals(new Double(10),new Double(((Decimal)result[16]).getDouble()));
		      assertEquals(new Double(0.001),new Double(((Decimal)result[17]).getDouble()));
		      assertEquals(new Double(0.01),new Double(((Decimal)result[18]).getDouble()));

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_modulus(){
		System.out.println("\nmodulus test:");
		String expStr = "int i; i=10;\n"+
						"int j; j=103;\n" +
						"int iplusj;iplusj=j%i; print_err(\"plus int:\"+iplusj);\n" +
						"long l;l="+((long)Integer.MAX_VALUE+10)+";print_err(l);\n" +
						"long m;m=2;print_err(m)\n" +
						"long lplusm;lplusm=l%m;print_err(\"plus long:\"+lplusm);\n" +
						"number n; n=10.2;print_err(n);\n" +
						"number m1; m1=2;print_err(m1);\n" +
						"number nplusm1; nplusm1=n%m1;print_err(\"plus number:\"+nplusm1);\n" +
						"number m1plusj;m1plusj=n%i;print_err(\"number plus int:\"+m1plusj);\n"+
						"decimal d; d=10.1;print_err(d);\n" +
						"decimal d1; d1=10;print_err(d1);\n" +
						"decimal dplusd1; dplusd1=d%d1;print_err(\"plus decimal:\"+dplusd1);\n" +
						"decimal dplusj;dplusj=d%i;print_err(\"decimal plus int:\"+dplusj);\n"+
						"decimal dplusn;dplusn=d%m1;print_err(\"decimal plus number:\"+dplusn);\n";

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
		      assertEquals(1000,((CloverInteger)result[2]).getInt());
		      assertEquals((long)Integer.MAX_VALUE+10,((CloverLong)result[5]).getLong());
		      assertEquals(new Double(1),new Double(((Decimal)result[8]).getDouble()));
		      assertEquals(new Double(-100),new Double(((Decimal)result[8]).getDouble()));
		      assertEquals(new Double(0.001),new Double(((Decimal)result[11]).getDouble()));
		      assertEquals(new Double(10),new Double(((Decimal)result[11]).getDouble()));
		      assertEquals(new Double(0),new Double(((Decimal)result[12]).getDouble()));

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_increment_decrement(){
		System.out.println("\nincrement-decrement test:");
		String expStr = "int i; i=10;print_err(++i);\n" +
						"--i;" +
						"print_err(--i);\n"+
						"long j;j="+(Long.MAX_VALUE-10)+";print_err(++j);\n" +
						"print_err(--j);\n"+
						"decimal d;d=2;print_err(++d);\n" +
						"print_err(--d);\n;" +
						"number n;n=3.5;print_err(++n);\n" +
						"print_err(--n);\n" +
						"{print_err(++n);}\n" +
						"print_err(++n);\n";

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
		      assertEquals(10,((CloverInteger)result[0]).getInt());
		      assertEquals(Long.MAX_VALUE-10,((CloverLong)result[1]).getLong());
		      assertEquals(new Double(2),new Double(((Decimal)result[2]).getDouble()));
		      assertEquals(new Double(5.5),new Double(((Decimal)result[3]).getDouble()));

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_equal(){
		System.out.println("\nequal test:");
		String expStr = "int i; i=10;print_err(\"i=\"+i);\n" +
						"int j;j=9;print_err(\"j=\"+j);\n" +
//						"boolean eq1; eq1=(i==j+1);print_err(\"eq1=\"+eq1);\n" +
						"boolean eq1; eq1=(i==j+1);print_err(\"eq1=\");print_err(eq1);\n" +
						"eq1=(i.eq.j);print_err(\"eq1=\");print_err(eq1);\n" +
						"long l;l=10;print_err(\"l=\"+l);\n" +
						"boolean eq2;eq2=(l==j);print_err(\"eq2=\");print_err(eq2);\n" +
						"eq2=(l.eq.i);print_err(\"eq2=\");print_err(eq2);\n" +
						"decimal d;d=10;print_err(\"d=\"+d);\n" +
						"boolean eq3;eq3=d==i;print_err(\"eq3=\");print_err(eq3);\n" +
						"number n;n=10;print_err(\"n=\"+n);\n" +
						"boolean eq4;eq4=n.eq.l;print_err(\"eq4=\");print_err(eq4);\n" +
						"boolean eq5;eq5=n==d;print_err(\"eq5=\");print_err(eq5);\n" +
						"string s;s='hello';print_err(\"s=\"+s);\n" +
						"string s1;s1=\"hello \";print_err(\"s1=\"+s1);\n" +
						"boolean eq6;eq6=s.eq.s1;print_err(\"eq6=\");print_err(eq6);\n" +
						"boolean eq7;eq7=s==trim(s1);print_err(\"eq7=\");print_err(eq7);\n" +
						"date mydate;mydate=2006-01-01;print_err(\"mydate=\");print_err(mydate)\n" +
						"date anothermydate;print_err(\"anothermydate=\");print_err(anothermydate);\n" +
						"boolean eq8;eq8=mydate.eq.anothermydate;print_err(\"eq8=\");print_err(eq8);\n" +
						"anothermydate=2006-1-1 0:0:0;print_err(\"anothermydate=\");print_err(anothermydate);\n" +
						"boolean eq9;eq9=mydate==anothermydate;print_err(\"eq9=\");print_err(eq9);\n" +
						"boolean eq10;eq10=eq9.eq.eq8;print_err(\"eq10=\");print_err(eq10);\n";

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
		      assertEquals(false,((Boolean)result[2]).booleanValue());
		      assertEquals(true,((Boolean)result[4]).booleanValue());
		      assertEquals(true,((Boolean)result[6]).booleanValue());
		      assertEquals(true,((Boolean)result[8]).booleanValue());
		      assertEquals(true,((Boolean)result[9]).booleanValue());
		      assertEquals(false,((Boolean)result[12]).booleanValue());
		      assertEquals(true,((Boolean)result[13]).booleanValue());
		      assertEquals(false,((Boolean)result[16]).booleanValue());
		      assertEquals(true,((Boolean)result[17]).booleanValue());
		      assertEquals(false,((Boolean)result[18]).booleanValue());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
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
		      assertEquals(true,((Boolean)result[2]).booleanValue());
		      assertEquals(true,((Boolean)result[4]).booleanValue());
		      assertEquals(false,((Boolean)result[6]).booleanValue());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_greater_less(){
		System.out.println("\nGreater and less test:");
		String expStr = "int i; i=10;print_err(\"i=\"+i);\n" +
						"int j;j=9;print_err(\"j=\"+j);\n" +
						"boolean eq1; eq1=(i>j);print_err(\"eq1=\");print_err(eq1);\n" +
						"long l;l=10;print_err(\"l=\"+l);\n" +
						"boolean eq2;eq2=(l>=j);print_err(\"eq2=\");print_err(eq2);\n" +
						"decimal d;d=10;print_err(\"d=\"+d);\n" +
						"boolean eq3;eq3=d=>i;print_err(\"eq3=\");print_err(eq3);\n" +
						"number n;n=10;print_err(\"n=\"+n);\n" +
						"boolean eq4;eq4=n.gt.l;print_err(\"eq4=\");print_err(eq4);\n" +
						"boolean eq5;eq5=n.ge.d;print_err(\"eq5=\");print_err(eq5);\n" +
						"string s;s='hello';print_err(\"s=\"+s);\n" +
						"string s1;s1=\"hello\";print_err(\"s1=\"+s1);\n" +
						"boolean eq6;eq6=s<s1;print_err(\"eq6=\");print_err(eq6);\n" +
						"date mydate;mydate=2006-01-01;print_err(\"mydate=\");print_err(mydate)\n" +
						"date anothermydate;print_err(\"anothermydate=\");print_err(anothermydate);\n" +
						"boolean eq7;eq7=mydate.lt.anothermydate;print_err(\"eq7=\");print_err(eq7);\n" +
						"anothermydate=2006-1-1 0:0:0;print_err(\"anothermydate=\");print_err(anothermydate);\n" +
						"boolean eq8;eq8=mydate<=anothermydate;print_err(\"eq8=\");print_err(eq8);\n" +
						"boolean eq9;eq9=eq7>eq6;print_err(\"eq9=\");print_err(eq9);\n";

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
		      assertEquals("eq1",true,((Boolean)result[2]).booleanValue());
		      assertEquals("eq2",true,((Boolean)result[4]).booleanValue());
		      assertEquals("eq3",true,((Boolean)result[6]).booleanValue());
		      assertEquals("eq4",false,((Boolean)result[8]).booleanValue());
		      assertEquals("eq5",true,((Boolean)result[9]).booleanValue());
		      assertEquals("eq6",false,((Boolean)result[12]).booleanValue());
		      assertEquals("eq7",true,((Boolean)result[15]).booleanValue());
		      assertEquals("eq8",true,((Boolean)result[16]).booleanValue());
		      assertEquals("eq9",false,((Boolean)result[17]).booleanValue());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_regex(){
		System.out.println("\nRegex test:");
		String expStr = "int i; i=10;print_err(\"i=\"+i);\n" +
						"int j;j=9;print_err(\"j=\"+j);\n" +
						"boolean eq1; eq1=(i~=j);print_err(\"eq1=\");print_err(eq1);\n" +
						"string s;s='Hej';print_err(s);\n" +
						"boolean eq2;eq2=(s~=\"[A-Za-z]{3}\");print_err(\"eq2=\");print_err(eq2);\n";

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
		      assertEquals(false,((Boolean)result[2]).booleanValue());
		      assertEquals(true,((Boolean)result[4]).booleanValue());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_1_expression() {
		String expStr="$Age>=135 or 200>$Age and not $Age<=0 and 1==999999999999999 or $Name==\"HELLO\"";
		
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
		      
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
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
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
		    }
		
	}
		
	public void test_3_expression() {
		String expStr="not (trim($Name).ne.\"HELLO\" || replace($Name,\".\" ,\"a\"))";
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
		      
		    } catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
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
						"if (d=0.1 || l<=1) n=0;\n" +
						"else {n=-1;print_err('n rovne -1')}\n" +
						"date date1; date1=2006-01-01;print_err(date1);\n" +
						"date date2; date2=2006-02-01;print_err(date2);\n" +
						"boolean result;result=false;\n" +
						"boolean compareDates;compareDates=date1<=date2;print_err(compareDates);\n" +
						"if (date1<=date2) \n" +
						"{  print_err('before if (i<jj)');\n" +
						"	if (i<j) print_err('date1<today and i<j') else print_err('date1<date2 only')\n" +
						"	result=true;}\n" +
						"result=false;" +
						"if (i<j) result=true;\n" +
						"else if (not result) result=true;\n" +
						"else print_err('last else');\n";

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
		      assertEquals(1,((CloverLong)result[2]).getLong());
		      assertEquals(new Double(0),new Double(((Decimal)result[3]).getDouble()));
		      assertEquals(new Double(0),new Double(((Decimal)result[4]).getDouble()));
		      assertEquals(true,((Boolean)result[7]).booleanValue());

		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_switch(){
		System.out.println("\nSwitch test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"int n;n=datediff(born,1900-01-01,month)%12;print_err(n);\n" +
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
						"	case 3:print_err('a kuku')\n" +
						"	case 8.0:mont='september';\n" +
						"	case 9.0:mont='october';\n" +
						"	case 10.0:mont='november';\n" +
						"	case 11.0:mont='december';\n" +
						"	default: mont='unknown';};\n"+
						"print_err('month:'+mont);\n" +
						"boolean ok;ok=(n.ge.0)and(n.lt.12);\n" +
						"switch (ok) {\n" +
						"	case true:print_err('OK')\n" +
						"	case false:print_err('WRONG')};\n" +
						"switch (born) {\n" +
						"	case 2006-01-01:{mont='January';print_err('january);}\n" +
						"	case 1973-04-23:{mont='April';print_err('april');}\n" +
						"	default:print_err('other')};\n"+
						"switch (born<1996-08-01) {\n" +
						"	case true:{print_err('older then ten');}\n" +
						"	default:print_err('younger then ten')};\n";
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
		      assertEquals(3,((CloverInteger)result[1]).getInt());
		      assertEquals("April",((StringBuffer)result[2]).toString());
		      assertEquals(true,((Boolean)result[4]).booleanValue());
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}
	
	public void test_while(){
		System.out.println("\nWhile test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"date now;now=today();print_err(now);\n" +
						"int yer;yer=0;\n" +
						"while (born<now) {\n" +
						"	born=dateadd(born,1,year);\n " +
						"	while (yer<5) yer=yer+1;\n" +
						"	yer=yer+1;}\n" +
						"print_err('years:'+yer);\n";
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
		      assertEquals(39,((CloverInteger)result[2]).getInt());
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_do_while(){
		System.out.println("\nDo-while test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"date now;now=today();print_err(now);\n" +
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
		      assertEquals(72,((CloverInteger)result[2]).getInt());
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}

	public void test_for(){
		System.out.println("\nDo-while test:");
		String expStr = "date born; born=$Born;print_err(born);\n" +
						"date now;now=today();print_err(now);\n" +
						"int yer;yer=0;\n" +
						"for (born;born<now;born=dateadd(born,1,year)) yer=yer+1;\n" +
						"print_err('years on the end:'+yer);\n" +
						"boolean b;\n" +
						"if (yer=100) b=true;\n" +
						"for (b=false;!b;yer=yer+1) \n" +
						"	if (yer==100) b=true;" +
						"print_err('years on the end:'+yer);\n"+
						"int i;\n" +
						"for (i=0;i.le.10;i=i+1) ;\n" +
						"print_err('on the end i='+i);\n";
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
		      assertEquals(34,((CloverInteger)result[2]).getInt());
		      assertEquals(11,((CloverInteger)result[3]).getInt());
		      
		} catch (ParseException e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    	throw new RuntimeException("Parse exception");
	    }
	}
}
