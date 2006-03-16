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

package test.org.jetel.interpreter;

import java.io.ByteArrayInputStream;
import java.util.Calendar;

import junit.framework.TestCase;

import org.jetel.data.*;
import org.jetel.interpreter.CLVFStart;
import org.jetel.interpreter.FilterExpParser;
import org.jetel.interpreter.FilterExpParserExecutor;
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
	
	public void test_1_expression() {
		String expStr="$Age>=135 or 200>$Age and $Age>0 and 1==999999999999999 or $Name==\"HELLO\"";
		
		try {
			  FilterExpParser parser = new FilterExpParser(record,
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      System.out.println("Interpreting parse tree..");
		      FilterExpParserExecutor executor=new FilterExpParserExecutor();
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
			  FilterExpParser parser = new FilterExpParser(record,
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      parseTree.dump("");
		      System.out.println("Interpreting parse tree..");
		      FilterExpParserExecutor executor=new FilterExpParserExecutor();
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      System.out.println("result: "+executor.getResult());
		      
		      
		      parseTree.dump("");
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
		
	}
		
	public void test_3_expression() {
		String expStr="trim($Name)==\"HELLO\" or replace($Name,\".\" ,\"a\")";
		try {
			  FilterExpParser parser = new FilterExpParser(record,
			  		new ByteArrayInputStream(expStr.getBytes()));
		      CLVFStart parseTree = parser.Start();

		      System.out.println("Initializing parse tree..");
		      parseTree.init();
		      parseTree.dump("");
		      System.out.println("Interpreting parse tree..");
		      FilterExpParserExecutor executor=new FilterExpParserExecutor();
		      executor.visit(parseTree,null);
		      System.out.println("Finished interpreting.");

		      System.out.println("result: "+executor.getResult());
		      
		      
		      parseTree.dump("");
		      
		    } catch (Exception e) {
		    	System.err.println(e.getMessage());
		    	e.printStackTrace();
		    }
		
	}
	
}
