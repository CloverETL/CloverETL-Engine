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

import java.util.Calendar;

import org.jetel.data.*;
import org.jetel.metadata.*;

/**
 * @author dpavlis
 * @since  10.8.2004
 *
 * TODO To change the template for this generated type comment go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
public class TestInterpreter {

	public static void main(String[] args) {
	
		DataRecordMetadata metadata=new DataRecordMetadata("TestInput",DataRecordMetadata.DELIMITED_RECORD);
		
		metadata.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
		metadata.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
		metadata.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
		metadata.addField(new DataFieldMetadata("Born",DataFieldMetadata.DATE_FIELD, "\n"));
		
		DataRecord record = new DataRecord(metadata);
		record.init();
		
		SetVal.setString(record,0,"AHOJ");
		SetVal.setInt(record,1,135);
		SetVal.setString(record,2,"Nove mesto na morave");
		SetVal.setValue(record,3,Calendar.getInstance().getTime());
		
		FilterExpParser parser = new FilterExpParser(record,System.in);
		
	    try {

	      /*
	        Start parsing from the nonterminal "Start".
	      */
	      SimpleNode parseTree = parser.Start();

	      /*
	        If parsing completed without exceptions, print the resulting
	        parse tree on standard output.
	      */
	      parseTree.dump("");
	      parseTree.stack.clear();
	      System.out.println("Initializing..");
	      parseTree.init();
	      System.out.println("Interpreting..");
	      parseTree.interpret();
	      System.out.println("Finished interpreting.");

	      System.out.println("Result is: "+parseTree.stack.pop().toString());
	      for (int i=0;i<parseTree.stack.top;i++){
	      	System.out.println(i+" : "+parseTree.stack.stack[i]);
	      }
	      
	    } catch (Exception e) {
	    	System.err.println(e.getMessage());
	    }
	}
}
