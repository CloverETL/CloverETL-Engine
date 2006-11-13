/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002  David Pavlis
*
*    This program is free software; you can redistribute it and/or modify
*    it under the terms of the GNU General Public License as published by
*    the Free Software Foundation; either version 2 of the License, or
*    (at your option) any later version.
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU General Public License for more details.
*
*    You should have received a copy of the GNU General Public License
*    along with this program; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

package javaExamples;
import java.io.*;
import org.jetel.metadata.*;
import org.jetel.component.ComponentFactory;
import org.jetel.data.*;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;

public class testDataParsing {

	public static void main(String args[]){
		
	FileInputStream in=null;
	PrintStream out=null;
	DataRecord record;
	
    //initialization; must be present
    Defaults.init();
    ComponentFactory.init();
    
	System.out.println("Input file: "+args[0]);
	System.out.println("Output file: "+args[1]);
	
	try{
		in=new FileInputStream(args[0]);
		out=new PrintStream(new FileOutputStream(args[1]));
	}
	catch(FileNotFoundException e){
		e.printStackTrace();
	}
	
	DataRecordMetadata metadata=new DataRecordMetadata("TestInput",DataRecordMetadata.DELIMITED_RECORD);
	
	metadata.addField(new DataFieldMetadata("Name",DataFieldMetadata.STRING_FIELD, ";"));
	metadata.addField(new DataFieldMetadata("Age",DataFieldMetadata.NUMERIC_FIELD, "|"));
	metadata.addField(new DataFieldMetadata("City",DataFieldMetadata.STRING_FIELD, "\n"));
	
	DelimitedDataParser parser=new DelimitedDataParser();
	try{
		parser.init(metadata);
		parser.setDataSource(in);
	}catch(ComponentNotReadyException ex){
		ex.printStackTrace();
	}
	record = new DataRecord(metadata);
	record.init();
	
	try {
		while((record=parser.getNext(record))!=null){
			out.print("Name:"+record.getField(0).toString());
			out.print(" Age:"+record.getField(1).toString());
			out.println(" City:"+record.getField(2).toString());
			//System.out.println("Name:"+record.getField(0).toString());
			//System.out.println("Age:"+record.getField(1).toString());
			//System.out.println("City:"+record.getField(2).toString());
			//System.out.println();
		}
	} catch (JetelException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	
	}
	
} 


