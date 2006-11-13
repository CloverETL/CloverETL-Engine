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

import java.io.FileInputStream;
import java.io.IOException;

import org.jetel.component.DataRecordTransform;
import org.jetel.data.DataRecord;
import org.jetel.data.GetVal;
import org.jetel.data.RecordKey;
import org.jetel.data.SetVal;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.lookup.SimpleLookupTable;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.DataRecordMetadataXMLReaderWriter;


public class testOrdersLookupReformat extends DataRecordTransform{
		int counter=0;
		SimpleLookupTable lookup;
		RecordKey key;
	
	public boolean init(DataRecordMetadata sourceMetadata[], DataRecordMetadata targetMetadata[]){
		DataRecordMetadata lookupMetadata;
		DataRecordMetadataXMLReaderWriter metadataReader=new DataRecordMetadataXMLReaderWriter();
		String[] lookupKey={"EmployeeID"};
		
		try{
			lookupMetadata=metadataReader.read(new FileInputStream("employees.fmt"));
			Parser parser=new DelimitedDataParser();
			parser.init(lookupMetadata);
			parser.setDataSource(new FileInputStream("employees.dat"));
			lookup=new SimpleLookupTable("lookup",lookupMetadata,lookupKey,parser );
//			try {
				lookup.init();
//			} catch (JetelException e) {
//				e.printStackTrace();
//				return false;
//			}
		}catch(IOException ex){
			ex.printStackTrace();
			return false;
		}catch(ComponentNotReadyException ex){
		    ex.printStackTrace();
		    return false;
		}
		
		String sourceDataKey[]={"EmployeeID"};
		key = new RecordKey(sourceDataKey, sourceMetadata[0]);
		key.init();
		lookup.setLookupKey(key);
		
		return true;
		
	}
	

	public boolean transform(DataRecord source[], DataRecord target[]){
		DataRecord lookupRec=lookup.get(source[0]);
		String employeeName=(lookupRec!=null)? GetVal.getString(lookupRec,"LastName")+" "+GetVal.getString(lookupRec,"FirstName")
					: "null";
		try{
		SetVal.setInt(target[0],"OrderID",GetVal.getInt(source[0],"OrderID"));
		SetVal.setString(target[0],"CustomerID",GetVal.getString(source[0],"CustomerID"));
		SetVal.setValue(target[0],"OrderDate",GetVal.getDate(source[0],"OrderDate"));
		SetVal.setString(target[0],"ShippedDate","02.02.1999");
		SetVal.setInt(target[0],"ShipVia",GetVal.getInt(source[0],"ShipVia"));
		SetVal.setString(target[0],"ShipCountry",GetVal.getString(source[0],"ShipCountry"));
		SetVal.setString(target[0],"EmployeeName",employeeName);
		}catch(Exception ex){
			System.err.println("Exception occured at "+counter+" msg:"+ex.getMessage());
			throw new RuntimeException(ex.getMessage());
		}
		counter++;
		return true;
	}
	
}

