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

package org.jetel.test;

import java.io.*;
import org.jetel.component.DataRecordTransform;
import org.jetel.metadata.*;
import org.jetel.data.*;
import org.jetel.data.lookup.SimpleLookupTable;
import org.jetel.data.parser.DelimitedDataParserNIO;
import org.jetel.exception.JetelException;


public class testOrdersLookupReformat extends DataRecordTransform{
		int counter=0;
		SimpleLookupTable lookup;
		//String[] keys={"EmployeeID"};
		RecordKey key;
	
	public boolean init(DataRecordMetadata sourceMetadata[], DataRecordMetadata targetMetadata[]){
		DataRecordMetadata lookupMetadata;
		DataRecordMetadataXMLReaderWriter metadataReader=new DataRecordMetadataXMLReaderWriter();
		String[] lookupKey={"EmployeeID"};
		
		try{
			lookupMetadata=metadataReader.read(new FileInputStream("employees.fmt"));
			lookup=new SimpleLookupTable(lookupMetadata,lookupKey, 
						new DelimitedDataParserNIO(), new FileInputStream("employees.dat"));
			try {
				lookup.init();
			} catch (JetelException e) {
				e.printStackTrace();
				return false;
			}
		}catch(IOException ex){
			ex.printStackTrace();
			return false;
		}
		
		String sourceDataKey[]={"EmployeeID"};
		key = new RecordKey(sourceDataKey, sourceMetadata[0]);
		key.init();
		
		return true;
		
	}
	

	public boolean transform(DataRecord source, DataRecord target){
		DataRecord lookupRec=lookup.get(key.getKeyString(source));
		String employeeName=(lookupRec!=null)? GetVal.getString(lookupRec,"LastName")+" "+GetVal.getString(lookupRec,"FirstName")
					: "null";
		try{
		SetVal.setInt(target,"OrderID",GetVal.getInt(source,"OrderID"));
		SetVal.setString(target,"CustomerID",GetVal.getString(source,"CustomerID"));
		SetVal.setValue(target,"OrderDate",GetVal.getDate(source,"OrderDate"));
		SetVal.setString(target,"ShippedDate","02.02.1999");
		SetVal.setInt(target,"ShipVia",GetVal.getInt(source,"ShipVia"));
		SetVal.setString(target,"ShipCountry",GetVal.getString(source,"ShipCountry"));
		SetVal.setString(target,"EmployeeName",employeeName);
		}catch(Exception ex){
			System.err.println("Exception occured at "+counter+" msg:"+ex.getMessage());
			throw new RuntimeException(ex.getMessage());
		}
		counter++;
		return true;
	}
	
}

