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


import org.jetel.component.RecordTransform;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.data.*;


public class reformatOrders implements RecordTransform{

	String message;
	int counter=0;
	int field=0;

	public boolean init(DataRecordMetadata sourceMetadata, DataRecordMetadata targetMetadata){
		return true;
	}
	public boolean init(DataRecordMetadata[] sourceMetadata, DataRecordMetadata targetMetadata){
		return true;
	}
	
	public boolean transform(DataRecord source, DataRecord target){
		StringBuffer strBuf=new StringBuffer(80);
		try{
			// let's concatenate shipping address into one long string
			strBuf.append(GetVal.getString(source,"ShipName")).append(';');
			strBuf.append(GetVal.getString(source,"ShipAddress")).append(';');
			strBuf.append(GetVal.getString(source,"ShipCity")).append(';');
			strBuf.append(GetVal.getString(source,"ShipCountry"));
			// mapping among source & target fields
			// some fields get assigned directly from source fields, some
			// are assigned from internall variables
			SetVal.setInt(target,"OrderKey",counter);
			SetVal.setInt(target,"OrderID",GetVal.getInt(source,"OrderID"));
			SetVal.setString(target,"CustomerID",GetVal.getString(source,"CustomerID"));
			SetVal.setValue(target,"OrderDate",GetVal.getDate(source,"OrderDate"));
			SetVal.setString(target,"ShippedDate","02.02.1999");
			SetVal.setInt(target,"ShipVia",GetVal.getInt(source,"ShipVia"));
			SetVal.setString(target,"ShipTo",strBuf.toString());
		}catch(Exception ex){
			message=ex.getMessage()+" ->occured with record :"+counter;
			throw new RuntimeException(message);
		}
		counter++;
			return true;
	}
	
	public boolean transform(DataRecord[] source, DataRecord target){
		return true;
	}

	public String getMessage(){
		return message;
	}
}
