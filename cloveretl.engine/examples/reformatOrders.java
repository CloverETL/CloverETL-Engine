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


import org.jetel.component.DataRecordTransform;
import org.jetel.data.*;


public class reformatOrders extends DataRecordTransform{

	int counter=0;
	int field=0;

	public boolean transform(DataRecord[] source, DataRecord[] target){
		StringBuffer strBuf=new StringBuffer(80);
		if (source[0]==null){
		   System.err.println("NULL source[0]");
		}
		try{
			// let's concatenate shipping address into one long string
			strBuf.append(GetVal.getString(source[0],"ShipName")).append(';');
			strBuf.append(GetVal.getString(source[0],"ShipAddress")).append(';');
			strBuf.append(GetVal.getString(source[0],"ShipCity")).append(';');
			strBuf.append(GetVal.getString(source[0],"ShipCountry"));
			// mapping among source & target fields
			// some fields get assigned directly from source fields, some
			// are assigned from internall variables
			SetVal.setInt(target[0],"OrderKey",counter);
			SetVal.setInt(target[0],"OrderID",GetVal.getInt(source[0],"OrderID"));
			SetVal.setString(target[0],"CustomerID",GetVal.getString(source[0],"CustomerID"));
			SetVal.setValue(target[0],"OrderDate",GetVal.getDate(source[0],"OrderDate"));
			SetVal.setString(target[0],"ShippedDate","02.02.1999");
			SetVal.setInt(target[0],"ShipVia",GetVal.getInt(source[0],"ShipVia"));
			SetVal.setString(target[0],"ShipTo",strBuf.toString());
		}catch(Exception ex){
		  ex.printStackTrace();
			errorMessage=ex.getMessage()+" ->occured with record :"+counter;
			return false;
		}
		counter++;
			return true;
	}
}
