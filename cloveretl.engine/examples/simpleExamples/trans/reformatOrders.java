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

	public int transform(DataRecord[] source, DataRecord[] target){
		StringBuffer strBuf=new StringBuffer(80);

		if (source[0]==NullRecord.NULL_RECORD){
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
			SetVal.setInt(target[0],"PRODUCTID",counter);
			SetVal.setInt(target[0],"ORDERID",GetVal.getInt(source[0],"OrderID"));
			SetVal.setString(target[0],"CUSTOMERID",GetVal.getString(source[0],"CustomerID"));
			SetVal.setString(target[0],"CUSTOMER",strBuf.toString());
			SetVal.setInt(target[0], "SHIPTIME", (int)( (GetVal.getDate(
					source[0], "RequiredDate").getTime() - GetVal.getDate(
					source[0], "ShippedDate").getTime())
					/ 1000 / 60 / 60 / 24));
		}catch(Exception ex){
		  ex.printStackTrace();
			errorMessage=ex.getMessage()+" ->occured with record :"+counter;
			return SKIP;
		}

		counter++;

		return ALL;
	}

}
