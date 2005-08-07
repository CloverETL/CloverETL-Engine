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

import org.jetel.component.DataRecordTransform;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.data.*;


public class reformatOrders extends DataRecordTransform{

	String message;
	int counter=0;
	int field=0;

	public boolean transform(DataRecord _source[], DataRecord _target[]){
		DataRecord source=_source[0];
		DataRecord target=_target[0];
		try{
		SetVal.setInt(target,"OrderID",GetVal.getInt(source,"OrderID"));
		SetVal.setString(target,"CustomerID",GetVal.getString(source,"CustomerID"));
		SetVal.setValue(target,"OrderDate",GetVal.getDate(source,"OrderDate"));
		SetVal.setString(target,"ShippedDate","02.02.1999");
		SetVal.setInt(target,"ShipVia",GetVal.getInt(source,"ShipVia"));
		SetVal.setString(target,"ShipCountry",GetVal.getString(source,"ShipCountry"));
		}catch(Exception ex){
			message=ex.getMessage()+" ->occured with record :"+counter;
			throw new RuntimeException(message);
		}
		counter++;
			return true;
	}
}
