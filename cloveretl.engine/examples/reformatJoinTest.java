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


public class reformatJoinTest extends DataRecordTransform{

	public boolean transform(DataRecord[] source, DataRecord[] target){

		
		System.out.print(source[0].getField(0).toString());
		target[0].getField(0).setValue(source[0].getField(0).getValue());
		System.out.print("|");
		System.out.print(source[0].getField(1).toString());
		target[0].getField(1).setValue(source[0].getField(1).getValue());
		System.out.print("|");
		System.out.print(source[0].getField(2).toString());
		target[0].getField(2).setValue(source[0].getField(2).getValue());
		if (source[1]!=null){
			System.out.print("|");
			System.out.print(source[1].getField(0).toString());
			target[0].getField(3).setValue(source[1].getField(0).getValue());
			System.out.print("|");
			System.out.print(source[1].getField(1).toString());
			target[0].getField(4).setValue(source[1].getField(1).getValue());
		}
		System.out.println();
		
		return true;
	}
}
