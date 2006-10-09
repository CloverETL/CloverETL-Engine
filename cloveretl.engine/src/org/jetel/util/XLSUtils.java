
/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-06  Javlin Consulting <info@javlinconsulting.cz>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public LcellCodeU.charAt(cellCode.length()-1)icense for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

package org.jetel.util;

import javax.naming.InvalidNameException;

/**
 * @author avackova
 *
 */
public class XLSUtils {

	private final static int CELL_NUMBER_IN_SHEET = 'Z'-'A'+1;//number of "AA" cell in excel sheet
	
	/**
	 * This method calculates xls colum number from it's code:
	 * 	A='A'-65=0,B='B'-65,....,Z='Z'-65=CELL_NUMBER_IN_SHEET-1,
	 *  XYZ = ('X'-64)*CELL_NUMBER_IN_SHEET^2+('Y'-64)*CELL_NUMBER_IN_SHEET+('Z'-65)
	 * @param cellCode
	 * @return colum number from its code
	 * @throws InvalidNameException
	 */
	public static short getCellNum(String cellCode)throws InvalidNameException{
		char[] cellCodeU = cellCode.toUpperCase().toCharArray();
		int length = cellCodeU.length;
		int cellNumber = 0;
		for (int j=0;j<length;j++){
			if (cellCodeU[j]<'A' || cellCodeU[j]>'Z')
				throw new InvalidNameException("Wrong column index: " + cellCode);
			cellNumber = cellNumber*CELL_NUMBER_IN_SHEET + (cellCodeU[j]-64);
		}
		cellNumber-=1;
		if (cellNumber<0) 
			throw new InvalidNameException("Wrong column index: " + cellCode);
		return (short)cellNumber;
	}
}
