/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2002-04  David Pavlis <david_pavlis@hotmail.com>
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
*
*/
package org.jetel.data;

import java.util.Comparator;

/**
 *  This class compares two records of the same structure 
 *
 *@author     dpavlis
 *@created    February 28, 2004
 *@since      February 28, 2004
 */
public class RecordComparator implements Comparator{

	private int keyFields[];
	
	/**
	 *  Constructor for the RecordKey object
	 *
	 *@param  keyFieldNames  Description of Parameter
	 *@param  metadata       Description of Parameter
	 *@since                 May 2, 2002
	 */
	public RecordComparator(int keyFields[]) {
		this.keyFields = keyFields;
	}

	/**
	 *  Gets the keyFields attribute of the RecordKey object
	 *
	 *@return    The keyFields value
	 */
	public int[] getKeyFields() {
		return keyFields;
	}


	/**
	 *  Compares two records (of the same layout) based on defined key-fields and returns (-1;0;1) if (< ; = ; >)
	 *
	 *@param  record1  Description of the Parameter
	 *@param  record2  Description of the Parameter
	 *@return          -1 ; 0 ; 1
	 */
	public int compare(Object o1, Object o2) {
		int compResult;
		DataRecord record1=(DataRecord)o1;
		DataRecord record2=(DataRecord)o2;
		if (record1.getMetadata() != record2.getMetadata()) {
			throw new RuntimeException("Can't compare - records have different metadata associated." +
					" Possibly different structure");
		}
		for (int i = 0; i < keyFields.length; i++) {
			compResult = record1.getField(keyFields[i]).compareTo(record2.getField(keyFields[i]));
			if (compResult != 0) {
				return compResult;
			}
		}
		return 0;
		// seem to be the same
	}


	/**
	 *  Compares two records (can have different layout) based on defined key-fields 
	 *  and returns (-1;0;1) if (< ; = ; >).<br>
	 *  The particular fields to be compared have to be of the same type !
	 *
	 *@param  secondKey  RecordKey defined for the second record
	 *@param  record1    First record
	 *@param  record2    Second record
	 *@return            -1 ; 0 ; 1
	 */
	public int compare(RecordKey secondKey, DataRecord record1, DataRecord record2) {
		int compResult;
		int[] record2KeyFields = secondKey.getKeyFields();
		if (keyFields.length != record2KeyFields.length) {
			throw new RuntimeException("Can't compare. keys have different number of DataFields");
		}
		for (int i = 0; i < keyFields.length; i++) {
			compResult = record1.getField(keyFields[i]).compareTo(record2.getField(record2KeyFields[i]));
			if (compResult != 0) {
				return compResult;
			}
		}
		return 0;
		// seem to be the same
	}
}
// end RecordKey


