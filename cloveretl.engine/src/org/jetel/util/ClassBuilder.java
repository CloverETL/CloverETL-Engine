/*
 *  jETeL/Clover - Java based ETL application framework.
 *  Copyright (C) 2002  David Pavlis
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 *  Created on May 31, 2003
 */
package org.jetel.util;

import java.util.HashMap;

import org.jetel.data.DataRecord;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This class. as its name indicates, assembles the code needed to build a
 * class and saves it to a file.
 * 
 * @author Wes Maciorowski
 * @version 1.0
 *
 */
public class ClassBuilder {
	/** used to store position of record and field tuples*/
	HashMap inputFieldRefs = null;
	/** used to store position of record and field tuples*/
	HashMap recordFieldRefs = null;

	/**
	 * Constructor.  It is used to create info for class name and constructor
	 * @param record
	 * @param arrayDataRecordMetadata
	 */
	public ClassBuilder(DataRecord record, DataRecordMetadata[] arrayDataRecordMetadata) {
		int[] rec_field_poz = null;
		StringBuffer bufRecord = new StringBuffer();
		StringBuffer bufField = new StringBuffer();
		
		inputFieldRefs = new HashMap();
		for(int i = 0; i < arrayDataRecordMetadata.length ; i++ ) {
			bufRecord.setLength(0);
			bufRecord.append('[').append(arrayDataRecordMetadata[i].getName()).append(']');
			for(int j = 0 ; j < arrayDataRecordMetadata[i].getNumFields() ; j++ ) {
				bufField.setLength(0);
				bufField.append(bufRecord).append('.');
				bufField.append('[').append(arrayDataRecordMetadata[i].getField(j).getName()).append(']');
				
				rec_field_poz = new int[2];
				rec_field_poz[0] = i;
				rec_field_poz[1] = j;
				
				inputFieldRefs.put( bufField.toString(), rec_field_poz);
				
			}
		}
		
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param tmpCode
	 * @return
	 */
	public int[][] constructMethod(String tmpCode) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * 
	 */
	public String getClassName() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param string
	 * @return
	 */
	public String getMethodName(String string) {
		// TODO Auto-generated method stub
		return null;
	}

}
