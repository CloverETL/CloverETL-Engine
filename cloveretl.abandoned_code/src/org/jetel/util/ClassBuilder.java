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
package org.jetel.util;

import java.util.ArrayList;
import java.util.HashMap;

import org.jetel.data.DataRecord;
import org.jetel.metadata.DataFieldMetadata;
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
	/** used to store position of input record and field tuples*/
	private HashMap inputFieldRefs = null;
	/** used to store position of fields in the processed record.  It may become important if there
	 * are any references to other fields in the same record.
	 */
	private HashMap recordFieldRefs = null;
	
	/** used to store the name of the Java class */
	private String recordClassName = null;

	/** used to store the name of the Java class */
	private String recordPackageName = null;
	
	/** used to store methods of the Java class */
	ArrayList javaMethodList = null;
	
	
	
	/**
	 * Constructor.  It is used to create info for class name and constructor
	 * @param record
	 * @param arrayDataRecordMetadata
	 */
	public ClassBuilder(DataRecord record, DataRecordMetadata[] arrayDataRecordMetadata) {
	/*
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

		recordFieldRefs = new HashMap();
		for(int i = 0; i < record.getNumFields() ; i++ ) {
			bufRecord.setLength(0);
			bufRecord.append('[').append(arrayDataRecordMetadata[i].getName()).append(']');
			bufRecord.append('.');
			bufRecord.append('[').append(arrayDataRecordMetadata[i].getField(i).getName()).append(']');
			bufField.setLength(0);
			bufField.append('[').append(arrayDataRecordMetadata[i].getField(i).getName()).append(']');
				
			rec_field_poz = new int[1];
			rec_field_poz[0] = i;
				
			recordFieldRefs.put( bufRecord.toString(), rec_field_poz);
			recordFieldRefs.put( bufField.toString(), rec_field_poz);
		}
		
		// let's assume that record names are well behaved and can be used as the names
		// for Java classes
	
		/*
		recordClassName = record.getMetadata().getName();
		
		recordPackageName = CloverProperties.USER_JAVA_PACKAGE_NAME;
		
		javaMethodList = new ArrayList();
		*/
	}

	/**
	 * @param tmpCode
	 * @return
	 */
	public int[][] constructMethod(DataFieldMetadata fieldMetadata) {
	
		int[][] inputRecordFieldDependencies = null;
		int[] intraRecordFieldDependencies = null;
		/*
		String tmpCode = fieldMetadata.getCodeStr();
		if(tmpCode != null) {
			CodeParser aCodeParser = new CodeParser(inputFieldRefs,recordFieldRefs);
			aCodeParser.parse();
			inputRecordFieldDependencies = aCodeParser.getInputRecordFieldDependencies();
			intraRecordFieldDependencies = aCodeParser.getIntraRecordFieldDependencies();
		}
		*/
		return inputRecordFieldDependencies;

	}

	/**
	 * 
	 */
	public String getClassName() {
		return recordClassName;
	}

	/**
	 * 
	 */
	public String getFullyQualifiedClassName() {
		return recordPackageName + "." + recordClassName;
	}

	/**
	 * @param string
	 * @return
	 */
	public String getMethodName(String string) {
//		 /*TODO Auto-generated method stub*/
		return null;
	}

	public void setInputFieldRefs(HashMap inputFieldRefs) {
		this.inputFieldRefs = inputFieldRefs;
	}

	public HashMap getInputFieldRefs() {
		return inputFieldRefs;
	}

	public void setRecordFieldRefs(HashMap recordFieldRefs) {
		this.recordFieldRefs = recordFieldRefs;
	}

	public HashMap getRecordFieldRefs() {
		return recordFieldRefs;
	}

}

