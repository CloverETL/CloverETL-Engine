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

import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.Comparator;
import java.util.Arrays;

import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  This class compares two records of the same structure (created based on
 *  the same or at least compatible metadata).<br> Used
 *  primarily for sorting data records when it can be passed
 *  into JDK's standard sorting methods.
 *
 *@author     dpavlis
 *@created    February 28, 2004
 *@since      February 28, 2004
 *@revision    $Revision:  $
 */
public class RecordComparator implements Comparator{

	protected int keyFields[];
    protected RuleBasedCollator collator;
	
	/**
	 *  Constructor for the RecordComparator object
	 *
	 *@param  keyField  indexes of fields to be considered for sorting
	 *@since                 May 2, 2002
	 */
	public RecordComparator(int keyFields[]) {
		this(keyFields,null);
	}

    /**
     * Constructor for the RecordComparator object
     * 
     * @param keyFields indexes of fields to be considered for sorting
     * @param collator  Collator which should be use for comparing String fields
     */
    public RecordComparator(int keyFields[], RuleBasedCollator collator){
        this.keyFields = keyFields;
        this.collator=collator;
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
        DataRecord record1 = (DataRecord) o1;
        DataRecord record2 = (DataRecord) o2;
        /*
         * by D.Pavlis following check has been "relaxed" to speed up
         * processing. if (record1.getMetadata() != record2.getMetadata()) {
         * throw new RuntimeException("Can't compare - records have different
         * metadata associated." + " Possibly different structure"); }
         */
        if (collator != null) {
            for (int i = 0; i < keyFields.length; i++) {
                DataField field1 = record1.getField(keyFields[i]);
                if (field1.getType() == DataFieldMetadata.STRING_FIELD) {
                    if ((compResult = ((StringDataField) field1).compareTo(
                            record2.getField(keyFields[i]), collator)) != 0)
                        return compResult;
                } else {
                    if ((compResult = field1.compareTo(record2
                            .getField(keyFields[i]))) != 0)
                        return compResult;
                }
            }

        } else {

            for (int i = 0; i < keyFields.length; i++) {
                if ((compResult = record1.getField(keyFields[i]).compareTo(
                        record2.getField(keyFields[i]))) != 0)
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
        
         if (collator != null) {
             for (int i = 0; i < keyFields.length; i++) {
                 DataField field1 = record1.getField(keyFields[i]);
                 if (field1.getType() == DataFieldMetadata.STRING_FIELD) {
                     if ((compResult = ((StringDataField) field1).compareTo(
                             record2.getField(record2KeyFields[i]),collator)) != 0)
                         return compResult;
                 }else{
                     if ((compResult = field1.compareTo(
                             record2.getField(record2KeyFields[i]))) != 0)
                         return compResult;
                 }
            }             
             
         }else{
        
		for (int i = 0; i < keyFields.length; i++) {
			compResult = record1.getField(keyFields[i]).compareTo(record2.getField(record2KeyFields[i]));
			if (compResult != 0) {
				return compResult;
			}
		}
         }
		return 0;
		// seem to be the same
	}
	
	/* (non-Javadoc) Implemented to satisfy Comparator interface
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj){
		if (obj instanceof RecordComparator){
			return Arrays.equals(this.keyFields,((RecordComparator)obj).getKeyFields()); 
		}else{
			return false;
		}
	}

    public Collator getCollator() {
        return collator;
    }

    public void setCollator(RuleBasedCollator collator) {
        this.collator = collator;
    }
}
// end RecordKey


