/*
 * jETeL/CloverETL - Java based ETL application framework.
 * Copyright (c) Javlin, a.s. (info@cloveretl.com)
 *  
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.jetel.data;

import java.text.RuleBasedCollator;
import java.util.Arrays;

import org.jetel.metadata.DataFieldType;

public class RecordOrderedComparator extends RecordComparator {
	
	private boolean[] keyOrderings;

	/**
	 *  Constructor for the RecordOrderedComparator object
	 *
	 * @param  keyField  indexes of fields to be considered for sorting
	 * @since                 May 2, 2002
	 */
	private RecordOrderedComparator(int[] keyFields) {
		super(keyFields);
		keyOrderings = new boolean[keyFields.length];
		Arrays.fill(keyOrderings, true);
	}

	/**
	 *  Constructor for the RecordOrderedComparator object
 	 * @param keyOrderings ordering of columns for each key (true=ascending)
	 *
	 * @param  keyField  indexes of fields to be considered for sorting
	 */
	public RecordOrderedComparator(int[] keyFields, boolean[] keyOrderings) {
		super(keyFields);
		this.keyOrderings = keyOrderings;
	}

    /**
     * Constructor for the RecordOrderedComparator object
     * 
     * @param keyFields indexes of fields to be considered for sorting
     * @param collator  Collator which should be use for comparing String fields
     */
	private RecordOrderedComparator(int[] keyFields, RuleBasedCollator collator) {
		super(keyFields, collator);
		keyOrderings = new boolean[keyFields.length];
		Arrays.fill(keyOrderings, true);
		
	}
	
	/**
     * Constructor for the RecordOrderedComparator object
     * 
     * @param keyFields indexes of fields to be considered for sorting
     * @param collator  Collator which should be use for comparing String fields
     */
	public RecordOrderedComparator(int[] keyFields, boolean[] keyOrderings, RuleBasedCollator collator) {
		super(keyFields, collator);
		this.keyOrderings = keyOrderings;
	}
	
	/**
	 *  Compares two records (of the same layout) based on defined key-fields and returns (-1;0;1) if (< ; = ; >)
	 *
	 *@param  record1  Description of the Parameter
	 *@param  record2  Description of the Parameter
	 *@return          -1 ; 0 ; 1
	 */
	@Override
	public int compare(DataRecord record1, DataRecord record2) {
        int compResult;
        /*
         * by D.Pavlis following check has been "relaxed" to speed up
         * processing. if (record1.getMetadata() != record2.getMetadata()) {
         * throw new RuntimeException("Can't compare - records have different
         * metadata associated." + " Possibly different structure"); }
         */
        if (useCollator) {
            for (int i = 0; i < keyFields.length; i++) {
                final DataField field1 = record1.getField(keyFields[i]);
                if (collators[i] != null && field1.getMetadata().getDataType() == DataFieldType.STRING) {
                    compResult = ((StringDataField) field1).compareTo(
                            record2.getField(keyFields[i]), collators[i]);
                } else {
                    compResult = field1.compareTo(record2
                            .getField(keyFields[i]));
                }
                if (!keyOrderings[i]) {
                	compResult = -compResult;
                }
                if (compResult != 0) {
                    if (equalNULLs) {
                        if (!(record1.getField(keyFields[i]).isNull() && record2.getField(keyFields[i]).isNull())) {
                            return compResult;
                        }
                        continue;
                    }
                    return compResult;
                }
            }

        } else {

            for (int i = 0; i < keyFields.length; i++) {
                compResult = record1.getField(keyFields[i]).compareTo(
                        record2.getField(keyFields[i]));
				if (!keyOrderings[i]) {
                	compResult = -compResult;
                }
                if (compResult != 0) {
                    if (equalNULLs) {
                        if (!(record1.getField(keyFields[i]).isNull() && record2.getField(keyFields[i]).isNull())) {
                            return compResult;
                        }
                        continue;
                    }
                    return compResult;
                }
            }
        }
        return 0;
        // seem to be the same
    }
	
		/**
     * Compares two records (can have different layout) based on defined
     * key-fields and returns (-1;0;1) if (< ; = ; >).<br>
     * The particular fields to be compared have to be of the same type !
     * 
     * @param secondKey
     *            RecordKey defined for the second record
     * @param record1
     *            First record
     * @param record2
     *            Second record
     * @return -1 ; 0 ; 1
     */
	@Override
	public int compare(RecordKey secondKey, DataRecord record1, DataRecord record2) {
	
		int compResult;
		int[] record2KeyFields = secondKey.getKeyFields();
		if (keyFields.length != record2KeyFields.length) {
			throw new RuntimeException("Can't compare. keys have different number of DataFields");
		}
        
         if (useCollator) {
             for (int i = 0; i < keyFields.length; i++) {
                 final DataField field1 = record1.getField(keyFields[i]);
                 if (collators[i] != null && field1.getMetadata().getDataType() == DataFieldType.STRING) {
                    compResult = ((StringDataField) field1).compareTo(
                             record2.getField(record2KeyFields[i]),collators[i]);
                 }else{
                     compResult = field1.compareTo(
                             record2.getField(record2KeyFields[i]));
                 }
                 
                 if (!keyOrderings[i]) {
                 	compResult = -compResult;
                 }
                 
                 if (compResult != 0) {
                     if (equalNULLs) {
                         if (!(record1.getField(keyFields[i]).isNull() && record2.getField(record2KeyFields[i]).isNull())) {
                             return compResult;
                         }
                         continue;
                     }
                     return compResult;
                 }
                 
            }             
             
         }else{
        
		for (int i = 0; i < keyFields.length; i++) {
		
                compResult = record1.getField(keyFields[i]).compareTo(
                        record2.getField(record2KeyFields[i]));
                        
				if (!keyOrderings[i]) {
                 	compResult = -compResult;
                 }
                
                if (compResult != 0) {
                    if (equalNULLs) {
                        if (!(record1.getField(keyFields[i]).isNull() && record2.getField(record2KeyFields[i]).isNull())) {
                            return compResult;
                        }
                        continue;
                    }
                    return compResult;
                }
            }
        }
		return 0;
		// seem to be the same
	}
	

}
