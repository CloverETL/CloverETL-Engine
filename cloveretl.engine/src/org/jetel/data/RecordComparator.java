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

import java.nio.ByteBuffer;
import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;

import org.jetel.enums.CollatorSensitivityType;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MiscUtils;

/**
 *  This class compares two records of the same structure (created based on
 *  the same or at least compatible metadata).<br> Used
 *  primarily for sorting data records when it can be passed
 *  into JDK's standard sorting methods.
 *
 *@author     dpavlis
 *@created    February 28, 2004
 *@since      February 28, 2004
 *@revision    $Revision$
 */
public class RecordComparator implements Comparator {

	protected int keyFields[];
    protected RuleBasedCollator[] collators;
    protected boolean useCollator = false;
    protected boolean equalNULLs = false; // specifies whether two NULLs are deemed equal
	private boolean[] sortOrderings;
	
	/**
	 *  Constructor for the RecordComparator object
	 *
	 *@param  keyField  indexes of fields to be considered for sorting
	 *@since                 May 2, 2002
	 */
	public RecordComparator(int keyFields[]) {
		this(keyFields,(RuleBasedCollator) null);
	}

    /**
     * Constructor for the RecordComparator object
     * 
     * @param keyFields indexes of fields to be considered for sorting
     * @param collator  Collator which should be use for comparing String fields
     */
	@Deprecated
    public RecordComparator(int keyFields[], RuleBasedCollator collator){
        this.keyFields = keyFields;
        sortOrderings = new boolean[keyFields.length];
        Arrays.fill(sortOrderings, true);
        
        if (useCollator = collator != null) {
            this.collators = new RuleBasedCollator[keyFields.length];
            Arrays.fill(collators, collator);
        }
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
        final DataRecord record1 = (DataRecord) o1;
        final DataRecord record2 = (DataRecord) o2;
        /*
         * by D.Pavlis following check has been "relaxed" to speed up
         * processing. if (record1.getMetadata() != record2.getMetadata()) {
         * throw new RuntimeException("Can't compare - records have different
         * metadata associated." + " Possibly different structure"); }
         */
        if (useCollator) {
            for (int i = 0; i < keyFields.length; i++) {
                final DataField field1 = record1.getField(keyFields[i]);
                if (collators[i] != null && field1.getType() == DataFieldMetadata.STRING_FIELD) {
                    compResult = ((StringDataField) field1).compareTo(
                            record2.getField(keyFields[i]), collators[i]);
                } else {
                    compResult = field1.compareTo(record2
                            .getField(keyFields[i]));
                }
                if (compResult != 0) {
                    if (equalNULLs) {
                        if (!(record1.getField(keyFields[i]).isNull && record2
                                .getField(keyFields[i]).isNull)) {
                            return orderCorrection(i, compResult);
                        }
                        continue;
                    }
                    return orderCorrection(i, compResult);
                }
            }

        } else {

            for (int i = 0; i < keyFields.length; i++) {
                compResult = record1.getField(keyFields[i]).compareTo(
                        record2.getField(keyFields[i]));

                if (compResult != 0) {
                    if (equalNULLs) {
                        if (!(record1.getField(keyFields[i]).isNull && record2
                                .getField(keyFields[i]).isNull)) {
                            return orderCorrection(i, compResult);
                        }
                        continue;
                    }
                    return orderCorrection(i, compResult);
                }
            }
        }
        return 0;
        // seem to be the same
    }

	/**
	 * Turns the compare result sign if the sort ordering is descending.
	 * @param keyField
	 * @param compResult
	 * @return
	 */
	private final int orderCorrection(int keyField, int compResult) {
		return sortOrderings[keyField] ? compResult : compResult * -1;
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
	public int compare(RecordKey secondKey, DataRecord record1, DataRecord record2) {
		int compResult;
		int[] record2KeyFields = secondKey.getKeyFields();
		if (keyFields.length != record2KeyFields.length) {
			throw new RuntimeException("Can't compare. keys have different number of DataFields");
		}
        
         if (useCollator) {
             for (int i = 0; i < keyFields.length; i++) {
                 final DataField field1 = record1.getField(keyFields[i]);
                 if (collators[i] != null && field1.getType() == DataFieldMetadata.STRING_FIELD) {
                    compResult = ((StringDataField) field1).compareTo(
                             record2.getField(record2KeyFields[i]),collators[i]);
                 }else{
                     compResult = field1.compareTo(
                             record2.getField(record2KeyFields[i]));
                 }
                 
                 if (compResult != 0) {
                     if (equalNULLs) {
                         if (!(record1.getField(keyFields[i]).isNull && record2
                                 .getField(record2KeyFields[i]).isNull)) {
                             return orderCorrection(i, compResult);
                         }
                         continue;
                     }
                     return orderCorrection(i, compResult);
                 }
                 
            }             
             
         }else{
        
		for (int i = 0; i < keyFields.length; i++) {
                compResult = record1.getField(keyFields[i]).compareTo(
                        record2.getField(record2KeyFields[i]));
                
                if (compResult != 0) {
                    if (equalNULLs) {
                        if (!(record1.getField(keyFields[i]).isNull && record2
                                .getField(record2KeyFields[i]).isNull)) {
                            return orderCorrection(i, compResult);
                        }
                        continue;
                    }
                    return orderCorrection(i, compResult);
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
			RecordComparator comp = (RecordComparator)obj;
			boolean collatorEquals = false;
			if (!useCollator){
				collatorEquals = !comp.useCollator;
			}else{
				Collator[] refCollators = comp.getCollators();
				if (collators.length == refCollators.length) {
					boolean isDifferent = false;
					for (int i=0; i<collators.length; i++) {
						if (collators[i] == refCollators[i]) continue;
						if (!collators[i].equals(refCollators[i])) {
							isDifferent = true;
							break;
						}
					}
					if (!isDifferent) collatorEquals = true;
				}
			}
			return collatorEquals && Arrays.equals(this.keyFields,comp.getKeyFields()) && 
				Arrays.equals(this.sortOrderings,comp.getSortOrderings()); 
		}else{
			return false;
		}
	}

	@Override
	public int hashCode() {
		int hashCode = Arrays.hashCode(keyFields);
		return 31*hashCode + (collators==null ? 0 : collators.hashCode());
	}
	
    public Collator getCollator() {
    	if (collators == null || collators.length == 0) return null;
        return collators[0];
    }

    public Collator[] getCollators() {
        return collators;
    }

    public void setCollator(RuleBasedCollator collator) {
    	if (collators == null) {
    		collators = new RuleBasedCollator[keyFields.length];
    	}
    	Arrays.fill(collators, collator);
    }
    
    public void setCollator(RuleBasedCollator[] collators) {
		useCollator = false;
    	if (collators == null) return;
    	for (Collator col: collators) {
    		if (col != null) {
    			useCollator = true;
    			return;
    		}
    	}
    }

    /**
     * True if two NULL values (fields with NULL flag set) are considered equal
     * 
     * @return Returns the equalNULLs.
     */
    public boolean isEqualNULLs() {
        return equalNULLs;
    }
    /**
     * Sets whether two NULL values (fields with NULL flag set) are considered equal.<br>
     * Default is false.
     * 
     * @param equalNULLs The equalNULLs to set.
     */
    public void setEqualNULLs(boolean equalNULLs) {
        this.equalNULLs = equalNULLs;
    }
    
    /**
     * Sets orderings for key fields.
     * @param sortOrderings
     */
    public void setSortOrderings(boolean[] sortOrderings) {
    	this.sortOrderings = sortOrderings;
    }
    
    /**
     * Gets orderings for key fields.
     * @return
     */
    public boolean[] getSortOrderings() {
    	return sortOrderings;
    }
    
    /**
     * This method serializes (saves) content of key fields only (for specified record) into
     * buffer.
     * 
     * @param buffer ByteBuffer into which serialize key fields
     * @param record data record from which key fields will be serialized into ByteBuffer
     */
    public void serializeKeyFields(ByteBuffer buffer,DataRecord record) {
        for (int i = 0; i < keyFields.length; i++) {
            record.getField(keyFields[i]).serialize(buffer);
        }
    }
    
    /**
     *  This method deserializes (restores) content of key fields only (for specified record) from
     * buffer.
     * 
     * @param buffer ByteBuffer from which deserialize key fields
     * @param record data record whose key fields will be deserialized from ByteBuffer
     * @since 29.1.2007
     */
    public void deserializeKeyFileds(ByteBuffer buffer,DataRecord record){
        for (int i = 0; i < keyFields.length; i++) {
            record.getField(keyFields[i]).deserialize(buffer);
        }
    }
    
    /**
     * 
     * @param metadata
     */
    public void updateCollators(DataRecordMetadata metadata) {
		Locale[] metadataLocale = getLocaleFromMetadata(metadata, keyFields);
		if (metadataLocale == null) return;
		Integer[] iSensitivity = getSensitivityFromMetadata(metadata, keyFields);
		
		if (collators == null) collators = new RuleBasedCollator[keyFields.length];
		for (int i=0; i<keyFields.length; i++) {
			if (metadataLocale[i] == null) continue;
			collators[i] = (RuleBasedCollator)Collator.getInstance(metadataLocale[i]);
			
			if (iSensitivity != null && iSensitivity[i] != null) collators[i].setStrength(iSensitivity[i].intValue());
			collators[i].setDecomposition(Collator.CANONICAL_DECOMPOSITION);
			useCollator = true;
		}
    }
    
    /**
     * Creates sensitivity array for collators.
     * @param metaData
     * @param keys
     * @return
     */
    private Integer[] getSensitivityFromMetadata(DataRecordMetadata metaData, int[] keys) {
		Integer[] sensitivities = new Integer[keys.length];
		boolean found = false;
		for (int i = 0; i < keys.length; i++) {
			String sCollatorSensitivity = metaData.getField(keys[i]).getCollatorSensitivity();
			CollatorSensitivityType type;
			if (sCollatorSensitivity != null && (type = CollatorSensitivityType.fromString(sCollatorSensitivity, null)) != null) {
				sensitivities[i] = type.getCollatorSensitivityValue();
				found = true;
			}
		}
		return found ? sensitivities : null;
    }

    /**
     * Creates locale array for collators.
     * @param metaData
     * @param keys
     * @return
     */
    private Locale[] getLocaleFromMetadata(DataRecordMetadata metaData, int[] keys) {
    	Locale[] metadataLocale = new Locale[keys.length];
    	boolean found;
    	if (found = metaData.getLocaleStr() != null) {
        	Arrays.fill(metadataLocale, MiscUtils.createLocale(metaData.getLocaleStr()));
    	}
		for (int i = 0; i < keys.length; i++) {
			metadataLocale[i] = MiscUtils.createLocale(metaData.getField(keys[i]).getLocaleStr());
			if (!found && metadataLocale[i] != null) found = true;
		}
		return found ? metadataLocale : null;
    }

}
// end RecordKey


