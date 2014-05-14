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

import org.jetel.exception.ComponentNotReadyException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.key.OrderType;
import org.jetel.util.key.RecordKeyTokens;

/**
 *  This class serves the role of DataRecords comparator.<br>
 * It can compare two records with different structure based on
 * specified fields. It is used when sorting, hashing or (in general)
 * comparing data.<br>
 * <br>
 * <i>Usage:</i><br>
 * <code>
 * key = new RecordKey(keyFieldNames,recordMetadata);<br>
 * key.init();<br>
 * key.compare(recordA,recordB);
 * </code>
 *
 * @author      dpavlis
 * @since       May 2, 2002
 * @created     January 26, 2003
 */
public class RecordOrderedKey extends RecordKey {

	private boolean keyOrderings[]; // TODO: erase this comment .... ascending is true
	private final static char KEY_ITEMS_DELIMITER = ':';
	private final static int DEFAULT_STRING_KEY_LENGTH = 32;
	private static final Object KEY_ORDERING_1ST_DELIMITER = "(";
	private static final Object KEY_ORDERING_2ND_DELIMITER = ")";
	private StringBuffer keyStr;


	/**
	 *  Constructor for the RecordOrderedKey object
	 *
	 * @param  keyFieldNames  names of individual fields composing the key
	 * @param  metadata       metadata describing structure of DataRecord for which the key is built
	 * @since                 May 2, 2002
	 */
	public RecordOrderedKey(String keyFieldNames[], DataRecordMetadata metadata) {
		super(keyFieldNames, metadata);
		this.keyOrderings = new boolean[keyFieldNames.length];
		Arrays.fill(this.keyOrderings, true);		
	}

	/**
	 *  Constructor for the RecordOrderedKey object
	 *
	 * @param  keyFieldNames  names of individual fields composing the key
	 * @param keyOrderings ordering of columns for each key (true=ascending)
	 * @param  metadata       metadata describing structure of DataRecord for which the key is built
	 */
	public RecordOrderedKey(String keyFieldNames[], boolean keyOrderings[], DataRecordMetadata metadata) {
		super(keyFieldNames, metadata);
		this.keyOrderings = keyOrderings;
	}

	public RecordOrderedKey(RecordKeyTokens keyRecordDesc, DataRecordMetadata metadata) throws ComponentNotReadyException {
		super(keyRecordDesc.getKeyFieldNames(), metadata);
		
		this.keyOrderings = new boolean[keyRecordDesc.size()];
		for (int i = 0; i < keyRecordDesc.size(); i++) {
			OrderType orderType = keyRecordDesc.getKeyField(i).getOrderType();
			if (orderType == OrderType.ASCENDING || orderType == null) {
				keyOrderings[i] = true;
			} else if (orderType == OrderType.DESCENDING) {
				keyOrderings[i] = false;
			} else {
				throw new ComponentNotReadyException("Unsupported key ordering type '" + orderType + "'.");
			}
		}
	}
	
	/**
	 *  Constructor for the RecordOrderedKey object
	 *
	 * @param  keyFieldNames  names of individual fields composing the key
	 * @param keyOrderings ordering of columns for each key (true=ascending)
	 * @param  metadata       metadata describing structure of DataRecord for which the key is built
     * @param  collator       language collator
	 */
	@Deprecated
	public RecordOrderedKey(String keyFieldNames[], boolean keyOrderings[], DataRecordMetadata metadata, RuleBasedCollator collator) {
		super(keyFieldNames, metadata);
		this.keyOrderings = keyOrderings;
		
		// if the collator could be used
		if (collators != null) {
			collators = new RuleBasedCollator[keyOrderings.length];
			Arrays.fill(collators, collator);
			useCollator = true;
		}
	}

	public RecordOrderedKey(String keyFieldNames[], boolean keyOrderings[], DataRecordMetadata metadata, RuleBasedCollator[] collators) {
		super(keyFieldNames, metadata);
		this.keyOrderings = keyOrderings;
		
		// if the collator could be used
		if (collators != null) {
			this.collators = collators;
			useCollator = true;
		}
	}

	/**
	 * @param keyFields indices of fields composing the key
	 * @param metadata metadata describing structure of DataRecord for which the key is built
	 */
	private RecordOrderedKey(int keyFields[], DataRecordMetadata metadata) {
		super (keyFields, metadata);
		this.keyOrderings = new boolean[keyFields.length];
		Arrays.fill(this.keyOrderings, true);
	}

	/**
	 * @param keyFields indices of fields composing the key
	 * @param keyOrderings ordering of columns for each key (true=ascending)
	 * @param metadata metadata describing structure of DataRecord for which the key is built
	 */
	public RecordOrderedKey(int keyFields[], boolean keyOrderings[], DataRecordMetadata metadata) {
		super (keyFields, metadata);
		this.keyOrderings = keyOrderings;
	}

	/**
	 * @param keyFields indices of fields composing the key
	 * @param keyOrderings ordering of columns for each key (true=ascending)
	 * @param metadata metadata describing structure of DataRecord for which the key is built
	 */
	@Deprecated
	public RecordOrderedKey(int keyFields[], boolean keyOrderings[], DataRecordMetadata metadata, RuleBasedCollator collator) {
		super (keyFields, metadata);
		this.keyOrderings = keyOrderings;
		
		if (collator != null) {
			collators = new RuleBasedCollator[keyOrderings.length];
			Arrays.fill(collators, collator);
			useCollator = true;
		}
	}

	/**
	 * Serializes ordered key into string, format is list of keys separated by default key separator,
	 * together with ordering inserted after each key in braces
	 * 
	 * TODO: check usage of this!
	 * 
	 * @param record	DataRecord to be used for key reading  
	 * @return
	 */
	
	public String getKeyOrderdString(DataRecord record) {
		
		if (keyStr == null) {
			keyStr = new StringBuffer(DEFAULT_STRING_KEY_LENGTH);
		} else {
			keyStr.setLength(0);			
		}
		
		for (int i = 0; i < keyFields.length; i++) {
			keyStr.append(record.getField(keyFields[i]).toString());
			keyStr.append(KEY_ORDERING_1ST_DELIMITER);
			keyStr.append(keyOrderings[i] ? "A" : "D");			
			keyStr.append(KEY_ORDERING_2ND_DELIMITER);
			keyStr.append(KEY_ITEMS_DELIMITER);
		}
		return keyStr.toString();	
	}
	
	/**
	 *  Gets the keyOrderings attribute of the RecordOrderedKey object
	 *
	 * @return    The keyOrderings value
	 */
	public boolean[] getKeyOrderings() {
		return keyOrderings;
	}
	
	/**
	 *  Compares two records (of the same layout) based on defined key-fields and returns (-1;0;1) if (< ; = ; >)
	 *
	 * @param  record1  Description of the Parameter
	 * @param  record2  Description of the Parameter
	 * @return          -1 ; 0 ; 1
	 */
	@Override
	public int compare(DataRecord record1, DataRecord record2) {
		if (record1 == record2) return 0;
		int compResult;
		if (record1.getMetadata() != record2.getMetadata()) {
			throw new RuntimeException("Can't compare - records have different metadata associated." +
					" Possibly different structure");
		}
	    DataField field;
		if (this.isEqualNULLs()){
			for (int i = 0; i < keyFields.length; i++) {
		        field = record1.getField(keyFields[i]);
				if (useCollator && collators[i] != null && (field instanceof StringDataField)) {
			        compResult = ((StringDataField)field).compareTo(record2.getField(keyFields[i]), collators[i]);
				} else {
					compResult = field.compareTo(record2.getField(keyFields[i]));
				}
		        if (!keyOrderings[i]) // Descending order
		        	compResult = -compResult;
		        if (compResult != 0) {
		            if (!(record1.getField(keyFields[i]).isNull() && record2.getField(keyFields[i]).isNull())) {
		                return compResult;
		            }
		        }
		    }
		}else {
		    for (int i = 0; i < keyFields.length; i++) {
		        field = record1.getField(keyFields[i]);
				if (useCollator && collators[i] != null && (field instanceof StringDataField)) {
			        compResult = ((StringDataField)field).compareTo(record2.getField(keyFields[i]), collators[i]);
				} else {
					compResult = field.compareTo(record2.getField(keyFields[i]));
				}
		        if (!keyOrderings[i]) // Descending order
		        	compResult = -compResult;
		        if (compResult != 0) {
		            return compResult;
		        }
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
	 * @param  secondKey  RecordKey defined for the second record
	 * @param  record1    First record
	 * @param  record2    Second record
	 * @return            -1 ; 0 ; 1
	 */
	@Override
	public int compare(RecordKey secondKey, DataRecord record1, DataRecord record2) {
		if (record1 == record2) return 0;
		int compResult;
		int[] record2KeyFields = secondKey.getKeyFields();
		if (keyFields.length != record2KeyFields.length) {
			throw new RuntimeException("Can't compare. Keys have different number of DataFields");
		}
		DataField field;
		if (this.isEqualNULLs()){
		    for (int i = 0; i < keyFields.length; i++) {
		    	field = record1.getField(keyFields[i]);
		    	if (useCollator && collators[i] != null && (field instanceof StringDataField)) {
		    		compResult = ((StringDataField)field).compareTo(record2.getField(record2KeyFields[i]), collators[i]);
		    	} else {
		    		compResult = field.compareTo(record2.getField(record2KeyFields[i]));
		    	}
		        if (!keyOrderings[i]) { // Descending key
		        	compResult = -compResult;
		        }
		        if (compResult != 0) {
		            if (!(record1.getField(keyFields[i]).isNull() && record2.getField(keyFields[i]).isNull())) {
		                return compResult;
		            }
		        }
		    }
		}else{
		    for (int i = 0; i < keyFields.length; i++) {
		    	field = record1.getField(keyFields[i]);
		    	if (useCollator && collators[i] != null && (field instanceof StringDataField)) {
		    		compResult = ((StringDataField)field).compareTo(record2.getField(record2KeyFields[i]), collators[i]);
		    	} else {
		    		compResult = field.compareTo(record2.getField(record2KeyFields[i]));
		    	}
		        if (!keyOrderings[i]) { // Descending key
		        	compResult = -compResult;
		        }
		        if (compResult != 0) {
		            return compResult;
		        }
		    }
		}
		return 0;
		// seem to be the same
	}



}
// end RecordOrderedKey
