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

import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.jetel.enums.CollatorSensitivityType;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.GraphElement;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.MiscUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.string.StringUtils;

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
public class RecordKey {

	protected int keyFields[];
	protected DataRecordMetadata metadata;
	protected String keyFieldNames[];
	private final static char KEY_ITEMS_DELIMITER = ':';
	private final static int DEFAULT_STRING_KEY_LENGTH = 32;
	private boolean isInitialized = false;
    protected RuleBasedCollator[] collators;
    boolean useCollator = false;

	private StringBuffer keyStr;

	private boolean equalNULLs = false; // specifies whether two NULLs are deemed equal

	private boolean comparedNulls = false; // XXX Temporary workaround until compareTo() will throw exception
	
	/**
	 *  Constructor for the RecordKey object
	 *
	 * @param  keyFieldNames  names of individual fields composing the key
	 * @param  metadata       metadata describing structure of DataRecord for which the key is built
	 * @since                 May 2, 2002
	 */
	public RecordKey(String keyFieldNames[], DataRecordMetadata metadata) {
		this.metadata = metadata;
		this.keyFieldNames = keyFieldNames;
	}

	/**
	 * @param keyFields indices of fields composing the key
	 * @param metadata metadata describing structure of DataRecord for which the key is built
	 */
	public RecordKey(int keyFields[], DataRecordMetadata metadata) {
		this.metadata = metadata;
		this.keyFields = keyFields;
	}
	
	// end init

    public DataRecordMetadata getMetadata() {
        return metadata;
    }
    
	/**
	 *  Assembles string representation of the key based on current record's value.
	 *
	 * @param  record  DataRecord whose field's values will be used to create key string.
	 * @return         The KeyString value
	 * @since          May 2, 2002
	 */
	public String getKeyString(DataRecord record) {
		return getKeyString(record, "");
	}

	/**
	 *  Assembles delimited string values of the key fields.
	 *
	 * @param  record  DataRecord whose field's values will be used to create key string.
	 * @param  delimiter delimiter inserted between individual field values.
	 * @return         The KeyString value with delimiters
	 * @since          April, 2012
	 */
	public String getKeyString(DataRecord record, String delimiter) {
		
		if (keyStr == null){ 
			keyStr = new StringBuffer(DEFAULT_STRING_KEY_LENGTH);
		}else{ 
			keyStr.setLength(0); 
		}

		if (keyFields.length > 0) {
			keyStr.append(record.getField(keyFields[0]).toString());
		}
		for (int i = 1; i < keyFields.length; i++) {
			keyStr.append(delimiter);
			keyStr.append(record.getField(keyFields[i]).toString());
		}
		return keyStr.toString();
	}

	/**
	 *  Performs initialization of internal data structures
	 *
	 * @since    May 2, 2002
	 */
	public void init() {
		
		if (isInitialized) return;

        if (metadata == null) {
        	throw new NullPointerException("Metadata are null.");
        }

	    if (keyFields == null) {
            Integer position;
            keyFields = new int[keyFieldNames.length];
            Map<String, Integer> fields = metadata.getFieldNamesMap();

            for (int i = 0; i < keyFieldNames.length; i++) {
                if ((position = fields.get(keyFieldNames[i])) != null) {
                    keyFields[i] = position.intValue();
                } else {
                    throw new RuntimeException(
                            "Field name specified as a key doesn't exist: "
                                    + keyFieldNames[i]);
                }
            }
        }else if (keyFieldNames==null){
            keyFieldNames=new String[keyFields.length];
            for (int i=0;i<keyFields.length;i++){
                keyFieldNames[i]=metadata.getField(keyFields[i]).getName();
            }
        }
	    
	    // update collators from field metadata
	    updateCollators(metadata, keyFields);

	    isInitialized = true;
	}


	/**
	 *  Gets the keyFields attribute of the RecordKey object
	 *
	 * @return    The keyFields value
	 */
	public int[] getKeyFields() {
		if (keyFields == null){
			keyFields = getKeyFieldsIndexes(this.metadata, this.keyFieldNames);
		}
		return keyFields;
	}

    
    private int[] getKeyFieldsIndexes(DataRecordMetadata mdata, String[] fieldNames) {
    	int[] indexes = new int[fieldNames.length];
    	Map<String, Integer> namesMap = mdata.getFieldNamesMap();
    	for (int i=0; i<fieldNames.length; i++){
    		indexes[i] = namesMap.get(fieldNames[i]);
    	}
		return indexes;
	}

	/**
     * Gets fields (indexes) which are not part of the key
     * 
     * @return
     * @since 31.1.2007
     */
    public int[] getNonKeyFields(){
        Set<Integer> allFields=new LinkedHashSet<Integer>();
        for(int i=0;i<metadata.getNumFields();i++){
            allFields.add(Integer.valueOf(i));
        }
        allFields.removeAll(Arrays.asList(keyFields));
        int[] nonKey=new int[allFields.size()];
        int counter=0;
        for(Integer index : allFields){
            nonKey[counter++]=index.intValue();
        }
        return nonKey;
    }

	/**
	 * Gets number of fields defined by this key.
	 * @return length of key
	 */
	public int getLength() {
	    return keyFields.length;
	}
	
	/**
	 *  Compares two records (of the same layout) based on defined key-fields and returns (-1;0;1) if (< ; = ; >)
	 *
	 * @param  record1  Description of the Parameter
	 * @param  record2  Description of the Parameter
	 * @return          -1 ; 0 ; 1
	 */
	public int compare(DataRecord record1, DataRecord record2) {
		if (record1 == record2) return 0;
		int compResult;
		if (!record1.getMetadata().equals(record2.getMetadata())) {
			throw new RuntimeException("Can't compare - records have different metadata associated.");
		}
    	comparedNulls = false;
		if (equalNULLs){
		    for (int i = 0; i < keyFields.length; i++) {
		    	DataField field = record1.getField(keyFields[i]);
				if (useCollator && collators[i] != null && (field instanceof StringDataField)) {
			        compResult = ((StringDataField)field).compareTo(record2.getField(keyFields[i]), collators[i]);
				} else {
					compResult = field.compareTo(record2.getField(keyFields[i]));
				}
		        if (compResult != 0) {
		            if (!(record1.getField(keyFields[i]).isNull&&record2.getField(keyFields[i]).isNull)){
		                return compResult;
		            }
		        }
		    }
		}else {
		    for (int i = 0; i < keyFields.length; i++) {
		    	DataField field = record1.getField(keyFields[i]);
				if (useCollator && collators[i] != null && (field instanceof StringDataField)) {
			        compResult = ((StringDataField)field).compareTo(record2.getField(keyFields[i]), collators[i]);
				} else {
					compResult = field.compareTo(record2.getField(keyFields[i]));
				}
		        if (compResult != 0) {
		            if (record1.getField(keyFields[i]).isNull && record2.getField(keyFields[i]).isNull) {
		            	comparedNulls = true;
		            }
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
	public int compare(RecordKey secondKey, DataRecord record1, DataRecord record2) {
		if (record1 == record2) return 0;
		int compResult;
		int[] record2KeyFields = secondKey.getKeyFields();
		if (keyFields.length != record2KeyFields.length) {
			throw new RuntimeException("Can't compare. Keys have different number of DataFields");
		}

		comparedNulls = false;
		if (equalNULLs){
		    for (int i = 0; i < keyFields.length; i++) {
		    	DataField field = record1.getField(keyFields[i]);
				if (useCollator && collators[i] != null && (field instanceof StringDataField)) {
			        compResult = ((StringDataField)field).compareTo(record2.getField(record2KeyFields[i]), collators[i]);
				} else {
					compResult = field.compareTo(record2.getField(record2KeyFields[i]));
				}
		        if (compResult != 0) {
		            if (!(record1.getField(keyFields[i]).isNull&&record2.getField(record2KeyFields[i]).isNull)){
		                return compResult;
		            }
		        }
		    }
		}else{
		    for (int i = 0; i < keyFields.length; i++) {
		    	DataField field = record1.getField(keyFields[i]);
				if (useCollator && collators[i] != null && (field instanceof StringDataField)) {
			        compResult = ((StringDataField)field).compareTo(record2.getField(record2KeyFields[i]), collators[i]);
				} else {
					compResult = field.compareTo(record2.getField(record2KeyFields[i]));
				}
		        if (compResult != 0) {
		            if (record1.getField(keyFields[i]).isNull && record2.getField(record2KeyFields[i]).isNull) {
		            	comparedNulls = true;
		            }
		            return compResult;
		        }
		    }
		}
		return 0;
		// seem to be the same
	}
	
	/**
	 * @deprecated Temporary workaround until compareTo() will throw exception.
	 * @return true if equalNULLs == false and last call to {@link #compare(DataRecord, DataRecord)}
	 * returned -1 because of comparison of two null valued fields.
	 */
	public boolean isComparedNulls() {
		return comparedNulls;
	}
	
	/**
	 *  Compares two records (can have different layout) based on defined key-fields.<br>
	 *  
	 * @param  secondKey  RecordKey defined for the second record
	 * @param  record1    First record
	 * @param  record2    Second record
	 * @return true/false
	 */
	public boolean equals(RecordKey secondKey, DataRecord record1, DataRecord record2) {
		if (record1 == record2) return true;
		int[] record2KeyFields = secondKey.getKeyFields();
		if (keyFields.length != record2KeyFields.length) { // records can not be equals based on defined key-fields
			return false;
		}
		if (equalNULLs) {
			for (int i = 0; i < keyFields.length; i++) {
				DataField field1 = record1.getField(keyFields[i]);
		    	DataField field2 = record2.getField(record2KeyFields[i]);
				if (!field1.equals(field2)) {
					if (!(field1.isNull() && field2.isNull())) {
						return false;
					}
				}
			}
		} else {
			for (int i = 0; i < keyFields.length; i++) {
				DataField field1 = record1.getField(keyFields[i]);
		    	DataField field2 = record2.getField(record2KeyFields[i]);
				if (!field1.equals(field2)) {
					return false;
				}
			}
		}
		return true;
	}


	/**
	 *  Description of the Method
	 *
	 * @param  record1  Description of the Parameter
	 * @param  record2  Description of the Parameter
	 * @return          Description of the Return Value
	 */
	public boolean equals(DataRecord record1, DataRecord record2) {
		if (record1.getMetadata() != record2.getMetadata()) {
			throw new RuntimeException("Can't compare - records have different metadata associated." +
					" Possibly different structure");
		}
		if (equalNULLs){
		    for (int i = 0; i < keyFields.length; i++) {
		    	DataField field1 = record1.getField(keyFields[i]);
		    	DataField field2 = record2.getField(keyFields[i]);
				if (useCollator && collators[i] != null && field1 instanceof StringDataField && field2 instanceof StringDataField) {
					Object o1 = field1.getValue();
					Object o2 = field2.getValue();
			        if ((o1 == null || o2 == null) && !collators[i].equals(o1.toString(), o2.toString())) {
						if (!(field1.isNull() && field2.isNull())) {
							return false;
						}
			        }
				}

		        if (!field1.equals(field2)) {
		            if (!(field1.isNull && field2.isNull)){
		                return false;
		            }
		        }
		    }
		}else{
		    for (int i = 0; i < keyFields.length; i++) {
		    	DataField field1 = record1.getField(keyFields[i]);
		    	DataField field2 = record2.getField(keyFields[i]);
				if (useCollator && collators[i] != null && field1 instanceof StringDataField && field2 instanceof StringDataField) {
					Object o1 = field1.getValue();
					Object o2 = field2.getValue();
					if (o1 == null || o2 == null) return false;
			        if (!collators[i].equals(o1.toString(), o2.toString())) {
			            return false;
			        }
				} else if (!field1.equals(field2)) {
		            return false;
		        }
		    }
		}
		return true;
	}
	

	/**
	 * This method serializes (saves) content of key fields only (for specified record) into
	 * buffer.
	 * 
	 * @param buffer ByteBuffer into which serialize key fields
	 * @param record data record from which key fields will be serialized into ByteBuffer
	 */
	public void serializeKeyFields(CloverBuffer buffer,DataRecord record) {
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
    public void deserializeKeyFileds(CloverBuffer buffer,DataRecord record){
        for (int i = 0; i < keyFields.length; i++) {
            record.getField(keyFields[i]).deserialize(buffer);
        }
    }
    
    
	/**
	 * This method creates DataRecordMetadata object which represents fields composing this key. It can
	 * be used for creating data record composed from key fields only.
	 * @return DataRecordMetadata object
	 */
	public DataRecordMetadata generateKeyRecordMetadata(){
		DataRecordMetadata metadata = new DataRecordMetadata(this.metadata.getName()+"key");
		for (int i = 0; i < keyFields.length; i++) {
			metadata.addField(this.metadata.getField(keyFields[i]).duplicate());
		}
		return metadata;
	}
	
	/**
     * toString method: creates a String representation of the object
     * @return the String representation
     */
    @Override
	public String toString() {
        StringBuffer buffer = new StringBuffer();
        buffer.append("RecordKey[");
        if (keyFields == null) {
            buffer.append("keyFields = ").append("null");
        } else {
            buffer.append("keyFields = ").append("[");
            for (int i = 0; i < keyFields.length; i++) {
                if (i != 0) {
                    buffer.append(", ");
                }
                buffer.append(keyFields[i]);
            }
            buffer.append("]");
        }
        buffer.append(", metadata = ").append(metadata.toString());
        if (keyFieldNames == null) {
            buffer.append(", keyFieldNames = ").append("null");
        } else {
            buffer.append(", keyFieldNames = ").append(Arrays.asList(keyFieldNames).toString());
        }
        buffer.append(", KEY_ITEMS_DELIMITER = ").append(KEY_ITEMS_DELIMITER);
        buffer.append(", DEFAULT_KEY_LENGTH = ").append(DEFAULT_STRING_KEY_LENGTH);
        buffer.append(", EQUAL_NULLS = ").append(equalNULLs);
        buffer.append(", keyStr = ").append(keyStr);
        buffer.append("]");
        return buffer.toString();
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
     * This method checks if two RecordKeys are comparable
     * 
     * @param secondKey
     * @return Integer array with numbers of incomparable fields, odd numbers are from this key 
     * 	and even numbers are from second key. When lenghts of the keys differ, proper numbers are
     * 	returned as null. When keys are comparable there is returned array of length zero.
     */
    private Integer[] getIncomparableFields(RecordKey secondKey){
    	List<Integer> incomparable = new ArrayList<Integer>();
		int[] record2KeyFields = secondKey.getKeyFields();
		DataRecordMetadata secondMetadata = secondKey.metadata;
		for (int i = 0; i < Math.max(keyFields.length, record2KeyFields.length); i++) {
			if (i<keyFields.length && i<record2KeyFields.length) {
				if (metadata.getDataFieldType(keyFields[i]) != secondMetadata.getDataFieldType(record2KeyFields[i])) {
					incomparable.add(keyFields[i]);
					incomparable.add(record2KeyFields[i]);
				}
			}else if (i>=keyFields.length) {
				incomparable.add(null);
				incomparable.add(record2KeyFields[i]);
			}else {
				incomparable.add(keyFields[i]);
				incomparable.add(null);
			}
		}
		
    	return incomparable.toArray(new Integer[0]);
    }
    
    /**
     * This method checks if two keys are comparable
     * 
     * @param masterKey first key
     * @param masterAtrribute xml attribute corresponding to first key
     * @param slaveKey second key
     * @param slaveAttribute xml attribute corresponding to second key
     * @param status status to save problems
     * @param component owner of status 
     * @return
     */
    public static ConfigurationStatus checkKeys(RecordKey masterKey, String masterAttribute, 
    		RecordKey slaveKey, String slaveAttribute, ConfigurationStatus status, GraphElement component) {
    	
    	boolean isNull = false;
    	Integer[] incomparable = null;
    	ConfigurationProblem problem;
    	
    	//slave key is null
    	if (slaveKey == null) {
    		problem = new ConfigurationProblem("Slave key does not exist.", Severity.ERROR, component, Priority.NORMAL, masterAttribute );
    		status.add(problem);
    		return status;
    	}
    	
    	//init master key
    	try{
    		masterKey.init();
    	}catch(NullPointerException e) {
    		problem = new ConfigurationProblem("Key metadata are null.",Severity.WARNING, component, Priority.NORMAL, masterAttribute );
    		status.add(problem);
    		incomparable = new Integer[0];
    		isNull = true;
    	}catch(RuntimeException e) {
    		problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, component, Priority.NORMAL, masterAttribute); 
    		status.add(problem);
    		incomparable = new Integer[0];
    	}
    	//init slave key
    	try{
    		slaveKey.init();
    	}catch(NullPointerException e) {
    		problem = new ConfigurationProblem("Slave metadata are null.",Severity.WARNING, component, Priority.NORMAL, masterAttribute );
    		status.add(problem);
    		incomparable = new Integer[0];
    		isNull = true;
    	}catch(RuntimeException e) {
    		problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, component, Priority.NORMAL, masterAttribute);
    		status.add(problem);
    		incomparable = new Integer[0];
    	}
    	
    	//if one of metadata are null check lengths of keys
    	if (isNull) {
    		int masterLength = masterKey.keyFields != null ? masterKey.keyFields.length :
    			masterKey.keyFieldNames.length;
    		int slaveLength = slaveKey.keyFields != null ? slaveKey.keyFields.length :
    			slaveKey.keyFieldNames.length;
    		if (!(masterLength == slaveLength)) {
    			problem = new ConfigurationProblem("Keys have different number of DataFields", Severity.ERROR, component, Priority.NORMAL, slaveAttribute);
    			status.add(problem);
    		}
    	}
    	
    	//check if key fields are comparable
    	String message;
    	Integer d,s;
    	if (incomparable == null) {
    		incomparable = masterKey.getIncomparableFields(slaveKey);
    	}
		for (int i = 0; i < incomparable.length; i+=2) {
			d = incomparable[i];
			s = incomparable[i+1];
			message = "Field "
					+ (d != null ? StringUtils.quote(masterKey.metadata.getName() + '.' + masterKey.metadata.getField(d).getName())
							+ " (" + masterKey.metadata.getDataFieldType(d).getName()	+ ")" 
						: "null")
					+ " is not comparable with field "
					+ (s != null ? StringUtils.quote(slaveKey.metadata.getName() + '.' + slaveKey.metadata.getField(s).getName())
							+ " ("	+ slaveKey.metadata.getDataFieldType(s).getName()	+ ")" 
						: "null");
			if (d == null || s == null) {
				problem = new ConfigurationProblem(message, Severity.ERROR, component, Priority.NORMAL);
			}else {
				problem = new ConfigurationProblem(message, Severity.WARNING, component, Priority.NORMAL);
			}
			problem.setAttributeName(slaveAttribute);
			status.add(problem);
		}
   	
    	return status;
    }

	public String[] getKeyFieldNames() {
		return keyFieldNames;
	}
	
	@Deprecated
    public void setCollator(RuleBasedCollator collator) {
    	int len = keyFields != null ? keyFields.length : keyFieldNames.length;
    	collators = new RuleBasedCollator[len];
    	Arrays.fill(collators, collator);
    	useCollator = collators != null;
    }
	
	/**
	 * Utility method for identifying the first different data field.
	 * @param record1
	 * @param record2
	 * @return First data field of <code>record1</code> which causes that <code>compare(record1, record2)</code> 
	 * does not return 0. Returns null in case that both records are equal or given records have different metadata!
	 */
	public DataField getFieldViolatingEquals(DataRecord record1, DataRecord record2) {
		
		if (record1 == record2 || !record1.getMetadata().equals(record2.getMetadata())) {
			return null;
		}
		int compResult;
    	comparedNulls = false;
		if (equalNULLs){
		    for (int i = 0; i < keyFields.length; i++) {
		    	DataField field = record1.getField(keyFields[i]);
				if (useCollator && collators[i] != null && (field instanceof StringDataField)) {
			        compResult = ((StringDataField)field).compareTo(record2.getField(keyFields[i]), collators[i]);
				} else {
					compResult = field.compareTo(record2.getField(keyFields[i]));
				}
		        if (compResult != 0) {
		            if (!(record1.getField(keyFields[i]).isNull&&record2.getField(keyFields[i]).isNull)){
		                return field;
		            }
		        }
		    }
		} else {
		    for (int i = 0; i < keyFields.length; i++) {
		    	DataField field = record1.getField(keyFields[i]);
				if (useCollator && collators[i] != null && (field instanceof StringDataField)) {
			        compResult = ((StringDataField)field).compareTo(record2.getField(keyFields[i]), collators[i]);
				} else {
					compResult = field.compareTo(record2.getField(keyFields[i]));
				}
		        if (compResult != 0) {
		            if (record1.getField(keyFields[i]).isNull && record2.getField(keyFields[i]).isNull) {
		            	comparedNulls = true;
		            }
		            return field;
		        }
		    }
		}
		return null;
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
			String sLocale = metaData.getField(keys[i]).getLocaleStr();
			if (sLocale == null) continue;

			metadataLocale[i] = MiscUtils.createLocale(sLocale);
			if (!found && metadataLocale[i] != null) found = true;
		}
		return found ? metadataLocale : null;
    }
    
	/**
	 * Creates collator array.
	 * @param metadata
	 * @return
	 */
	private void updateCollators(DataRecordMetadata metadata, int[] keys) {
		Locale[] metadataLocale = getLocaleFromMetadata(metadata, keys);
		if (metadataLocale == null) return;
		Integer[] iSensitivity = getSensitivityFromMetadata(metadata, keys);
		
		if (collators == null) collators = new RuleBasedCollator[keys.length];
		for (int i=0; i<keys.length; i++) {
			//collator was prepared from outside the recordkey and this collator has higher priority
			if (collators[i] != null || metadataLocale[i] == null) continue;
			collators[i] = (RuleBasedCollator)Collator.getInstance(metadataLocale[i]);
			
			if (iSensitivity != null && iSensitivity[i] != null) collators[i].setStrength(iSensitivity[i].intValue());
			collators[i].setDecomposition(Collator.CANONICAL_DECOMPOSITION);
			useCollator = true;
		}
	}
}
// end RecordKey


