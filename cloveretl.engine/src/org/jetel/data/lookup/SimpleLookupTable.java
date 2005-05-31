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
package org.jetel.data.lookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.jetel.data.Defaults;
import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.data.HashKey;
import org.jetel.data.parser.Parser;
import org.jetel.exception.JetelException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 *  Simple lookup table which reads data from flat file and creates Map structure.
 *
 * @author     dpavlis
 * @since    May 2, 2002
 */
public class SimpleLookupTable implements LookupTable {

	private DataRecordMetadata metadata;
	private Parser dataParser;
	private Map lookupTable;
	private RecordKey indexKey;
	private HashKey lookupKey;
	private String name;
	private int numFound;
	private DataRecord lookupData;
	
	/**
	* Default capacity of HashMap when standard constructor is used.
	*/
	private final static int DEFAULT_INITIAL_CAPACITY = Defaults.Lookup.LOOKUP_INITIAL_CAPACITY;


	/**
	 *Constructor for the SimpleLookupTable object.<br>It uses HashMap class to
	 *store indexKey->data pairs in it.
	 *
	 * @param  parser    Reference to parser which should be used for parsing input data
	 * @param  metadata  Metadata describing input data
	 * @param  keys      Names of fields which comprise indexKey to lookup table
	 * @since            May 2, 2002
	 */
	public SimpleLookupTable(DataRecordMetadata metadata, String[] keys, Parser parser) {
		this(metadata,keys,parser,null);
	}


	/**
	 *Constructor for the SimpleLookupTable object.
	 *
	 * @param  parser     Reference to parser which should be used for parsing input data
	 * @param  metadata   Metadata describing input data
	 * @param  keys       Names of fields which comprise indexKey to lookup table
	 * @param  mapObject  Object implementing Map interface. It will be used to hold indexKey->data pairs
	 * @since             May 2, 2002
	 */
	public SimpleLookupTable(DataRecordMetadata metadata, String[] keys, Parser parser, Map mapObject) {
		this.dataParser = parser;
		this.metadata = metadata;
		lookupTable = mapObject;
		indexKey = new RecordKey(keys, metadata);
		indexKey.init();
	}


	/**
	 *  Looks-up data based on speficied indexKey.<br> The indexKey should be result of calling RecordKey.getKeyString()
	 *
	 * @param  keyString  Key String which will be used for lookup of data record
	 * @return            Associated DataRecord or NULL if not found
	 */
	public DataRecord get(String keyString) {
	    lookupData.getField(0).fromString(keyString);
	    return get();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#get(org.jetel.data.DataRecord)
	 */
	public DataRecord get(DataRecord keyRecord){
	    lookupKey.setDataRecord(keyRecord);
	    return get();
	}

	
	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#get(java.lang.Object[])
	 */
	public DataRecord get(Object[] keys) {
	    for(int i=0;i<keys.length;i++){
	        lookupData.getField(i).setValue(keys[i]);
	    }
	    return get();
	}
	
	private DataRecord get(){
	    DataRecord data=(DataRecord)lookupTable.get(lookupKey);
	    numFound= (data!=null ? 1 : 0);
	    return data;
	}
	
	/**
	 *  Initializtaion of lookup table - loading all data into it.
	 *
	 * @exception  IOException  Description of Exception
	 * @since                   May 2, 2002
	 */
	public void init() throws JetelException {
	    DataRecord record=new DataRecord(metadata);
	    record.init();
		
	    if (lookupTable==null){
	        lookupTable = new HashMap(DEFAULT_INITIAL_CAPACITY);
	    }
		// populate the lookupTable (Map) with data
		while (dataParser.getNext(record) != null) {
		    DataRecord storeRecord=record.duplicate();
			lookupTable.put(new HashKey(indexKey, storeRecord), storeRecord);
		}
		dataParser.close();
		numFound=0;
	}

	
	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#setLookupKey(java.lang.Object)
	 */
	public void setLookupKey(Object key){
	    if (key instanceof String){
	        // little extra code to support looking up by String key
            // will be used only if key is composed of 1 field of type STRING
	        DataRecordMetadata keyMetadata=indexKey.generateKeyRecordMetadata();
            if (indexKey.getKeyFields().length == 1 && keyMetadata.getField(0).getType() == DataFieldMetadata.STRING_FIELD) {
                lookupData = new DataRecord(keyMetadata);
                lookupData.init();
                int[] keyFields={0};
                lookupKey=new HashKey(new RecordKey(keyFields,keyMetadata),lookupData);
            }else{
                throw new RuntimeException(
                        "Can't use \""
                                + key
                                + "\" (String) for lookup - not compatible with the key defined for this lookup table !");
            }
	    }else if (key instanceof Object[]){
	        Object[] keys=(Object[])key;
	        if (indexKey.getKeyFields().length == keys.length){
	            DataRecordMetadata keyMetadata=indexKey.generateKeyRecordMetadata();
	            lookupData = new DataRecord(keyMetadata);
                lookupData.init();
                int[] keyFields=new int[keyMetadata.getNumFields()];
                for(int i=0;i<keyFields.length;keyFields[i]=i,i++);
                lookupKey=new HashKey(new RecordKey(keyFields,keyMetadata),lookupData);
	        }else{
	            throw new RuntimeException("Supplied lookup values are not compatible with the key defined for this lookup table !");
	        }
	        
	    }else if (key instanceof RecordKey){
	        // reference to DataRecord (lookupData] will be added later in get(DataRecord ) method
	        this.lookupKey=new HashKey((RecordKey)key,null);
	    }else{
	        throw new RuntimeException("Incompatible Object type specified as lookup key: "+key.getClass().getName());
	    }
	}
	
	public void close(){
	    lookupTable.clear();
	}
	
	public DataRecordMetadata getMetadata(){
	    return metadata;
	}
	
	public DataRecord getNext(){
	    return null; // only one indexKey - one record is allowed
	}
	
	public String getName(){
	    return name;
	}
	
	public int getNumFound(){
	    return numFound;
	}
	
	public int getSize(){
	    return lookupTable.size();
	}
}

