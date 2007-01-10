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
package org.jetel.lookup;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.InvalidAlgorithmParameterException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.w3c.dom.Element;

/**
 *  Simple lookup table which reads data from flat file and creates Map structure.
 *
 * The XML DTD describing the internal structure is as follows:
 * 
 *  * &lt;!ATTLIST LookupTable
 *              id ID #REQUIRED
 *              type NMTOKEN (simpleLookup) #REQUIRED
 *              metadata CDATA #REQUIRED
 *              key CDATA #REQUIRED
 *              dataType NMTOKEN (delimited | fixed) #REQUIRED
 *              fileURL CDATA #REQUIRED
 *              charset CDATA #IMPLIED
 *              initialSize CDATA #IMPLIED&gt;
 *              
 * @author     dpavlis
 * @since    May 2, 2002
 */
public class SimpleLookupTable extends GraphElement implements LookupTable {

    private static final String XML_LOOKUP_TYPE_SIMPLE_LOOKUP = "simpleLookup";
    private static final String XML_LOOKUP_INITIAL_SIZE = "initialSize";
    private static final String XML_LOOKUP_KEY = "key";
    private static final String XML_METADATA_ID ="metadata";
    private static final String XML_LOOKUP_DATA_TYPE = "dataType";
    private static final String XML_FILE_URL = "fileURL";
    private static final String XML_DATA_TYPE_DELIMITED ="delimited";
    private static final String XML_CHARSET = "charset";
    
	protected DataRecordMetadata metadata;
	protected Parser dataParser;
	protected Map lookupTable;
	protected RecordKey indexKey;
	protected HashKey lookupKey;
	protected int numFound;
	protected DataRecord lookupData;
	protected int tableInitialSize=0;
	
	/**
	* Default capacity of HashMap when standard constructor is used.
	*/
	protected final static int DEFAULT_INITIAL_CAPACITY = Defaults.Lookup.LOOKUP_INITIAL_CAPACITY;


	/**
	 *Constructor for the SimpleLookupTable object.<br>It uses HashMap class to
	 *store indexKey->data pairs in it.
	 *
	 * @param  parser    Reference to parser which should be used for parsing input data
	 * @param  metadata  Metadata describing input data
	 * @param  keys      Names of fields which comprise indexKey to lookup table
	 * @since            May 2, 2002
	 */
	public SimpleLookupTable(String id, DataRecordMetadata metadata, String[] keys, Parser parser) {
		this(id,metadata,keys,parser,null);
	}

	public SimpleLookupTable(String id, DataRecordMetadata metadata, String[] keys, Parser parser, int initialSize) {
		this(id,metadata,keys,parser,null);
		this.tableInitialSize=initialSize;
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
	public SimpleLookupTable(String id, DataRecordMetadata metadata, String[] keys, Parser parser, Map mapObject) {
        super(id);
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
	
	protected DataRecord get(){
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
	public void init() throws ComponentNotReadyException {
		super.init();
	    DataRecord record=new DataRecord(metadata);
	    record.init();
		
	    if (lookupTable==null){
	        if (tableInitialSize>0){
	            lookupTable = new HashMap(tableInitialSize);
	        }else{
	            lookupTable = new HashMap(DEFAULT_INITIAL_CAPACITY);
	        }
	    }
		/* populate the lookupTable (Map) with data
         * if provided dataParser is not null, otherwise it is assumed that the lookup
         * table will be populated later by calling put() method
         */
        
        if (dataParser != null) {
            try {
                while (dataParser.getNext(record) != null) {
                    DataRecord storeRecord = record.duplicate();
                    lookupTable.put(new HashKey(indexKey, storeRecord),
                            storeRecord);
                }
            } catch (JetelException e) {
                throw new ComponentNotReadyException(this, e.getMessage(), e);
            }
            dataParser.close();
        }
		numFound=0;
	}
	
    public static SimpleLookupTable fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
        SimpleLookupTable lookupTable = null;
        String id;
        String type;
        
        //reading obligatory attributes
        try {
            id = xattribs.getString(XML_ID_ATTRIBUTE);
            type = xattribs.getString(XML_TYPE_ATTRIBUTE);
        } catch(AttributeNotFoundException ex) {
            throw new XMLConfigurationException("Can't create lookup table - " + ex.getMessage(),ex);
        }
        
        //check type
        if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_SIMPLE_LOOKUP)) {
            throw new XMLConfigurationException("Can't create simple lookup table from type " + type);
        }
        
        //create simple lookup table
        try{
            int initialSize = xattribs.getInteger(XML_LOOKUP_INITIAL_SIZE, Defaults.Lookup.LOOKUP_INITIAL_CAPACITY);
            String[] keys = xattribs.getString(XML_LOOKUP_KEY).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
            DataRecordMetadata metadata = graph.getDataRecordMetadata(xattribs.getString(XML_METADATA_ID));
            Parser parser;
            String dataTypeStr = xattribs.getString(XML_LOOKUP_DATA_TYPE);
            
            // which data parser to use
            if(dataTypeStr.equalsIgnoreCase(XML_DATA_TYPE_DELIMITED)) {
                parser = new DelimitedDataParser(xattribs.getString(XML_CHARSET, Defaults.DataParser.DEFAULT_CHARSET_DECODER));
            } else {
                parser = new FixLenCharDataParser(xattribs.getString(XML_CHARSET, Defaults.DataParser.DEFAULT_CHARSET_DECODER));
            }
            parser.init(metadata);
            if (xattribs.exists(XML_FILE_URL)) {
            	parser.setDataSource(new FileInputStream(xattribs.getString(XML_FILE_URL)));
            }else{
            	parser = null;
            }
            lookupTable = new SimpleLookupTable(id, metadata, keys, parser, initialSize);
            
            return lookupTable;
            
         }catch(Exception ex){
             throw new XMLConfigurationException("can't create simple lookup table",ex);
         }
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
	
	public void free() {
	    if (lookupTable!=null){
	    	lookupTable.clear();
	    	lookupTable=null;
	    }
	}
	
	public DataRecordMetadata getMetadata(){
	    return metadata;
	}
	
	public DataRecord getNext(){
	    return null; // only one indexKey - one record is allowed
	}
	
	public int getNumFound(){
	    return numFound;
	}
	
	public int getSize(){
	    return lookupTable.size();
	}

    /* (non-Javadoc)
     * @see org.jetel.graph.GraphElement#checkConfig()
     */
    @Override
    public ConfigurationStatus checkConfig(ConfigurationStatus status) {
        super.checkConfig(status);
        //TODO
        return status;
    }
    
    /**
     * @param   key not used
     * @param   data Data to store in lookup table by using previously defined key
     * @see org.jetel.data.lookup.LookupTable#put(java.lang.Object, org.jetel.data.DataRecord)
     */
    public boolean put(Object key,DataRecord data){
        DataRecord storeRecord=data.duplicate();
        lookupTable.put(new HashKey(indexKey, storeRecord), storeRecord);
        return true;
    }

    /**
     * @param   key a DataRecord object which will be used (together with previously
     * defined key) for locating & deleting data)
     * @see org.jetel.data.lookup.LookupTable#remove(java.lang.Object)
     */
    public boolean remove(Object key) {
        if (key instanceof DataRecord) {
            DataRecord storeRecord = (DataRecord) key;
            return lookupTable.remove(new HashKey(indexKey, storeRecord))!=null;
        }else{
            throw new IllegalArgumentException("Requires key parameter of type "+DataRecord.class.getName());
        }
    }
    
    /* (non-Javadoc)
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<DataRecord> iterator() {
    	return lookupTable.values().iterator();
    }
}

