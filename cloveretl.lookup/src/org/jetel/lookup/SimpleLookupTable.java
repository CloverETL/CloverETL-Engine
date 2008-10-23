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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.parser.DataParser;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenByteDataParser;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.NotInitializedException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
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
public class SimpleLookupTable extends AbstractLookupTable {

    private static final String XML_LOOKUP_TYPE_SIMPLE_LOOKUP = "simpleLookup";
    private static final String XML_LOOKUP_INITIAL_SIZE = "initialSize";
    private static final String XML_LOOKUP_KEY = "key";
    private static final String XML_FILE_URL = "fileURL";
    private static final String XML_CHARSET = "charset";
	private static final String XML_BYTEMODE_ATTRIBUTE = "byteMode";
	private static final String XML_DATA_ATTRIBUTE = "data";
	
    private final static String[] REQUESTED_ATTRIBUTE = {XML_ID_ATTRIBUTE, XML_TYPE_ATTRIBUTE, XML_METADATA_ID,
    	XML_LOOKUP_KEY
    };

    protected String metadataName;
	protected DataRecordMetadata metadata;
	protected String fileURL;
	protected String charset;
	protected boolean byteMode = false;
	protected Parser dataParser;
	protected Map<HashKey, DataRecord> lookupTable;
	protected String[] keys;
	protected RecordKey indexKey;
	protected HashKey lookupKey;
	protected int numFound;
	protected DataRecord lookupData;
	protected int tableInitialSize=DEFAULT_INITIAL_CAPACITY;
	
	// data of the lookup table, can be used instead of an input file
	protected String data;
	
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

	public SimpleLookupTable(String id, String metadataName, String[] keys, int initialSize){
		super(id);
		this.metadataName = metadataName;
		this.keys = keys;
		this.tableInitialSize = initialSize;
	}

	/**
	 *Constructor for the SimpleLookupTable object.
	 *
	 * @param  parser     Reference to not-initialized parser which should be used for parsing input data
	 * @param  metadata   Metadata describing input data
	 * @param  keys       Names of fields which comprise indexKey to lookup table
	 * @param  mapObject  Object implementing Map interface. It will be used to hold indexKey->data pairs
	 * @since             May 2, 2002
	 */
	public SimpleLookupTable(String id, DataRecordMetadata metadata, String[] keys, Parser parser, Map<HashKey, DataRecord> mapObject) {
        super(id);
		this.dataParser = parser;
		this.metadata = metadata;
		lookupTable = mapObject;
		indexKey = new RecordKey(keys, metadata);
		indexKey.init();
	}


    public DataRecord get(HashKey lookupKey) {
        if (lookupKey == null) {
            throw new NullPointerException("lookupKey");
        }

        setLookupKey(lookupKey.getRecordKey());

        return get(lookupKey.getDataRecord());
    }

	/**
	 *  Looks-up data based on speficied indexKey.<br> The indexKey should be result of calling RecordKey.getKeyString()
	 *
	 * @param  keyString  Key String which will be used for lookup of data record
	 * @return            Associated DataRecord or NULL if not found
	 */
	public DataRecord get(String keyString) {
		
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}
		
		if (lookupData==null) throw new RuntimeException("Lookup key was not set/defined for lookup \""+this.getId()+"\"");
	    lookupData.getField(0).fromString(keyString);
	    return get();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#get(org.jetel.data.DataRecord)
	 */
	public DataRecord get(DataRecord keyRecord){
		
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}
		
		if (lookupKey==null) throw new RuntimeException("Lookup key was not set/defined for lookup \""+this.getId()+"\"");
	    lookupKey.setDataRecord(keyRecord);
	    return get();
	}

	
	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#get(java.lang.Object[])
	 */
	public DataRecord get(Object[] keys) {
		
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}
		
		if (lookupData==null) throw new RuntimeException("Lookup key was not set/defined for lookup \""+this.getId()+"\"");
		for(int i=0;i<keys.length;i++){
	        lookupData.getField(i).setValue(keys[i]);
	    }
	    return get();
	}

    protected DataRecord get(){
        DataRecord data = lookupTable.get(lookupKey);
        numFound= (data!=null ? 1 : 0);
        return data;
    }
    
	/**
	 *  Initializtaion of lookup table - loading all data into it.
	 *
	 * @exception  IOException  Description of Exception
	 * @since                   May 2, 2002
	 */
	synchronized public void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();

		if (metadata == null) {
			metadata = getGraph().getDataRecordMetadata(metadataName);
		}		
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata " + StringUtils.quote(metadataName) + 
					" does not exist!!!");
		}
		
        if (indexKey == null) {
        	indexKey = new RecordKey(keys, metadata);
        }
        indexKey.init();
        
		DataRecord record=new DataRecord(metadata);
	    record.init();
		
	    if (lookupTable==null){
	        lookupTable = new HashMap<HashKey, DataRecord>(tableInitialSize);
	    }
	    
	    if (charset == null) {
	    	charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
	    }
	    if (dataParser == null && (fileURL != null || data!= null)) {
    		switch (metadata.getRecType()) {
			case DataRecordMetadata.DELIMITED_RECORD:
				dataParser = new DelimitedDataParser(charset);
				break;
			case DataRecordMetadata.FIXEDLEN_RECORD:
				dataParser = byteMode ? new FixLenByteDataParser(charset) : new FixLenCharDataParser(charset);
				break;
			case DataRecordMetadata.MIXED_RECORD:
	    		dataParser = new DataParser(charset);
				break;
			default:
				throw new ComponentNotReadyException(this, XML_METADATA_ID, "Unknown metadata type: " + metadata.getRecType());
			}
	    }
        
		/* populate the lookupTable (Map) with data
         * if provided dataParser is not null, otherwise it is assumed that the lookup
         * table will be populated later by calling put() method
         */
        
        if (dataParser != null) {
            dataParser.init(metadata);
            try {
				if (fileURL != null) {
					dataParser.setDataSource(FileUtils.getReadableChannel(
							getGraph() != null ? getGraph().getProjectURL() : null,
							fileURL));
				} else if (data != null) {
					dataParser.setDataSource(new ByteArrayInputStream(data.getBytes(charset)));
				}
				while (dataParser.getNext(record) != null) {
	                    DataRecord storeRecord = record.duplicate();
	                    lookupTable.put(new HashKey(indexKey, storeRecord), storeRecord);
	            }
            } catch (Exception e) {
                throw new ComponentNotReadyException(this, e.getMessage(), e);
            }
            dataParser.close();
        }
		numFound=0;
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		DataRecord record=new DataRecord(metadata);
	    record.init();
		lookupTable.clear();
	    //read records from file
        if (dataParser != null) {
            dataParser.reset();
            try {
				if (fileURL != null) {
					dataParser.setDataSource(FileUtils.getReadableChannel(
							getGraph() != null ? getGraph().getProjectURL() : null, 
							fileURL));
				}else if (data != null) {
					dataParser.setDataSource(new ByteArrayInputStream(data.getBytes()));
				}                
				while (dataParser.getNext(record) != null) {
                    DataRecord storeRecord = record.duplicate();
                    lookupTable.put(new HashKey(indexKey, storeRecord), storeRecord);
                }
            } catch (Exception e) {
                throw new ComponentNotReadyException(this, e.getMessage(), e);
            }
            dataParser.close();
        }
		numFound=0;
	}

    public static SimpleLookupTable fromProperties(TypedProperties properties) throws AttributeNotFoundException,
			GraphConfigurationException {

    	for (String property : REQUESTED_ATTRIBUTE) {
			if (!properties.containsKey(property)) {
				throw new AttributeNotFoundException(property);
			}
		}
    	String type = properties.getProperty(XML_TYPE_ATTRIBUTE);
    	if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_SIMPLE_LOOKUP)){
    		throw new GraphConfigurationException("Can't create simple lookup table from type " + type);
    	}
        int initialSize = properties.getIntProperty(XML_LOOKUP_INITIAL_SIZE, Defaults.Lookup.LOOKUP_INITIAL_CAPACITY);
        String[] keys = properties.getStringProperty(XML_LOOKUP_KEY).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        String metadata = properties.getStringProperty(XML_METADATA_ID);
        
        SimpleLookupTable lookupTable = new SimpleLookupTable(properties.getStringProperty(XML_ID_ATTRIBUTE), metadata, keys, 
        		initialSize);
        
        if (properties.containsKey(XML_NAME_ATTRIBUTE)){
        	lookupTable.setName(properties.getStringProperty(XML_NAME_ATTRIBUTE));
        }
        if (properties.containsKey(XML_FILE_URL)) {
        	lookupTable.setFileURL(properties.getStringProperty(XML_FILE_URL));
        }
        if (properties.containsKey(XML_CHARSET)) {
        	lookupTable.setCharset(properties.getStringProperty(XML_CHARSET));
        }
        if (properties.containsKey(XML_BYTEMODE_ATTRIBUTE)){
        	lookupTable.setByteMode(properties.getBooleanProperty(XML_BYTEMODE_ATTRIBUTE));
        }
        if (properties.containsKey(XML_DATA_ATTRIBUTE)) {
        	lookupTable.setData(properties.getStringProperty(XML_DATA_ATTRIBUTE));
        }
        
        return lookupTable;
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
            String metadata = xattribs.getString(XML_METADATA_ID);
            
            lookupTable = new SimpleLookupTable(id, metadata, keys, initialSize);
            
            if (xattribs.exists(XML_NAME_ATTRIBUTE)){
            	lookupTable.setName(xattribs.getString(XML_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_FILE_URL)) {
            	lookupTable.setFileURL(xattribs.getString(XML_FILE_URL));
            }
            if (xattribs.exists(XML_CHARSET)) {
            	lookupTable.setCharset(xattribs.getString(XML_CHARSET));
            }
            if (xattribs.exists(XML_BYTEMODE_ATTRIBUTE)){
            	lookupTable.setByteMode(xattribs.getBoolean(XML_BYTEMODE_ATTRIBUTE));
            }
            if (xattribs.exists(XML_DATA_ATTRIBUTE)) {
            	lookupTable.setData(xattribs.getString(XML_DATA_ATTRIBUTE));
            }
            
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
	    	if ((this.lookupKey == null) || (key != this.lookupKey.getRecordKey())) {
	    		this.lookupKey=new HashKey((RecordKey)key,null);
	    	}
	    }else{
	        throw new RuntimeException("Incompatible Object type specified as lookup key: "+key.getClass().getName());
	    }
	}
	
	synchronized public void free() {
        if(!isInitialized()) return;
        super.free();

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

		if (metadata == null) {
			metadata = getGraph().getDataRecordMetadata(metadataName);
		}		
        if (metadata == null) {
        	status.add(new ConfigurationProblem("Metadata " + StringUtils.quote(metadataName) + 
					" does not exist!!!", Severity.ERROR, this, Priority.NORMAL, XML_METADATA_ID));
        }
        
        if (indexKey == null) {
        	indexKey = new RecordKey(keys, metadata);
        }
    	try{
    		indexKey.init();
    	}catch(NullPointerException e) {
    		status.add(new ConfigurationProblem("Key metadata are null.",Severity.WARNING, this, Priority.NORMAL, XML_LOOKUP_KEY ));
    	}catch(RuntimeException e) {
    		status.add(new ConfigurationProblem(e.getMessage(), Severity.ERROR, this, Priority.NORMAL, XML_LOOKUP_KEY));
    	}
    
    	if (fileURL != null){
    		try {
				FileUtils.getReadableChannel(getGraph().getProjectURL(), fileURL);
			} catch (IOException e) {
	    		status.add(new ConfigurationProblem(e.getMessage(), Severity.ERROR, this, Priority.NORMAL, XML_FILE_URL));
			}
    	}
    	
        return status;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public boolean put(DataRecord dataRecord) {
        DataRecord storeRecord = dataRecord.duplicate();
        lookupTable.put(new HashKey(indexKey, storeRecord), storeRecord);

        return true;
    }

    @Override
    public boolean remove(DataRecord dataRecord) {
        return (lookupTable.remove(new HashKey(indexKey, dataRecord)) != null);
    }
    
    /* (non-Javadoc)
     * @see java.lang.Iterable#iterator()
     */
    public Iterator<DataRecord> iterator() {
    	return lookupTable.values().iterator();
    }

	public String getFileURL() {
		return fileURL;
	}

	public void setFileURL(String fileURL) {
		this.fileURL = fileURL;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public boolean isByteMode() {
		return byteMode;
	}

	public void setByteMode(boolean byteMode) {
		this.byteMode = byteMode;
	}

	public String getData() {
		return data;
	}
	
	public void setData(String data) {
		this.data = data;
	}

}

