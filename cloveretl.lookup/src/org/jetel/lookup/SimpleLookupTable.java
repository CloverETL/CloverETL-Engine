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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.parser.DataParser;
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
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.DuplicateKeyMap;
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
public class SimpleLookupTable extends GraphElement implements LookupTable {

    private static final String XML_LOOKUP_TYPE_SIMPLE_LOOKUP = "simpleLookup";
    private static final String XML_LOOKUP_INITIAL_SIZE = "initialSize";
    private static final String XML_LOOKUP_KEY = "key";
    private static final String XML_FILE_URL = "fileURL";
    private static final String XML_CHARSET = "charset";
	private static final String XML_DATA_ATTRIBUTE = "data";
	private static final String XML_KEY_DUPLICATES_ATTRIBUTE = "keyDuplicates";
	
    private final static String[] REQUESTED_ATTRIBUTE = {XML_ID_ATTRIBUTE, XML_TYPE_ATTRIBUTE, XML_METADATA_ID,
    	XML_LOOKUP_KEY
    };

    protected String metadataName;
	protected DataRecordMetadata metadata;
	protected String fileURL;
	protected String charset;
	protected Parser dataParser;
	protected Map lookupTable;
	protected String[] keys;
	protected RecordKey indexKey;
	protected int tableInitialSize=DEFAULT_INITIAL_CAPACITY;
	protected boolean keyDuplicates = false;
	
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

	public Lookup createLookup(RecordKey key, DataRecord keyRecord) {
		SimpleLookup lookup = new SimpleLookup(lookupTable, key, keyRecord);
		lookup.setLookupTable(this);
		return lookup;
	}
	
	public Lookup createLookup(RecordKey key) {
		return createLookup(key, null);
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
	    	Map<HashKey, DataRecord> store = new HashMap<HashKey, DataRecord>(tableInitialSize);
	        lookupTable = keyDuplicates ? new DuplicateKeyMap(store) : store;
	    }
	    
	    if (charset == null) {
	    	charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
	    }
	    if (dataParser == null && (fileURL != null || data!= null)) {
    		dataParser = new DataParser(charset);
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
        if (properties.containsKey(XML_KEY_DUPLICATES_ATTRIBUTE)){
        	lookupTable.setKeyDuplicates(properties.getBooleanProperty(XML_KEY_DUPLICATES_ATTRIBUTE));
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
            if (xattribs.exists(XML_KEY_DUPLICATES_ATTRIBUTE)){
            	lookupTable.setKeyDuplicates(xattribs.getBoolean(XML_KEY_DUPLICATES_ATTRIBUTE));
            }
            if (xattribs.exists(XML_DATA_ATTRIBUTE)) {
            	lookupTable.setData(xattribs.getString(XML_DATA_ATTRIBUTE));
            }
            
            return lookupTable;
            
         }catch(Exception ex){
             throw new XMLConfigurationException("can't create simple lookup table",ex);
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
	
	public int getSize(){
	    return lookupTable instanceof DuplicateKeyMap ? ((DuplicateKeyMap)lookupTable).totalSize() : lookupTable.size();
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

    public boolean isReadOnly() {
        return false;
    }

    public boolean put(DataRecord dataRecord) {
        DataRecord storeRecord = dataRecord.duplicate();
        lookupTable.put(new HashKey(indexKey, storeRecord), storeRecord);

        return true;
    }

    public boolean remove(DataRecord dataRecord) {
    	if (!(lookupTable instanceof DuplicateKeyMap)) {
    		return (lookupTable.remove(new HashKey(indexKey, dataRecord)) != null);
    	}
    	return ((DuplicateKeyMap)lookupTable).remove(new HashKey(indexKey, dataRecord), dataRecord);
    }
    
    public boolean remove(HashKey key) {
        return (lookupTable.remove(key) != null);
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

	public String getData() {
		return data;
	}
	
	public void setData(String data) {
		this.data = data;
	}

	public boolean isKeyDuplicates() {
		return keyDuplicates;
	}

	public void setKeyDuplicates(boolean keyDuplicates) {
		this.keyDuplicates = keyDuplicates;
	}

	public char[] getKey() throws ComponentNotReadyException, UnsupportedOperationException, NotInitializedException {
		if (!isInitialized()) throw new NotInitializedException(this);
		
		char[] result = new char[indexKey.getLength()];
		int[] keyField = indexKey.getKeyFields();
		for (int i = 0; i < result.length; i++) {
			result[i] = metadata.getFieldType(keyField[i]);
		}
		return result;
	}

}

class SimpleLookup implements Lookup{

	protected Map data;
	protected List<DataRecord> curentResult;
	protected HashKey key;
	private int no;
	private int numFound = 0;
	protected SimpleLookupTable lookupTable;
	
	SimpleLookup(Map data, RecordKey key, DataRecord record) {
		this.data = data;
		if (!(data instanceof DuplicateKeyMap)) {
			curentResult = new ArrayList<DataRecord>(1);
		}
		this.key = new HashKey(key, record);
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.Lookup#getKey()
	 */
	public RecordKey getKey() {
		return key.getRecordKey();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.Lookup#getLookupTable()
	 */
	public LookupTable getLookupTable() {
		return lookupTable;
	}
	
	void setLookupTable(SimpleLookupTable lookupTable){
		this.lookupTable = lookupTable;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.Lookup#getNumFound()
	 */
	public int getNumFound() {
		return numFound;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.Lookup#seek()
	 */
	public void seek() {
		if (key.getDataRecord() == null) throw new IllegalStateException("No key data for performing lookup");
		if (data instanceof DuplicateKeyMap) {
			curentResult = ((DuplicateKeyMap)data).getAll(key);
			numFound = curentResult != null ? curentResult.size() : 0;
		}else{
			curentResult.clear();
			if (data.containsKey(key)) {
				curentResult.add((DataRecord) data.get(key));
			}
			numFound = curentResult.size();
		}
		no = 0;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.Lookup#seek(org.jetel.data.DataRecord)
	 */
	public void seek(DataRecord keyRecord) {
		key.setDataRecord(keyRecord);
		seek();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	public boolean hasNext() {
		return curentResult != null && no < numFound;
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	public DataRecord next() {
		if (curentResult == null || no >= numFound) throw new NoSuchElementException();
		return curentResult.get(no++);
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	public void remove() {
		if (curentResult == null || no >= numFound) throw new IllegalStateException();
		if (data instanceof DuplicateKeyMap){
			curentResult.remove(--no);
		}else{
			data.remove(key);
		}
	}

}
