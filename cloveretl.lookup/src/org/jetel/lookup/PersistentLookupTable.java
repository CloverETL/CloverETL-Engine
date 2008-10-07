package org.jetel.lookup;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.RecordManagerOptions;
import jdbm.btree.BTree;
import jdbm.helper.Serializer;
import jdbm.helper.Tuple;
import jdbm.helper.TupleBrowser;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.BasicLookupTableIterator;
import org.jetel.data.lookup.LookupTable;
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
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

public class PersistentLookupTable extends GraphElement implements LookupTable {
	private static final String XML_LOOKUP_TYPE_PERSISTENCE_LOOKUP = "persistentLookup";
	private static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
	private static final String XML_LOOKUP_KEY_ATTRIBUTE = "key";
	private static final String XML_REPLACE_ATTRIBUTE = "replace";
	private static final String XML_COMMIT_INTERVAL_ATTRIBUTE = "commitInterval";
	private static final String XML_DISABLE_TRANSACTIONS_ATTRIBUTE = "disableTransactions"; // Option to disable transaction (to increase performance at the cost of potential data loss).
	private static final String XML_PAGE_SIZE_ATTRIBUTE = "pageSize"; // Default page size (number of entries per node)
	private static final String XML_CACHE_SIZE_ATTRIBUTE = "cacheSize"; // Cache size - given maximum number of objects
	
    private final static String[] REQUESTED_ATTRIBUTE = {XML_ID_ATTRIBUTE, XML_TYPE_ATTRIBUTE, XML_METADATA_ID,
    	XML_LOOKUP_KEY_ATTRIBUTE, XML_FILE_URL_ATTRIBUTE
    };

    private static final String BTREE_NAME = "btree";
	private static final boolean DEFAULT_REPLACE = true;
	private static final int DEFAULT_COMMIT_INTERVAL = 1000;
	private static final boolean DEFAULT_DISABLE_TRANSACTIONS = false;
	private static final int DEFAULT_PAGE_SIZE = BTree.DEFAULT_SIZE;
	private static final int DEFAULT_CACHE_SIZE = 1000;
	
	private static Log logger = LogFactory.getLog(PersistentLookupTable.class);
	
	// variable for xml attributes
	private String metadataName;
	private DataRecordMetadata metadata = null;
	protected String[] keys;
	private String fileURL = null;
	private boolean replace = DEFAULT_REPLACE;
	private int commitInterval = DEFAULT_COMMIT_INTERVAL;
	private boolean disableTransactions = DEFAULT_DISABLE_TRANSACTIONS;
	private int pageSize = DEFAULT_PAGE_SIZE;
	private int cacheSize = DEFAULT_CACHE_SIZE;
	
	private RecordManager recordManager;
	private BTree tree;
	
	private static KeySerializer keySerializer;
	private static RecordSerializer recordSerializer;
	private static RecordKeyComparator recordKeyComparator;
	
	protected RecordKey indexKey;
	private int uncommitedRecords = 0;
	private int numFound;
	private DataRecord keyRecord;

	public PersistentLookupTable(String id, DataRecordMetadata metadata, String[] keys, String fileURL) {
		super(id);
		this.metadata = metadata;
		this.keys = keys;
		this.fileURL = fileURL;
	}
	
	public PersistentLookupTable(String id, String metadataName, String[] keys, String fileURL) {
		super(id);
		this.metadataName = metadataName;
		this.keys = keys;
		this.fileURL = fileURL;
	}
	
	public DataRecord get(String keyString) {
		prepareGet();
		
		keyRecord.reset();
        keyRecord.getField(0).fromString(keyString);
		
		return find(keyRecord);
	}

	public DataRecord get(Object[] keys) {
		prepareGet();
		
		if (metadata.getNumFields() < keys.length) {
			logger.error("Too long keys.");
			return null;
		}
		
		keyRecord.reset();
		
		for (int i = 0; i < keys.length; i++) {
	        keyRecord.getField(i).setValue(keys[i]);
	    }
		
		return find(keyRecord);
	}

	public DataRecord get(DataRecord keyRecord) {
		prepareGet();
		
		return find(keyRecord);
	}
	
	private void prepareGet() {
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}
		
		if (uncommitedRecords != 0) {
			try {
				recordManager.commit();
			} catch (IOException ioe) {
				logger.error("Commit failed", ioe);
			}
		}
	}
	
	private DataRecord find(DataRecord keyRecord) {
		try {
	    	DataRecord record = (DataRecord)tree.find(keyRecord);
	    	numFound = (record != null ? 1 : 0);
	    	return record;
		} catch (IOException ioe) {
			logger.error("Find failed", ioe);
			return null;
		}
	}
	
    public Iterator<DataRecord> iterator(Object lookupKey) {
        return new BasicLookupTableIterator(this, lookupKey);
    }

	public DataRecordMetadata getMetadata() {
		return metadata;
	}

	public DataRecord getNext() {
		return null; // only (one recordKey) - (one record) is allowed
	}

	public int getNumFound() {
		return numFound;
	}

	/**
     * @param   key not used
     * @param   data Data to store in lookup table by using previously defined key
     * @see org.jetel.data.lookup.LookupTable#put(java.lang.Object, org.jetel.data.DataRecord)
     */
    public boolean put(Object key, DataRecord data) {
    	DataRecord storeRecord = data.duplicate();
        try {
        	tree.insert(storeRecord, storeRecord, replace);
			if (++uncommitedRecords == commitInterval) {
				recordManager.commit();
				uncommitedRecords = 0;
			}
		} catch (IOException ioe) {
			logger.error("Record wasn't put to the lookup table", ioe);
			return false;
		}
        return true;
    }

	public boolean remove(Object key) {
		try {
			tree.remove(key);
		} catch (IOException ioe) {
			logger.error("Remove failed.", ioe);
			return false;
		}
		return true;
	}

	public void setLookupKey(Object key) {
		// unuse
	}

	public Iterator<DataRecord> iterator() {
		try {
			return new PersistentLookupIterator(tree);
		} catch (IOException ioe) {
			throw new RuntimeException("Creating of iterator failed.", ioe);
		}
	}
	
	private void loadMetadata() throws ComponentNotReadyException {
		if (metadata == null) {
			metadata = getGraph().getDataRecordMetadata(metadataName);
		}		
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata " + StringUtils.quote(metadataName) + 
					" does not exist!!!");
		}
	}
	
	@Override
	public synchronized void init() throws ComponentNotReadyException {
		if (isInitialized()) return;
		super.init();
		
		loadMetadata();
		
		indexKey = new RecordKey(keys, metadata);
        indexKey.init();
		
		keySerializer = new KeySerializer(indexKey, metadata);
    	recordSerializer = new RecordSerializer(metadata);
    	recordKeyComparator = new RecordKeyComparator(indexKey);
		
		if (StringUtils.isEmpty(fileURL)) {
			throw new ComponentNotReadyException("Attribute " + StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + 
					" isn't defined.");
		}
		
		openBTree();
		
		numFound = 0;
		
		keyRecord = new DataRecord(metadata);
		keyRecord.init();
	}
	
	private Properties getRecordManagerOptions() {
		Properties props = new Properties();
	    props.put(RecordManagerOptions.DISABLE_TRANSACTIONS, disableTransactions);
	    props.put(RecordManagerOptions.CACHE_SIZE, cacheSize);
	    return props;
	}
	
	private void openBTree() throws ComponentNotReadyException {
		try {
			recordManager = RecordManagerFactory.createRecordManager(fileURL, getRecordManagerOptions());
			
			long recordId = recordManager.getNamedObject(BTREE_NAME);
	        if ( recordId != 0 ) { // reload an existing B+Tree
	            tree = BTree.load(recordManager, recordId);
	        } else { // create a new B+Tree data structure
	        	tree = BTree.createInstance(recordManager, recordKeyComparator, keySerializer, recordSerializer, pageSize);
	            recordManager.setNamedObject(BTREE_NAME, tree.getRecid());
	        }
		} catch (IOException ioe) {
			throw new ComponentNotReadyException(this, 
					"error while creating or opening a record manager", ioe);
		}
	}
	
	@Override
	public synchronized void free() {
		super.free();
		
		try {
			recordManager.commit();
			recordManager.close();
		} catch (IOException ioe) {
			logger.error("Free failed", ioe);
		}
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		numFound = 0;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);
		
		try {
			loadMetadata();
		} catch (ComponentNotReadyException cnre) {
			status.add(new ConfigurationProblem(cnre.getMessage() + 
					" does not exist!!!", Severity.ERROR, this, Priority.NORMAL, XML_METADATA_ID));
		}
		
       	
    	try{
    		indexKey = new RecordKey(keys, metadata);
    		indexKey.init();
    	} catch (NullPointerException e) {
    		status.add(new ConfigurationProblem("Key metadata are null.",Severity.WARNING, this, Priority.NORMAL, XML_LOOKUP_KEY_ATTRIBUTE));
    	} catch (RuntimeException e) {
    		status.add(new ConfigurationProblem(e.getMessage(), Severity.ERROR, this, Priority.NORMAL, XML_LOOKUP_KEY_ATTRIBUTE));
    	}
    	
    	if (StringUtils.isEmpty(fileURL)) {
			status.add(new ConfigurationProblem("Attribute " + StringUtils.quote(XML_FILE_URL_ATTRIBUTE) + 
					" isn't defined.", Severity.ERROR, this, Priority.NORMAL, XML_FILE_URL_ATTRIBUTE));
		}
    	
    	return status;
	}
	
	private void setCommitInterval(int commitInterval) {
		this.commitInterval = commitInterval;
	}
	
	private void setReplace(boolean replace) {
		this.replace = replace;
	}

	private void setDisableTransactions(boolean disableTransactions) {
		this.disableTransactions = disableTransactions;
	}
	
	private void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	
	private void setCacheSize(int cacheSize) {
		this.cacheSize = cacheSize;
	}
	
    public static PersistentLookupTable fromProperties(TypedProperties properties) throws AttributeNotFoundException, GraphConfigurationException{

    	for (String property : REQUESTED_ATTRIBUTE) {
			if (!properties.containsKey(property)) {
				throw new AttributeNotFoundException(property);
			}
		}
    	String type = properties.getProperty(XML_TYPE_ATTRIBUTE);
    	if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_PERSISTENCE_LOOKUP)){
    		throw new GraphConfigurationException("Can't create persistent lookup table from type " + type);
    	}
        String[] keys = properties.getProperty(XML_LOOKUP_KEY_ATTRIBUTE).
		split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);

		PersistentLookupTable lookupTable = new PersistentLookupTable(
				properties.getProperty(XML_ID_ATTRIBUTE), 
				properties.getProperty(XML_METADATA_ID), 
				keys, 
				properties.getProperty(XML_FILE_URL_ATTRIBUTE));
		
		if (properties.containsKey(XML_NAME_ATTRIBUTE)){
			lookupTable.setName(properties.getProperty(XML_NAME_ATTRIBUTE));
		}
		if (properties.containsKey(XML_COMMIT_INTERVAL_ATTRIBUTE)){
			lookupTable.setCommitInterval(properties.getIntProperty(XML_COMMIT_INTERVAL_ATTRIBUTE));
		}
		if (properties.containsKey(XML_REPLACE_ATTRIBUTE)){
			lookupTable.setReplace(properties.getBooleanProperty(XML_REPLACE_ATTRIBUTE));
		}
		if (properties.containsKey(XML_DISABLE_TRANSACTIONS_ATTRIBUTE)){
			lookupTable.setDisableTransactions(properties.getBooleanProperty(XML_DISABLE_TRANSACTIONS_ATTRIBUTE));
		}
		if (properties.containsKey(XML_PAGE_SIZE_ATTRIBUTE)){
			lookupTable.setPageSize(properties.getIntProperty(XML_PAGE_SIZE_ATTRIBUTE));
		}
		if (properties.containsKey(XML_CACHE_SIZE_ATTRIBUTE)){
			lookupTable.setCacheSize(properties.getIntProperty(XML_CACHE_SIZE_ATTRIBUTE));
		}
		
		return lookupTable;
    }

	public static PersistentLookupTable fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
    ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
        String type;
        
        try {
            type = xattribs.getString(XML_TYPE_ATTRIBUTE);
        } catch (AttributeNotFoundException ex) {
            throw new XMLConfigurationException("Can't create lookup table - " + ex.getMessage(), ex);
        }
        
        //check type
        if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_PERSISTENCE_LOOKUP)) {
            throw new XMLConfigurationException("Can't create persistent lookup table from type " + type);
        }
        
        //create simple lookup table
        try {
//            int initialSize = xattribs.getInteger(XML_LOOKUP_INITIAL_SIZE, Defaults.Lookup.LOOKUP_INITIAL_CAPACITY);
            String[] keys = xattribs.getString(XML_LOOKUP_KEY_ATTRIBUTE).
            				split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
            
            PersistentLookupTable lookupTable = new PersistentLookupTable(
            		xattribs.getString(XML_ID_ATTRIBUTE), 
            		xattribs.getString(XML_METADATA_ID), 
            		keys, 
            		xattribs.getString(XML_FILE_URL_ATTRIBUTE));
            
            if (xattribs.exists(XML_NAME_ATTRIBUTE)){
            	lookupTable.setName(xattribs.getString(XML_NAME_ATTRIBUTE));
            }
            if (xattribs.exists(XML_COMMIT_INTERVAL_ATTRIBUTE)){
            	lookupTable.setCommitInterval(xattribs.getInteger(XML_COMMIT_INTERVAL_ATTRIBUTE));
            }
            if (xattribs.exists(XML_REPLACE_ATTRIBUTE)){
            	lookupTable.setReplace(xattribs.getBoolean(XML_REPLACE_ATTRIBUTE));
            }
            if (xattribs.exists(XML_DISABLE_TRANSACTIONS_ATTRIBUTE)){
            	lookupTable.setDisableTransactions(xattribs.getBoolean(XML_DISABLE_TRANSACTIONS_ATTRIBUTE));
            }
            if (xattribs.exists(XML_PAGE_SIZE_ATTRIBUTE)){
            	lookupTable.setPageSize(xattribs.getInteger(XML_PAGE_SIZE_ATTRIBUTE));
            }
            if (xattribs.exists(XML_CACHE_SIZE_ATTRIBUTE)){
            	lookupTable.setCacheSize(xattribs.getInteger(XML_CACHE_SIZE_ATTRIBUTE));
            }
            
            return lookupTable;
         } catch (Exception ex) {
             throw new XMLConfigurationException("Can't create persistent lookup table.", ex);
         }
    }
	
	private static class KeySerializer implements Serializer {
		private static final long serialVersionUID = 6817061133810739560L;
		private transient static final int MAX_RECORD_SIZE = Defaults.Record.MAX_RECORD_SIZE;
		private transient ByteBuffer buffer;
		private transient RecordKey recordKey;
		private transient DataRecord record;
		
		public KeySerializer(RecordKey recordKey, DataRecordMetadata metadata) {
			this.record = new DataRecord(metadata);
			this.record.init();
			this.recordKey = recordKey;
			
			this.buffer = ByteBuffer.allocateDirect(MAX_RECORD_SIZE);
		}

		public byte[] serialize(Object obj) throws IOException {
			return keySerializer.doSerialize(obj);
		}
		
		public byte[] doSerialize(Object obj) throws IOException {
			if (!(obj instanceof DataRecord)) {
				throw new IllegalArgumentException("HashKey type expected");
			}
			DataRecord dataRecord = (DataRecord) obj;
	
			buffer.clear();
			recordKey.serializeKeyFields(buffer, dataRecord);
			return byteBufferToArray(buffer);
		}
		
		public Object deserialize(byte[] serialized) throws IOException {
			return keySerializer.doDeserialize(serialized);
		}
		
		public Object doDeserialize(byte[] serialized) throws IOException {
			record.reset();
			recordKey.deserializeKeyFileds(ByteBuffer.wrap(serialized), record);
			return record.duplicate();
		}
	}
	
	private static class RecordSerializer implements Serializer {
		private static final long serialVersionUID = 6277627192816600633L;
		private transient static final int MAX_RECORD_SIZE = Defaults.Record.MAX_RECORD_SIZE;
		
		private transient DataRecord record;
		private transient ByteBuffer buffer;
		
	    /**
	     * Construct an RecordSerializer.
	     */
	    public RecordSerializer(DataRecordMetadata metadata) {
	    	this.record = new DataRecord(metadata);
	    	this.record.init();
	    	this.buffer = ByteBuffer.allocateDirect(MAX_RECORD_SIZE);
	    }
    
		public byte[] serialize(Object obj) throws IOException {
			return recordSerializer.doSerialize(obj);
		}
		
		public byte[] doSerialize(Object obj) throws IOException {
			if (!(obj instanceof DataRecord)) {
				throw new IllegalArgumentException("DataRecord type expected");
			}
			DataRecord record = (DataRecord) obj;
	
			buffer.clear();
		    record.serialize(buffer);
			return byteBufferToArray(buffer);
		}

		public Object deserialize(byte[] serialized) throws IOException {
			return recordSerializer.doDeserialize(serialized);
		}
		
		public Object doDeserialize(byte[] serialized) throws IOException {
			record.reset();
			record.deserialize(ByteBuffer.wrap(serialized));
			return record.duplicate();
		}
	}

	private static byte[] byteBufferToArray(ByteBuffer buffer) {
		int length = buffer.position();
		buffer.flip();
		byte[] byteArray = new byte[length];
		buffer.get(byteArray, 0, length);
		return byteArray;
	}

	private static class RecordKeyComparator implements Comparator<DataRecord>, Serializable {
		private static final long serialVersionUID = 32605276655163072L;

		private transient RecordKey recordKey;
		
		public RecordKeyComparator(RecordKey recordKey) {
			this.recordKey = recordKey;
		}
		
		public int compare(DataRecord key1, DataRecord key2) {
			return recordKeyComparator.doCompare(key1, key2);
		}
		
		public int doCompare(DataRecord key1, DataRecord key2) {
			return recordKey.compare(key1, key2);
		}
	}
	
	private static class PersistentLookupIterator implements Iterator<DataRecord> {
		private BTree tree;
		private TupleBrowser browser;
		private DataRecord nextRecord;
		private DataRecord lastRecord;
		
		public PersistentLookupIterator(BTree tree) throws IOException {
			this.tree = tree;
			browser = tree.browse();
			nextRecord = null;
			lastRecord = null;
		}
		
		public boolean hasNext() {
			if (nextRecord != null) {
				return true;
			}
			
			Tuple tuple = new Tuple();
			try {
				if (browser.getNext(tuple)) {
					nextRecord = (DataRecord)tuple.getValue();
					return true;
				}
				return false;
			} catch (IOException ioe) {
				logger.error("hasNext() failed", ioe);
				return false;		
			}
		}

		public DataRecord next() {
			if (nextRecord == null) {
				if (!hasNext()) {
					throw new NoSuchElementException();
				}
			}
			lastRecord = nextRecord;
			nextRecord = null;
			return lastRecord;
		}

		public void remove() {
			if (lastRecord == null) {
				throw new IllegalStateException();
			}
			
			try {
				tree.remove(lastRecord);
			} catch (IOException ioe) {
				logger.error("remove() failed", ioe);
			}
			
			lastRecord = null;
		}
		
	}
}
