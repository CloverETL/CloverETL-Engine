package org.jetel.lookup;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Queue;

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
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
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
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Persistent lookup table</h3>
 *
 * File based lookup table.
 * 
 * <table border="1">
 *  <th>Lookup table:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Persistent lookup table</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>File based lookup table. Complete lookup table stored in external file. It uses jdbm.
 * </td></tr>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>id</b></td><td>component identification</td></tr>
 *  <tr><td><b>type</b></td><td>component type</td></tr>
 *  <tr><td><b>metadata</b></td><td>metadata associated with the DataRecord stored in this lookup table.</td></tr>
 *  <tr><td><b>fileURL</b></td><td>data file when lookup table is stored.</td></tr>
 *  <tr><td><b>key</b></td><td>names of fields which comprise indexKey to lookup table. Specifies what object will be used for looking up data.</td></tr>
 *  <tr><td><b>replace</b></td><td>option to replace existing entry (persistent lookup cannot store duplicate entries)</td></tr>
 *  <tr><td><b>commitInterval</b></td><td>commit interval in number of records (too high interval can invoke java.nio.BufferUnderflowException)</td></tr>
 *  <tr><td><b>disableTransactions</b></td><td>option to disable transaction (to increase performance at the cost of potential data loss).</td></tr>
 *  <tr><td><b>pageSize</b></td><td>number of entries per node</td></tr>
 *  <tr><td><b>cacheSize</b></td><td>given maximum number of objects in cache</td></tr>
 *  </table>
 *
 *	<h4>Example:</h4>
 *  Reading data from input port (dbload):
 *  <pre>&lt;LookupTable fileURL="c:/Temp/data" id="LookupTable1" key="EmployeeID" 
 *  metadata="Metadata0" name="name" type="persistentLookup"/&gt;</pre>
 *
 * @author      Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
 *(c) Javlin Consulting (www.javlinconsulting.cz)
 * @since 		20.10.2008
 */
public class PersistentLookupTable extends GraphElement implements LookupTable {

    /**
     * An implementation of the lookup proxy class for the persistent lookup table.
     *
     * @author Martin Janik <martin.janik@javlin.cz>
     *
     * @version 3rd November 2008
     * @since 3rd November 2008
     */
    private final class PersistentLookup implements Lookup {

        /** the record key used for lookup */
        private final RecordKey lookupKey;

        /** the data record used for lookup */
        private DataRecord dataRecord = null;

        /** the queue of matching data records returned by the last lookup */
        private Queue<DataRecord> matchingDataRecords = null;
        /** the number of data records returned by the last lookup */
        private int matchingDataRecordsCount = -1;

        /**
         * Creates an instance of the <code>PersistentLookup</code> class for the given lookup key.
         *
         * @param lookupKey the lookup key that will be used for lookup
         */
        public PersistentLookup(RecordKey lookupKey, DataRecord inRecord) {
            this.lookupKey = lookupKey;
            this.dataRecord = inRecord;
        }

        public RecordKey getKey() {
            return lookupKey;
        }

        public LookupTable getLookupTable() {
            return PersistentLookupTable.this;
        }

        public synchronized void seek() {
            if (dataRecord == null) {
                throw new IllegalStateException("Data record not set, use the seek(DataRecord) method!");
            }

            matchingDataRecords = new LinkedList<DataRecord>();

            DataRecord matchingDataRecord = get(dataRecord);

            while (matchingDataRecord != null) {
                matchingDataRecords.add(matchingDataRecord);
                matchingDataRecord = getNext();
            }

            matchingDataRecordsCount = matchingDataRecords.size();
        }

        public synchronized void seek(DataRecord dataRecord) {
            if (dataRecord == null) {
                throw new NullPointerException("dataRecord");
            }

            this.dataRecord = dataRecord;

            seek();
        }

        public int getNumFound() {
            if (matchingDataRecords == null) {
                throw new IllegalStateException("The seek() method has NOT been called!");
            }

            return matchingDataRecordsCount;
        }

        public boolean hasNext() {
            if (matchingDataRecords == null) {
                throw new IllegalStateException("The seek() method has NOT been called!");
            }

            return !matchingDataRecords.isEmpty();
        }

        public DataRecord next() {
            if (matchingDataRecords == null) {
                throw new IllegalStateException("The seek() method has NOT been called!");
            }

            return matchingDataRecords.remove();
        }

        public void remove() {
            throw new UnsupportedOperationException("Method not supported!");
        }

    }

    private static final String XML_LOOKUP_TYPE_PERSISTENCE_LOOKUP = "persistentLookup";
	private static final String XML_FILE_URL_ATTRIBUTE = "fileURL";
	private static final String XML_LOOKUP_KEY_ATTRIBUTE = "key";
	private static final String XML_REPLACE_ATTRIBUTE = "replace";
	private static final String XML_COMMIT_INTERVAL_ATTRIBUTE = "commitInterval";
	private static final String XML_DISABLE_TRANSACTIONS_ATTRIBUTE = "disableTransactions";
	private static final String XML_PAGE_SIZE_ATTRIBUTE = "pageSize";
	private static final String XML_CACHE_SIZE_ATTRIBUTE = "cacheSize";
	
    private final static String[] REQUESTED_ATTRIBUTE = {XML_ID_ATTRIBUTE, XML_TYPE_ATTRIBUTE, 
    	XML_METADATA_ID, XML_LOOKUP_KEY_ATTRIBUTE, XML_FILE_URL_ATTRIBUTE
    };

    private static final String BTREE_NAME = "btree";
    
    // used in GUI - com.cloveretl.gui.wizard.lookupTable PersistentLookupTablePage.java
    public static final boolean DEFAULT_REPLACE = true;
	public static final int DEFAULT_COMMIT_INTERVAL = 100;
	public static final boolean DEFAULT_DISABLE_TRANSACTIONS = false;
	public static final int DEFAULT_PAGE_SIZE = BTree.DEFAULT_SIZE;
	public static final int DEFAULT_CACHE_SIZE = 1000;
	
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
	
    public DataRecord get(HashKey lookupKey) {
        if (lookupKey == null) {
            throw new NullPointerException("lookupKey");
        }

        return get(lookupKey.getDataRecord());
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
	
	public DataRecordMetadata getMetadata() {
		return metadata;
	}

	public DataRecord getNext() {
		return null; // only (one recordKey) - (one record) is allowed
	}

	public int getNumFound() {
		return numFound;
	}

	public boolean isReadOnly() {
	    return false;
	}

    public boolean put(DataRecord dataRecord) {
    	DataRecord storeRecord = dataRecord.duplicate();

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

	public boolean remove(DataRecord dataRecord) {
		try {
			tree.remove(dataRecord);
		} catch (IOException ioe) {
			logger.error("Remove failed.", ioe);

			return false;
		}

		return true;
	}

    public boolean remove(HashKey hashKey) {
        return remove(hashKey.getDataRecord());
    }

	public void setLookupKey(Object key) {
		if (key instanceof String) {
			// little extra code to support looking up by String key
            // will be used only if key is composed of 1 field of type STRING
	        DataRecordMetadata keyMetadata = indexKey.generateKeyRecordMetadata();
            if (indexKey.getKeyFields().length == 1 && 
            		keyMetadata.getField(0).getType() == DataFieldMetadata.STRING_FIELD) {
                int[] keyFields = {0};
                RecordKey lookupKey = new RecordKey(keyFields, keyMetadata);
                recordKeyComparator.setLookupKey(lookupKey);
            } else {
                throw new RuntimeException(
                        "Can't use \""
                                + key
                                + "\" (String) for lookup - not compatible with the key defined for this lookup table !");
            }
		} else if (key instanceof Object[]) {
			Object[] keys = (Object[])key;
	        if (indexKey.getKeyFields().length == keys.length) {
	            DataRecordMetadata keyMetadata = indexKey.generateKeyRecordMetadata();
                int[] keyFields = new int[keyMetadata.getNumFields()];
                for (int i = 0; i < keyFields.length; keyFields[i] = i, i++);
                RecordKey lookupKey = new RecordKey(keyFields, keyMetadata);
                recordKeyComparator.setLookupKey(lookupKey);
	        } else {
	            throw new RuntimeException("Supplied lookup values are not compatible with the key defined for this lookup table !");
	        }
		} else if (key instanceof RecordKey) {
			recordKeyComparator.setLookupKey((RecordKey)key);
		} else {
			throw new RuntimeException("Incompatible Object type specified " +
					"as lookup key: " +key.getClass().getName());
		}
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
	        	tree = BTree.createInstance(recordManager, recordKeyComparator, 
	        			keySerializer, recordSerializer, pageSize);
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
			if (recordManager != null) {
				recordManager.commit();
				recordManager.close();
				recordManager = null;
			}
		} catch (IOException ioe) {
			logger.error("Free failed", ioe);
		}
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
		numFound = 0;
		recordKeyComparator.reset();
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
	
    public Lookup createLookup(RecordKey lookupKey, DataRecord inRecord) {
        if (!isInitialized()) {
            throw new NotInitializedException("The lookup table has NOT been initialized!");
        }

        if (lookupKey == null) {
            throw new NullPointerException("key");
        }

        return new PersistentLookup(lookupKey, inRecord);
    }

    public Lookup createLookup(RecordKey lookupKey) {
        return createLookup(lookupKey, null);
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
		private transient RecordKey lookupKey;
		
		public RecordKeyComparator(RecordKey recordKey) {
			this.recordKey = recordKey;
			this.lookupKey = recordKey;
		}
		
		public void setLookupKey(RecordKey lookupKey) {
			this.lookupKey = lookupKey;
		}
		
		public void reset() {
			lookupKey = recordKey;
		}
		
		public int compare(DataRecord key1, DataRecord key2) {
			if ( key1 == null ) {
	            throw new IllegalArgumentException( "Argument 'key1' is null" );
	        }

	        if ( key2 == null ) {
	            throw new IllegalArgumentException( "Argument 'key2' is null" );
	        }
			return recordKeyComparator.doCompare(key1, key2);
		}
		
		public int doCompare(DataRecord key1, DataRecord key2) {
			if (lookupKey.getMetadata().equals(key2.getMetadata())) {
				return recordKey.compare(lookupKey, key1, key2);
			}
			return recordKey.compare(lookupKey, key2, key1);
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
