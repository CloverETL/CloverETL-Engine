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
package org.jetel.lookup;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.DataRecordMap;
import org.jetel.data.DataRecordMap.DataRecordIterator;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.Lookup;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.parser.Parser;
import org.jetel.data.parser.TextParserFactory;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.exception.GraphConfigurationException;
import org.jetel.exception.NotInitializedException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
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
public class SimpleLookupTable extends GraphElement implements LookupTable {

	
	private static final String XML_LOOKUP_TYPE_SIMPLE_LOOKUP = "simpleLookup";
	private static final String XML_LOOKUP_INITIAL_SIZE = "initialSize";
	private static final String XML_LOOKUP_KEY = "key";
	private static final String XML_FILE_URL = "fileURL";
	private static final String XML_CHARSET = "charset";
	private static final String XML_DATA_ATTRIBUTE = "data";
	private static final String XML_KEY_DUPLICATES_ATTRIBUTE = "keyDuplicates";

	private final static String[] REQUESTED_ATTRIBUTE = { XML_ID_ATTRIBUTE, XML_TYPE_ATTRIBUTE, XML_METADATA_ID, XML_LOOKUP_KEY };

	protected String metadataName;
	protected DataRecordMetadata metadata;
	protected String fileURL;
	protected String charset;
	protected Parser dataParser;
	protected DataRecordMap lookupTable;
	protected String[] keys;
	protected RecordKey indexKey;
	protected int tableInitialSize = DEFAULT_INITIAL_CAPACITY;
	protected boolean keyDuplicates = false;

	// data of the lookup table, can be used instead of an input file
	protected String data;

	/**
	 * Default capacity of HashMap when standard constructor is used.
	 */
	protected final static int DEFAULT_INITIAL_CAPACITY = Defaults.Lookup.LOOKUP_INITIAL_CAPACITY;

	/**
	 *Constructor for the SimpleLookupTable object.<br>
	 * It uses hash map to store indexKey->data pairs in it.
	 * 
	 * @param parser
	 *            Reference to parser which should be used for parsing input data
	 * @param metadata
	 *            Metadata describing input data
	 * @param keys
	 *            Names of fields which comprise indexKey to lookup table
	 * @since May 2, 2002
	 */
	public SimpleLookupTable(String id, DataRecordMetadata metadata, String[] keys, Parser parser) {
		this(id, metadata, keys, parser, null);
	}

	public SimpleLookupTable(String id, DataRecordMetadata metadata, String[] keys, Parser parser,
			int initialSize) {
		this(id, metadata, keys, parser, null);
		this.tableInitialSize = initialSize;
	}

	public SimpleLookupTable(String id, String metadataName, String[] keys, int initialSize) {
		super(id);
		this.metadataName = metadataName;
		this.keys = keys;
		this.tableInitialSize = initialSize;
	}

	/**
	 *Constructor for the SimpleLookupTable object.
	 * 
	 * @param parser
	 *            Reference to not-initialized parser which should be used for parsing input data
	 * @param metadata
	 *            Metadata describing input data
	 * @param keys
	 *            Names of fields which comprise indexKey to lookup table
	 * @param mapObject
	 *            Object implementing Map interface. It will be used to hold indexKey->data pairs
	 * @since May 2, 2002
	 */
	public SimpleLookupTable(String id, DataRecordMetadata metadata, String[] keys, Parser parser,
			DataRecordMap mapObject) {
		super(id);
		this.dataParser = parser;
		this.metadata = metadata;
		lookupTable = mapObject;
		indexKey = new RecordKey(keys, metadata);
		indexKey.init();
	}

	@Override
	public Lookup createLookup(RecordKey key, DataRecord keyRecord) {
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}

		SimpleLookup lookup = new SimpleLookup(lookupTable, key, keyRecord, keyDuplicates);
		lookup.setLookupTable(this);

		return lookup;
	}

	@Override
	public Lookup createLookup(RecordKey key) {
		return createLookup(key, null);
	}

	/**
	 * Initializtaion of lookup table - loading all data into it.
	 * 
	 * @exception IOException
	 *                Description of Exception
	 * @since May 2, 2002
	 */
	@Override
	public synchronized void init() throws ComponentNotReadyException {
		if (isInitialized()) {
			// throw new IllegalStateException("The lookup table has already been initialized!");
			return;
		}

		super.init();

		if (metadata == null) {
			metadata = getGraph().getDataRecordMetadata(metadataName, true);
		}
		if (metadata == null) {
			throw new ComponentNotReadyException("Metadata " + StringUtils.quote(metadataName) + " does not exist!!!");
		}

		if (indexKey == null) {
			indexKey = new RecordKey(keys, metadata);
		}
		indexKey.init();
		indexKey.setEqualNULLs(true);

		DataRecord record = DataRecordFactory.newRecord(metadata);
		record.init();

		if (lookupTable == null) {
			lookupTable = new DataRecordMap(indexKey, keyDuplicates, tableInitialSize);
		}

		if (charset == null) {
			charset = Defaults.DataParser.DEFAULT_CHARSET_DECODER;
		}
		if (dataParser == null && (fileURL != null || data != null)) {
			dataParser = TextParserFactory.getParser(metadata,charset);
		}
		if (dataParser != null) {
			dataParser.init();
		}
	}

	
	
	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		if (lookupTable != null) {
			lookupTable.clear();
		}
	}

	@Override
	public synchronized void preExecute() throws ComponentNotReadyException {
		super.preExecute();

		if (firstRun()) {// a phase-dependent part of initialization
			// all necessary elements have been initialized in init()
		} else {
			if (dataParser != null) {
				dataParser.reset();				
			}
		}
		
		if (dataParser != null) {
			/*
			 * populate the lookupTable (Map) with data if provided dataParser is not null, otherwise it is assumed that
			 * the lookup table will be populated later by calling put() method
			 */
			DataRecord record = DataRecordFactory.newRecord(metadata);
			record.init();
			try {
				if (fileURL != null) {
					dataParser.setDataSource(FileUtils.getReadableChannel((getGraph() != null) ? getGraph().getRuntimeContext().getContextURL() : null, fileURL));
				} else if (data != null) {
					dataParser.setDataSource(new ByteArrayInputStream(data.getBytes(charset)));
				}
				if (metadata.getSkipSourceRows() > 0) {
					dataParser.skip(metadata.getSkipSourceRows());
				}
				while (dataParser.getNext(record) != null) {
					lookupTable.put(record.duplicate());
				}
			} catch (Exception e) {
				throw new ComponentNotReadyException(this, e);
			} finally {
				try {
					dataParser.close();
				} catch (IOException e) {
					throw new ComponentNotReadyException(this, "Data parser cannot be closed.", e);
				}
			}
		}
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();		
	}

	public static SimpleLookupTable fromProperties(TypedProperties properties)
			throws AttributeNotFoundException, GraphConfigurationException {

		for (String property : REQUESTED_ATTRIBUTE) {
			if (!properties.containsKey(property)) {
				throw new AttributeNotFoundException(property);
			}
		}
		String type = properties.getStringProperty(XML_TYPE_ATTRIBUTE);
		if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_SIMPLE_LOOKUP)) {
			throw new GraphConfigurationException("Can't create simple lookup table from type " + type);
		}
		int initialSize = properties.getIntProperty(XML_LOOKUP_INITIAL_SIZE, Defaults.Lookup.LOOKUP_INITIAL_CAPACITY);
		String[] keys = properties.getStringProperty(XML_LOOKUP_KEY).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		String metadata = properties.getStringProperty(XML_METADATA_ID);

		SimpleLookupTable lookupTable = new SimpleLookupTable(properties.getStringProperty(XML_ID_ATTRIBUTE), metadata, keys, initialSize);

		if (properties.containsKey(XML_NAME_ATTRIBUTE)) {
			lookupTable.setName(properties.getStringProperty(XML_NAME_ATTRIBUTE));
		}
		if (properties.containsKey(XML_FILE_URL)) {
			lookupTable.setFileURL(properties.getStringProperty(XML_FILE_URL));
		}
		if (properties.containsKey(XML_CHARSET)) {
			lookupTable.setCharset(properties.getStringProperty(XML_CHARSET));
		}
		if (properties.containsKey(XML_KEY_DUPLICATES_ATTRIBUTE)) {
			lookupTable.setKeyDuplicates(properties.getBooleanProperty(XML_KEY_DUPLICATES_ATTRIBUTE));
		}
		if (properties.containsKey(XML_DATA_ATTRIBUTE)) {
			lookupTable.setData(properties.getStringProperty(XML_DATA_ATTRIBUTE));
		}

		return lookupTable;
	}

	public static SimpleLookupTable fromXML(TransformationGraph graph, Element nodeXML)
			throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
		SimpleLookupTable lookupTable = null;
		String id;
		String type;

		// reading obligatory attributes
		id = xattribs.getString(XML_ID_ATTRIBUTE);
		type = xattribs.getString(XML_TYPE_ATTRIBUTE);

		// check type
		if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_SIMPLE_LOOKUP)) {
			throw new XMLConfigurationException("Can't create simple lookup table from type " + type);
		}

		// create simple lookup table
		int initialSize = xattribs.getInteger(XML_LOOKUP_INITIAL_SIZE, Defaults.Lookup.LOOKUP_INITIAL_CAPACITY);
		String[] keys = xattribs.getString(XML_LOOKUP_KEY).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
		String metadata = xattribs.getString(XML_METADATA_ID);

		lookupTable = new SimpleLookupTable(id, metadata, keys, initialSize);
		lookupTable.setGraph(graph);

		if (xattribs.exists(XML_NAME_ATTRIBUTE)) {
			lookupTable.setName(xattribs.getString(XML_NAME_ATTRIBUTE));
		}
		if (xattribs.exists(XML_FILE_URL)) {
			lookupTable.setFileURL(xattribs.getString(XML_FILE_URL));
		}
		if (xattribs.exists(XML_CHARSET)) {
			lookupTable.setCharset(xattribs.getString(XML_CHARSET));
		}
		if (xattribs.exists(XML_KEY_DUPLICATES_ATTRIBUTE)) {
			lookupTable.setKeyDuplicates(xattribs.getBoolean(XML_KEY_DUPLICATES_ATTRIBUTE));
		}
		if (xattribs.exists(XML_DATA_ATTRIBUTE)) {
			lookupTable.setData(xattribs.getString(XML_DATA_ATTRIBUTE));
		}

		return lookupTable;
	}

	@Override
	public synchronized void free() {
		if (isInitialized()) {
			super.free();
			if (lookupTable != null) {
				lookupTable.clear();
				lookupTable = null;
			}
		}
	}

	@Override
	public DataRecordMetadata getMetadata() {
		return metadata;
	}

	public DataRecord getNext() {
		return null; // only one indexKey - one record is allowed
	}

	public int getSize() {
		return lookupTable.size();
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (metadata == null) {
			metadata = getGraph().getDataRecordMetadata(metadataName, false);
		}

		if (indexKey == null) {
			indexKey = new RecordKey(keys, metadata);
		}

		try {
			indexKey.init();
		} catch (NullPointerException e) {
			status.add(new ConfigurationProblem("Key metadata are null.", Severity.WARNING, this, Priority.NORMAL, XML_LOOKUP_KEY));
			indexKey = null; // we have to create it once again in init method after creating metadata from stub
		} catch (RuntimeException e) {
			status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, this, Priority.NORMAL, XML_LOOKUP_KEY));
		}

		if (fileURL != null) {
			try {
				FileUtils.getReadableChannel(getGraph().getRuntimeContext().getContextURL(), fileURL);
			} catch (IOException e) {
				status.add(new ConfigurationProblem(ExceptionUtils.getMessage(e), Severity.ERROR, this, Priority.NORMAL, XML_FILE_URL));
			}
		}

		if (data != null && metadata.containsCarriageReturnInDelimiters()) {
			status.add(new ConfigurationProblem("Cannot use carriage return as a delimiter when inline data is specified!", Severity.ERROR, this, Priority.NORMAL, XML_DATA_ATTRIBUTE));
		}

		return status;
	}

	@Override
	public boolean isPutSupported() {
		return true;
	}

	@Override
	public boolean isRemoveSupported() {
		return true;
	}

	@Override
	public boolean put(DataRecord dataRecord) {
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}
		lookupTable.put(dataRecord.duplicate());

		return true;
	}

	@Override
	public boolean remove(DataRecord dataRecord) {
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}

		return lookupTable.remove(dataRecord);
	}

	@Override
	public boolean remove(HashKey key) {
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}
		return lookupTable.remove(key.getRecordKey(), key.getDataRecord());
	}

	@Override
	public Iterator<DataRecord> iterator() {
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}
		return lookupTable.valueIterator();
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

	@Override
	public DataRecordMetadata getKeyMetadata() throws ComponentNotReadyException {
		if (!isInitialized()) {
			throw new NotInitializedException(this);
		}

		return indexKey.generateKeyRecordMetadata();
	}

	@Override
	public void setCurrentPhase(int phase) {
		//isn't required by the lookup table
	}

}

class SimpleLookup implements Lookup {

	protected DataRecord curentResult;
	private RecordKey key;
	private int numFound = 0;
	private DataRecordMap.DataRecordLookup tableLookup;
	protected SimpleLookupTable lookupTable;

	private boolean duplicate;
	private DataRecordIterator iterator;

	SimpleLookup(DataRecordMap data, RecordKey key, DataRecord record, boolean duplicate) {
		this.tableLookup = data.createDataRecordLookup(key, record);
		this.key = key;
		this.duplicate = duplicate;
	}

	@Override
	public RecordKey getKey() {
		return key;
	}

	@Override
	public LookupTable getLookupTable() {
		return lookupTable;
	}

	void setLookupTable(SimpleLookupTable lookupTable) {
		this.lookupTable = lookupTable;
	}

	@Override
	public int getNumFound() {
		if (duplicate) {
			return iterator != null ? iterator.size() : 0;
		} else {
			return numFound;
		}
	}

	@Override
	public void seek() {
		if (duplicate) {
			iterator = tableLookup.getAll();
		} else {
			curentResult = tableLookup.get();
			numFound = curentResult != null ? 1 : 0;
		}
	}

	@Override
	public void seek(DataRecord keyRecord) {
		tableLookup.setDataRecord(keyRecord);
		seek();
	}

	@Override
	public boolean hasNext() {
		if (duplicate) {
			return iterator == null ? false : iterator.hasNext();
		} else {
			return curentResult != null;
		}
	}

	@Override
	public DataRecord next() {
		if (duplicate) {
			return iterator.next();
		} else {
			if (curentResult == null)
				throw new NoSuchElementException();
			DataRecord ret = curentResult;
			curentResult = null;
			return ret;
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Method not supported!");
	}
}