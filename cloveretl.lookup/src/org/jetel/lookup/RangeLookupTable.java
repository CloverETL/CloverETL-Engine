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
import java.security.InvalidParameterException;
import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordComparator;
import org.jetel.data.RecordKey;
import org.jetel.data.StringDataField;
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
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.MiscUtils;
import org.jetel.util.file.FileUtils;
import org.jetel.util.primitive.TypedProperties;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 * Range lookup table contains records, which defines intervals, eg: Lookup table defined 
 * as follows:<br>
 * low_slow,0,10,0,50<br>
 * low_fast,0,10,50,100<br>
 * high_slow,10,20,0,50<br>
 * high_fast,10,20,50,100<br>
 * with startFields = {1,3} and endFields = {2,4} 
 * has 4 intervals with 2 searching parameters: first from interval 0-20, and second from interval 0-100.<br>
 * Intervals can overlap. By default start point is included and end point is excluded.
 * It is possible to change this settings during construction or by by proper set method (call init() then).<br>
 * It is possible to use "unlimited" intervals by setting <i>null</i> value to <i>start</i>
 * or <i>end</i> field, eg. interval <i>null,0</i> "contains" elements smaller then 0.
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Sep 20, 2007
 *
 */
public class RangeLookupTable extends GraphElement implements LookupTable {
	
	
    private static final String XML_LOOKUP_TYPE_RANGE_LOOKUP = "rangeLookup";
    private static final String XML_FILE_URL = "fileURL";
    private static final String XML_START_FIELDS = "startFields";
    private static final String XML_END_FIELDS = "endFields";
    private static final String XML_CHARSET = "charset";
    private static final String XML_USE_I18N = "useI18N";
    private static final String XML_LOCALE = "locale";
    private static final String XML_START_INCLUDE = "startInclude";
    private static final String XML_END_INCLUDE = "endInclude";
	private static final String XML_DATA_ATTRIBUTE = "data";

    private final static String[] REQUESTED_ATTRIBUTE = {XML_ID_ATTRIBUTE, XML_TYPE_ATTRIBUTE, XML_METADATA_ID,
    	XML_START_FIELDS, XML_END_FIELDS
    };

    public static final boolean DEFAULT_START_INCLUDE = true;
	public static final boolean DEFAULT_END_INCLUDE = false;
	
    protected DataRecordMetadata metadata;//defines lookup table
    protected String metadataId;
	protected Parser dataParser;
	protected SortedSet<DataRecord> lookupTable;//set of intervals
	protected RecordKey startKey;
	protected String[] startFields;
	protected int[] startField;
	protected RecordKey endKey;
	protected String[] endFields;
	protected int[] endField;
	protected IntervalRecordComparator comparator;
	protected RuleBasedCollator[] collators = null;
	protected boolean[] startInclude;
	protected boolean[] endInclude;
	protected boolean useI18N;
	protected String locale;
	protected String charset;
	protected String fileURL;
	// data of the lookup table, can be used instead of an input file
	protected String data;
	
	/**
	 * Constructor for most general range lookup table 
	 * 
	 * @param id id
	 * @param metadata metadata defining this lookup table
	 * @param parser parser for reading defining records
	 * @param collator collator for comparing string fields
	 * @param startInclude indicates whether start points belong to the intervals or not
	 * @param endInclude indicates whether end points belong to the intervals or not
	 */
	public RangeLookupTable(String id, DataRecordMetadata metadata, String[] startFields, 
			String[] endFields, Parser parser, RuleBasedCollator collator, boolean[] startInclude, boolean[] endInclude){
		super(id);
		this.metadata = metadata;
		this.startFields = startFields;
		this.endFields = endFields;
		this.dataParser = parser;
		this.collators = new RuleBasedCollator[metadata.getFields().length];
		Arrays.fill(collators, collator);
		if (startInclude.length != (metadata.getNumFields() - 1)/2) {
			throw new InvalidParameterException("startInclude parameter has wrong number " +
					"of elements: " + startInclude.length + " (should be " + 
					(metadata.getNumFields() - 1)/2 + ")");
		}
		this.startInclude = startInclude;
		if (endInclude.length != (metadata.getNumFields() - 1)/2) {
			throw new InvalidParameterException("endInclude parameter has wrong number " +
					"of elements: " + endInclude.length + " (should be " + 
					(metadata.getNumFields() - 1)/2 + ")");
		}
		this.endInclude = endInclude;
	}

	/**
	 * Constructor with standard settings for start and end points
	 * 
	 * @param id
	 * @param metadata
	 * @param parser
	 * @param collator
	 */
	public RangeLookupTable(String id, DataRecordMetadata metadata, String[] startFields, 
			String[] endFields, Parser parser, RuleBasedCollator collator){
		this(id,metadata,startFields, endFields, parser,collator,new boolean[(metadata.getNumFields() - 1)/2],
				new boolean[(metadata.getNumFields() - 1)/2]);
		Arrays.fill(startInclude, DEFAULT_START_INCLUDE);
		Arrays.fill(endInclude, DEFAULT_END_INCLUDE);
	}

	public RangeLookupTable(String id, DataRecordMetadata metadata, String[] startFields, 
			String[] endFields, Parser parser,	boolean[] startInclude, boolean[] endInclude){
		this(id,metadata,startFields, endFields, parser,null,startInclude,endInclude);
	}
	
	public RangeLookupTable(String id, DataRecordMetadata metadata, String[] startFields, 
			String[] endFields, Parser parser){
		this(id,metadata,startFields, endFields, parser,null);
	}
	
	public RangeLookupTable(String id, String metadataId, String[] startFields, String[] endFields){
		super(id);
		this.metadataId = metadataId;
		this.startFields = startFields;
		this.endFields = endFields;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		if (metadata == null) {
			metadata = getGraph().getDataRecordMetadata(metadataId, false);
		}		

		if (startKey == null) {
			startKey = new RecordKey(startFields, metadata);
		}		
		if (endKey == null) {
			endKey = new RecordKey(endFields, metadata);
		}		
		RecordKey.checkKeys(startKey, XML_START_FIELDS, endKey, XML_END_FIELDS, status, this);
  		startKey = null;//we have to create it once again in init method after creating metadata from stub
  		endKey = null;//we have to create it once again in init method after creating metadata from stub
  	    	  		
		if (startInclude == null) {
			startInclude = new boolean[startFields.length];
			Arrays.fill(startInclude, DEFAULT_START_INCLUDE);
		}
		
		if (endInclude == null) {
			endInclude = new boolean[endFields.length];
			Arrays.fill(endInclude, DEFAULT_END_INCLUDE);
		}
		
		for (int i = 0; i < startFields.length; i++) {
			if (startFields[i].equals(endFields[i]) && !(startInclude[i] && endInclude[i])) {
				status.add(new ConfigurationProblem("Interval "
						+ StringUtils.quote(startFields[i] + " - "
								+ endFields[i]) + " is empty ("
						+ (!startInclude[i] ? "startInclude[" : "endInclude[")
						+ i + "] is false).", Severity.WARNING, this,
						Priority.NORMAL));
			}
		}
		
        if (data != null && metadata.containsCarriageReturnInDelimiters()) {
            status.add(new ConfigurationProblem("Cannot use carriage return as a delimiter when inline data is specified!",
            		Severity.ERROR, this, Priority.NORMAL, XML_DATA_ATTRIBUTE));
        }

		return status;
	}
	
	@Override
	public synchronized void init() throws ComponentNotReadyException {
        if (isInitialized()) {
//            throw new IllegalStateException("The lookup table has already been initialized!");
        	return;
        }

		super.init();
		
		if (metadata == null) {
			metadata = getGraph().getDataRecordMetadata(metadataId, true);
		}
        if (metadata == null) {
        	throw new ComponentNotReadyException("Metadata " + StringUtils.quote(metadataId) + 
					" does not exist!!!");
        }

		if (startKey == null) {
			startKey = new RecordKey(startFields, metadata);
			startKey.init();
		}        
		startField = startKey.getKeyFields();
		if (endKey == null) {
			endKey = new RecordKey(endFields, metadata);
			endKey.init();
		}		
		endField = endKey.getKeyFields();
		
		if (startInclude == null) {
			startInclude = new boolean[startFields.length];
			Arrays.fill(startInclude, DEFAULT_START_INCLUDE);
		}
		
		if (endInclude == null) {
			endInclude = new boolean[endFields.length];
			Arrays.fill(endInclude, DEFAULT_END_INCLUDE);
		}
		
		if (collators == null && useI18N) {
			collators = new RuleBasedCollator[metadata.getFields().length];
			if (locale != null) {
				Arrays.fill(collators, (RuleBasedCollator)Collator.getInstance(MiscUtils.createLocale(locale)));
			}else{
				Arrays.fill(collators, (RuleBasedCollator)Collator.getInstance());
			}
		}
		
		comparator = new IntervalRecordComparator(metadata, startField, endField, getCollator());
		collators = ((IntervalRecordComparator)comparator).getCollators();
		
		lookupTable = Collections.synchronizedSortedSet(new TreeSet<DataRecord>(comparator));


	    if (dataParser == null && (fileURL != null || data!= null)) {
			dataParser = TextParserFactory.getParser(metadata, charset);
	    }
	    
	    DataRecord tmpRecord = DataRecordFactory.newRecord(metadata);
	    tmpRecord.init();

	    //read records from file
        if (dataParser != null) {
            dataParser.init();
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
		
		// read records from file
		if (dataParser != null) {
			DataRecord tmpRecord = DataRecordFactory.newRecord(metadata);
			tmpRecord.init();
			try {
				if (fileURL != null) {
					dataParser.setDataSource(FileUtils.getReadableChannel((getGraph() != null) ? getGraph().getRuntimeContext().getContextURL() : null, fileURL));
				} else if (data != null) {
					dataParser.setDataSource(new ByteArrayInputStream(data.getBytes()));
				}
				while (dataParser.getNext(tmpRecord) != null) {
					lookupTable.add(tmpRecord.duplicate());
				}
			} catch (Exception e) {
				throw new ComponentNotReadyException(this, e.getMessage(), e);
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
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
		lookupTable.clear();
	}

	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}

    @Override
    public synchronized void free() {
        if (isInitialized()) {
            super.free();
        }
    }
    
	@Override
	public DataRecordMetadata getMetadata() {
		return metadata;
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

		lookupTable.add(dataRecord.duplicate());

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
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<DataRecord> iterator() {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

		return lookupTable.iterator();
	}
	
    public static RangeLookupTable fromProperties(TypedProperties properties) throws AttributeNotFoundException,
			GraphConfigurationException {

    	for (String property : REQUESTED_ATTRIBUTE) {
			if (!properties.containsKey(property)) {
				throw new AttributeNotFoundException(property);
			}
		}
    	String type = properties.getStringProperty(XML_TYPE_ATTRIBUTE);
    	if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_RANGE_LOOKUP)){
    		throw new GraphConfigurationException("Can't create range lookup table from type " + type);
    	}
        String id = properties.getStringProperty(XML_ID_ATTRIBUTE);
        String metadataString = properties.getStringProperty(XML_METADATA_ID);
        String[] startFields = properties.getStringProperty(XML_START_FIELDS).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        String[] endFields = properties.getStringProperty(XML_END_FIELDS).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        RangeLookupTable lookupTable = new RangeLookupTable(id, metadataString, startFields, endFields);
        if (properties.containsKey(XML_NAME_ATTRIBUTE)){
        	lookupTable.setName(properties.getStringProperty(XML_NAME_ATTRIBUTE));
        }
        lookupTable.setUseI18N(properties.getBooleanProperty(XML_USE_I18N, false));
		if (properties.containsKey(XML_LOCALE)){
			lookupTable.setLocale(XML_LOCALE);
		}
		if (properties.containsKey(XML_FILE_URL)){
			lookupTable.setFileURL(properties.getStringProperty(XML_FILE_URL));
		}
		lookupTable.setCharset(properties.getStringProperty(XML_CHARSET, Defaults.DataParser.DEFAULT_CHARSET_DECODER));
		boolean[] startInclude = null;
		if (properties.containsKey(XML_START_INCLUDE)){
			String[] sI = properties.getStringProperty(XML_START_INCLUDE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			startInclude = new boolean[sI.length];
			for (int i = 0; i < sI.length; i++) {
				startInclude[i] = Boolean.parseBoolean(sI[i]);
			}
		}
		boolean[] endInclude = null;
		if (properties.containsKey(XML_END_INCLUDE)){
			String[] eI = properties.getStringProperty(XML_END_INCLUDE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			endInclude = new boolean[eI.length];
			for (int i = 0; i < eI.length; i++) {
				endInclude[i] = Boolean.parseBoolean(eI[i]);
			}
			if (startInclude == null) {
		    	startInclude = new boolean[endInclude.length];
		    	for (int i = 0; i < endInclude.length; i++) {
					startInclude[i] = true;
				}
			}
		}else if (properties.containsKey(XML_START_INCLUDE)){
		   	endInclude = new boolean[startInclude.length];
			for (int i = 0; i < endInclude.length; i++) {
				endInclude[i] = false;
			}
		}
		lookupTable.setStartInclude(startInclude);
		lookupTable.setEndInclude(endInclude);
		
        if (properties.containsKey(XML_DATA_ATTRIBUTE)) {
        	lookupTable.setData(properties.getStringProperty(XML_DATA_ATTRIBUTE));
        }

        return lookupTable;
    }
    	
    public static RangeLookupTable fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException, AttributeNotFoundException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
        RangeLookupTable lookupTable = null;
        String id;
        String type;
        String metadata;
        String[] startFields;
        String[] endFields;
        
        //reading obligatory attributes
        id = xattribs.getString(XML_ID_ATTRIBUTE);
        type = xattribs.getString(XML_TYPE_ATTRIBUTE);
        metadata = xattribs.getString(XML_METADATA_ID);
        startFields = xattribs.getString(XML_START_FIELDS).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        endFields = xattribs.getString(XML_END_FIELDS).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        
        //check type
        if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_RANGE_LOOKUP)) {
            throw new XMLConfigurationException("Can't create range lookup table from type " + type);
        }
        
        lookupTable = new RangeLookupTable(id, metadata, startFields, endFields);
        
        if (xattribs.exists(XML_NAME_ATTRIBUTE)){
        	lookupTable.setName(xattribs.getString(XML_NAME_ATTRIBUTE));
        }
        lookupTable.setUseI18N(xattribs.getBoolean(XML_USE_I18N, false));
		if (xattribs.exists(XML_LOCALE)){
			lookupTable.setLocale(XML_LOCALE);
		}
		if (xattribs.exists(XML_FILE_URL)){
			lookupTable.setFileURL(xattribs.getString(XML_FILE_URL));
		}
		lookupTable.setCharset(xattribs.getString(XML_CHARSET, Defaults.DataParser.DEFAULT_CHARSET_DECODER));
		boolean[] startInclude = null;
		if (xattribs.exists(XML_START_INCLUDE)){
			String[] sI = xattribs.getString(XML_START_INCLUDE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			startInclude = new boolean[sI.length];
			for (int i = 0; i < sI.length; i++) {
				startInclude[i] = Boolean.parseBoolean(sI[i]);
			}
		}
		boolean[] endInclude = null;
		if (xattribs.exists(XML_END_INCLUDE)){
			String[] eI = xattribs.getString(XML_END_INCLUDE).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
			endInclude = new boolean[eI.length];
			for (int i = 0; i < eI.length; i++) {
				endInclude[i] = Boolean.parseBoolean(eI[i]);
			}
			if (startInclude == null) {
		    	startInclude = new boolean[endInclude.length];
		    	for (int i = 0; i < endInclude.length; i++) {
					startInclude[i] = true;
				}
			}
		}else if (xattribs.exists(XML_START_INCLUDE)){
		   	endInclude = new boolean[startInclude.length];
			for (int i = 0; i < endInclude.length; i++) {
				endInclude[i] = false;
			}
		}
		lookupTable.setStartInclude(startInclude);
		lookupTable.setEndInclude(endInclude);
		
        if (xattribs.exists(XML_DATA_ATTRIBUTE)) {
        	lookupTable.setData(xattribs.getString(XML_DATA_ATTRIBUTE));
        }

        return lookupTable;
	}
	
	public boolean[] getEndInclude() {
		return endInclude;
	}

	public void setEndInclude(boolean[] endInclude) {
		this.endInclude = endInclude;
	}
	
	public void setEndInclude(boolean endInclude){
		setEndInclude(new boolean[]{endInclude});
	}

	public boolean[] getStartInclude() {
		return startInclude;
	}

	public void setStartInclude(boolean[] startInclude) {
		this.startInclude = startInclude;
	}

	public void setStartInclude(boolean startInclude){
		setStartInclude(new boolean[]{startInclude});
	}

	public int[] getStartFields(){
		if (startField == null) {
			startKey = new RecordKey(startFields, metadata);
			startKey.init();
			startField = startKey.getKeyFields();			
		}
		return startField;
	}

	public int[] getEndFields(){
		if (endField == null) {
			endKey = new RecordKey(endFields, metadata);
			endKey.init();
			endField = endKey.getKeyFields();			
		}
		return endField;
	}
	
	public String getLocale() {
		return locale;
	}

	public void setLocale(String locale) {
		this.locale = locale;
	}

	public boolean isUseI18N() {
		return useI18N;
	}

	public void setUseI18N(boolean useI18N) {
		this.useI18N = useI18N;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getFileURL() {
		return fileURL;
	}

	public void setFileURL(String fileURL) {
		this.fileURL = fileURL;
	}
	
	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public RuleBasedCollator getCollator() {
		return collators != null && collators.length > 0 ? collators[0] : null;
	}

	public RuleBasedCollator[] getCollators() {
		return collators;
	}


	@Override
	public Lookup createLookup(RecordKey key) {
		return createLookup(key, null);
	}

	@Override
	public Lookup createLookup(RecordKey key, DataRecord keyRecord) {
        if (!isInitialized()) {
            throw new NotInitializedException(this);
        }

		return new RangeLookup(this, key, keyRecord);
	}

	@Override
	public DataRecordMetadata getKeyMetadata() throws ComponentNotReadyException {
		if (!isInitialized()) {
            throw new NotInitializedException(this);
		}
		
		DataRecordMetadata keyMetadata = new DataRecordMetadata( "_rangeLookupTable_" + getName(), getMetadata().getRecType());
		keyMetadata.setFieldDelimiter(getMetadata().getFieldDelimiter());
		keyMetadata.setRecordDelimiter(getMetadata().getRecordDelimiter());

		for (int index : startField) {
			keyMetadata.addField(getMetadata().getField(index).duplicate());
		}

		return keyMetadata;
	}

	@Override
	public void setCurrentPhase(int phase) {
		//isn't required by the lookup table
	}

}

class RangeLookup implements Lookup{
	
	private SortedSet<DataRecord> lookup;
	private RangeLookupTable lookupTable;
	private DataRecord tmpRecord;
	private int[] startField;
	private int[] endField;
	private RecordKey key;
	private DataRecord inRecord;
	private int[] keyFields;
	private SortedSet<DataRecord> subTable;
	private Iterator<DataRecord> subTableIterator;
	private DataRecord next;
	private RuleBasedCollator[] collators;
	private boolean useCollator = false;
	private int[] comparison;
	private int numFound;
	private boolean[] startInclude;
	private boolean[] endInclude;

	RangeLookup(RangeLookupTable lookup, RecordKey key, DataRecord record){
		this.lookupTable = lookup;
	    tmpRecord=DataRecordFactory.newRecord(lookupTable.getMetadata());
	    tmpRecord.init();
	    startField = lookupTable.getStartFields();
	    endField = lookupTable.getEndFields();
	    startInclude = lookupTable.getStartInclude();
	    endInclude = lookupTable.getEndInclude();
	    for (Collator col: collators = lookupTable.getCollators()) {
	    	if (col != null) {
	    		useCollator = true;
	    		break;
	    	}
	    }
	    this.lookup = lookupTable.lookupTable;
		this.key = key;
		this.inRecord = record;
		this.keyFields = key.getKeyFields();
	}

	@Override
	public RecordKey getKey() {
		return key;
	}

	@Override
	public LookupTable getLookupTable() {
		return lookupTable;
	}
	
	@Override
	public synchronized int getNumFound() {
		int alreadyFound = numFound;
		while (getNext() != null) {};
		int tmp = numFound;
		subTableIterator = subTable.iterator();
		for (int i=0;i<alreadyFound;i++){
			getNext();
		}
		return tmp;
	}

	@Override
	public void seek() {
		if (inRecord == null) throw new IllegalStateException("No key data for performing lookup");
		for (int i = 0; i < startField.length; i++){
			tmpRecord.getField(startField[i]).setValue(inRecord.getField(keyFields[i]));
			tmpRecord.getField(endField[i]).setValue(inRecord.getField(keyFields[i]));
		}
		synchronized (lookup) {
			subTable = lookup.tailSet(tmpRecord);
			subTableIterator = subTable.iterator();
		}
		numFound = 0;
		next = getNext();
	}

	@Override
	public void seek(DataRecord keyRecord) {
		inRecord = keyRecord;
		seek();
	}

	private DataRecord getNext(){
		if (subTableIterator == null) {
			return null;
		}
		boolean[] ok = new boolean[startField.length];
		Arrays.fill(ok, false);
		boolean allOK = false;
		DataRecord result = null;
		nextCandidate: while (!(allOK = checkAll(ok)) && subTableIterator.hasNext()){
			Arrays.fill(ok, false);
			result = subTableIterator.next();
			for (int i = 0; i < startField.length; i++) {
				comparison = compare(tmpRecord, result, i);
				if ((comparison[0] < 0 || (comparison[0] == 0 && !startInclude[i])) || (comparison[1] > 0 || (comparison[1] == 0 && !endInclude[i]))) {
					//if value is not in interval try next
					continue nextCandidate;
				} else {
					ok[i] = true;
				}
			}
		};
		if (!allOK) {
			return null;
		}
		numFound++;
		return result;
	}
	
	private boolean checkAll(boolean[] value) {
		for (boolean b : value) {
			if (!b) return false;
		}
		return true;
	}
	
	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public DataRecord next() {
		if (next == null) {
			throw new NoSuchElementException();
		}
		DataRecord tmp = next.duplicate();
		next = getNext();
		return tmp;
	}

	private int[] compare(DataRecord keyRecord, DataRecord lookupRecord, int keyFieldNo) {
		int startComp = 0, endComp = 0;
		//if start field of lookup record is null, start field of key record is always greater
		if (lookupRecord.getField(startField[keyFieldNo]).isNull()) {
			startComp = 1;
		}
		//if end field of lookup record is null, end field of key record is always smaller
		if (lookupRecord.getField(endField[keyFieldNo]).isNull()){
			endComp = -1;
		}
		if (startComp != 1) {//lookup record's start field is not null
			if (useCollator && collators[startField[keyFieldNo]] != null && lookupRecord.getField(startField[keyFieldNo]).getMetadata().getType() == DataFieldMetadata.STRING_FIELD){
				startComp = ((StringDataField)keyRecord.getField(startField[keyFieldNo])).compareTo(
						lookupRecord.getField(startField[keyFieldNo]), collators[startField[keyFieldNo]]);
			}else{
				startComp = keyRecord.getField(startField[keyFieldNo]).compareTo(
						lookupRecord.getField(startField[keyFieldNo]));
			}
		}
		if (endComp != -1) {//lookup record's end field is not null
			if (useCollator && collators[endField[keyFieldNo]] != null && lookupRecord.getField(endField[keyFieldNo]).getMetadata().getType() == DataFieldMetadata.STRING_FIELD){
				endComp = ((StringDataField)keyRecord.getField(startField[keyFieldNo])).compareTo(
						lookupRecord.getField(endField[keyFieldNo]), collators[endField[keyFieldNo]]);
			}else{
				endComp = keyRecord.getField(endField[keyFieldNo]).compareTo(
						lookupRecord.getField(endField[keyFieldNo]));
			}
		}
		return new int[]{startComp, endComp};
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException("Method not supported!");
	}
	
}

	/**
	 * Comparator for special records (defining range lookup table). 
	 * It compares given start and end fields of two records using RecordComparator class.
	 * 
	 * @see RecordComparator
	 *
	 */
	class IntervalRecordComparator implements Comparator<DataRecord>{
		
		RecordComparator[] startComparator;//comparators for start fields
		int[] startFields;
		RecordComparator[] endComparator;//comparators for end fields
		int[] endFields;
		int startComparison;
		int endComparison;
		RuleBasedCollator[] collators;

		/**
		 * Costructor
		 * 
		 * @param metadata metadata of records, which defines lookup table
		 * @param collator collator for comparing string data fields
		 */
		public IntervalRecordComparator(DataRecordMetadata metadata, int[] startFields,
				int[] endFields, RuleBasedCollator collator) {
			if (startFields.length != endFields.length) {
				throw new IllegalArgumentException("Number of start fields is diffrent then number of and fields!!!");
			}
			this.startFields = startFields;
			this.endFields = endFields;
			startComparator = new RecordComparator[startFields.length];
			endComparator = new RecordComparator[endFields.length];
			collators = new RuleBasedCollator[metadata.getFields().length];
			Arrays.fill(collators, collator);
			for (int i=0;i<startComparator.length;i++){
				startComparator[i] = new RecordComparator(new int[]{startFields[i]},collator);
				startComparator[i].updateCollators(metadata);
				if (((RecordComparator)startComparator[i]).getCollators() != null && ((RecordComparator)startComparator[i]).getCollators().length > 0 &&
						((RecordComparator)startComparator[i]).getCollators()[0] != null) this.collators[startFields[i]] = ((RecordComparator)startComparator[i]).getCollators()[0];
				endComparator[i] = new RecordComparator(new int[]{endFields[i]},collator);
				endComparator[i].updateCollators(metadata);
				if (((RecordComparator)endComparator[i]).getCollator() != null && ((RecordComparator)endComparator[i]).getCollators().length > 0 &&
						((RecordComparator)endComparator[i]).getCollators()[0] != null) this.collators[endFields[i]] = ((RecordComparator)endComparator[i]).getCollators()[0];
			}
		}

		protected RuleBasedCollator[] getCollators() {
			return collators;
		}
		
		public IntervalRecordComparator(DataRecordMetadata metadata, int[] startFields,
				int[] endFields) {
			this(metadata, startFields, endFields, null);
		}
		
		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 * 
		 * Intervals are equal if their start and end points are equal. Null values are treated as "infinities".
		 * Interval o2 is after interval o1 if o1 is subinterval of o2 or start of o2 is
		 * after start of o1 and end of o2 is after end of o1:
		 * startComparison				endComparison			intervalComparison
		 * o1.start.compareTo(o2.start)	o1.end.compareTo(o2.end)o1.compareTo(o2)
		 * -1							-1						-1
		 * -1				 			 0				 		 1(o2 is subinterval of o1) 	
		 * -1							 1						 1(o2 is subinterval of o1)
		 *  0							-1						-1(o1 is subinterval of o2)
		 *  0							 0						 0(equal)
		 *  0							 1						 1(o2 is subinterval of o1)
		 *  1							-1						-1(o1 is subinterval of o2)
		 *  1							 0						-1(o1 is subinterval of o2)
		 *  1							 1						 1
		 */
		@Override
		public int compare(DataRecord o1, DataRecord o2) {
			for (int i=0;i<startComparator.length;i++){
				if (o1.getField(startFields[i]).isNull() && o2.getField(startFields[i]).isNull()) {
					startComparison = 0;
				}else if (o1.getField(startFields[i]).isNull()) {
					startComparison = -1;
				}else if (o2.getField(startFields[i]).isNull()) {
					startComparison = 1;
				}else{
					startComparison = startComparator[i].compare(o1, o2);
				}
				if (o1.getField(endFields[i]).isNull() && o2.getField(endFields[i]).isNull()) {
					endComparison = 0;
				}else if (o1.getField(endFields[i]).isNull()) {
					endComparison = 1;
				}else if (o2.getField(endFields[i]).isNull()){
					endComparison = -1;
				}else{
					endComparison = endComparator[i].compare(o1, o2);
				}
				if (endComparison < 0) return -1;
				if (!(startComparison == 0 && endComparison == 0) ){
					if (startComparison > 0 && endComparison == 0) {
						return -1;
					}else{
						return 1;
					}
				}
			}
			return 0;
		}

	}


