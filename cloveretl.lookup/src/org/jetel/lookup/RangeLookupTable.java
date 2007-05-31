package org.jetel.lookup;

import java.security.InvalidParameterException;
import java.text.Collator;
import java.text.RuleBasedCollator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordComparator;
import org.jetel.data.RecordKey;
import org.jetel.data.StringDataField;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.parser.DataParser;
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenByteDataParser;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.jetel.util.MiscUtils;
import org.jetel.util.StringUtils;
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
 * It is possible to change this settings during construction or by by proper set method.
 */
public class RangeLookupTable extends GraphElement implements LookupTable {
	
	
    private static final String XML_LOOKUP_TYPE_RANGE_LOOKUP = "rangeLookup";
    private static final String XML_METADATA_ID ="metadata";
    private static final String XML_LOOKUP_DATA_TYPE = "dataType";
    private static final String XML_FILE_URL = "fileURL";
    private static final String XML_DATA_TYPE_DELIMITED ="delimited";
    private static final String XML_DATA_TYPE_FIXED ="fixed";
 	private static final String XML_BYTEMODE_ATTRIBUTE = "byteMode";
    private static final String XML_START_FIELDS = "startFields";
    private static final String XML_END_FIELDS = "endFields";
    private static final String XML_CHARSET = "charset";
    private static final String XML_USE_I18N = "useI18N";
    private static final String XML_LOCALE = "locale";
    private static final String XML_START_INCLUDE = "startInclude";
    private static final String XML_END_INCLUDE = "endInclude";

    protected DataRecordMetadata metadata;//defines lookup table
    protected String metadataId;
	protected Parser dataParser;
	protected TreeSet<DataRecord> lookupTable;//set of intervals
	protected SortedSet<DataRecord> subTable;
	protected int numFound;
	protected RecordKey lookupKey;
	protected RecordKey startKey;
	protected String[] startFields;
	protected int[] startField;
	protected RecordKey endKey;
	protected String[] endFields;
	protected int[] endField;
	protected DataRecord tmpRecord;
	protected IntervalRecordComparator comparator;
	protected int[] keyFields = null;
	protected Iterator<DataRecord> subTableIterator;
	protected RuleBasedCollator collator = null;
	protected boolean[] startInclude;
	protected boolean[] endInclude;
	protected boolean useI18N;
	protected String locale;
	protected String dataType;
	protected boolean byteMode = false;
	protected String charset;
	protected String fileURL;
	
	private DataRecord tmp;
	private int startComparison;
	private int endComparison;
	
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
		this.collator = collator;
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
		Arrays.fill(startInclude, true);
		Arrays.fill(endInclude, false);
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
			metadata = getGraph().getDataRecordMetadata(metadataId);
		}		
        if (metadata == null) {
        	status.add(new ConfigurationProblem("Metadata " + StringUtils.quote(metadataId) + 
					" does not exist!!!", Severity.ERROR, this, Priority.NORMAL, XML_METADATA_ID));
        }

        if (startKey == null) {
			startKey = new RecordKey(startFields, metadata);
		}		
		if (endKey == null) {
			endKey = new RecordKey(endFields, metadata);
		}		
		RecordKey.checkKeys(startKey, XML_START_FIELDS, endKey, XML_END_FIELDS, status, this);
		
		if (startInclude == null) {
			startInclude = new boolean[startFields.length];
			Arrays.fill(startInclude, true);
		}
		
		if (endInclude == null) {
			endInclude = new boolean[endFields.length];
			Arrays.fill(endInclude, false);
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
		
		return status;
	}
	
	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public synchronized void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		if (metadata == null) {
			metadata = getGraph().getDataRecordMetadata(metadataId);
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
		
		if (collator == null && useI18N) {
			if (locale != null) {
				collator = (RuleBasedCollator)Collator.getInstance(MiscUtils.createLocale(locale));
			}else{
				collator = (RuleBasedCollator)Collator.getInstance();
			}
		}
		
		comparator = new IntervalRecordComparator(metadata, startField, endField, collator);
		
		lookupTable = new TreeSet<DataRecord>(comparator);

	    tmpRecord=new DataRecord(metadata);
	    tmpRecord.init();

	    if (dataType != null && fileURL != null) {
	    	if (dataType.equalsIgnoreCase(XML_DATA_TYPE_DELIMITED)) {
	    		dataParser = new DelimitedDataParser(charset);
	    	}else if (dataType.equalsIgnoreCase(XML_DATA_TYPE_FIXED)){
	    		dataParser = byteMode ? new FixLenByteDataParser(charset) : new FixLenCharDataParser(charset);
	    	}else{
	    		dataParser = new DataParser(charset);
	    	}
	    }
	    
	    //read records from file
        if (dataParser != null) {
            dataParser.init(metadata);
            try {
                dataParser.setDataSource(FileUtils.getReadableChannel(getGraph().getProjectURL(), fileURL));
                while (dataParser.getNext(tmpRecord) != null) {
                    lookupTable.add(tmpRecord.duplicate());
                }
            } catch (Exception e) {
                throw new ComponentNotReadyException(this, e.getMessage(), e);
            }
            dataParser.close();
        }
		numFound=0;

}
	
	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#get(java.lang.String)
	 */
	public DataRecord get(String keyString) {
		tmpRecord.getField(1).fromString(keyString);
		tmpRecord.getField(2).fromString(keyString);
		return get();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#get(java.lang.Object[])
	 */
	public DataRecord get(Object[] keys) {
		//prepare "interval" from keyRecord:set start end end for the value
		for (int i=0;i<keys.length;i++){
			tmpRecord.getField(startField[i]).setValue(keys[i]);
			tmpRecord.getField(endField[i]).setValue(keys[i]);
		}
		return get();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#get(org.jetel.data.DataRecord)
	 */
	public DataRecord get(DataRecord keyRecord) {
		if (keyFields == null){
			throw new RuntimeException("Set lookup key first!!!!");
		}
		//prepare "interval" from keyRecord:set start end end for the value
		for (int i=0;i<lookupKey.getLength();i++){
			tmpRecord.getField(startField[i]).setValue(keyRecord.getField(keyFields[i]));
			tmpRecord.getField(endField[i]).setValue(keyRecord.getField(keyFields[i]));
		}
		return get();
	}
	
	/**
	 * This method finds all greater records, then set in get(Object[]) or get(DataRecord) or get(String)
	 * method, in lookup table and stores them in subTable
	 * 
	 * @return
	 */
	private DataRecord get(){
		//get all greater intervals
		subTable = lookupTable.tailSet(tmpRecord);
		subTableIterator = subTable.iterator();
		numFound = 0;
		return getNext();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#getMetadata()
	 */
	public DataRecordMetadata getMetadata() {
		return metadata;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#getNext()
	 */
	public DataRecord getNext() {
		//get next interval if exists
		if (subTableIterator != null && subTableIterator.hasNext()) {
			tmp = subTableIterator.next();
		}else{
			return null;
		}
		//if value is not in interval try next
		for (int i=0;i<startField.length;i++){
			if (collator != null){
				if (tmpRecord.getField(startField[i]).getMetadata().getType() == DataFieldMetadata.STRING_FIELD){
					startComparison = ((StringDataField)tmpRecord.getField(startField[i])).compareTo(
							tmp.getField(startField[i]), collator);
					endComparison = ((StringDataField)tmpRecord.getField(endField[i])).compareTo(
							tmp.getField(endField[i]),collator);
				}else{
					startComparison = tmpRecord.getField(startField[i]).compareTo(tmp.getField(startField[i]));
					endComparison = tmpRecord.getField(endField[i]).compareTo(tmp.getField(endField[i]));
				}
			}else{
				startComparison = tmpRecord.getField(startField[i]).compareTo(tmp.getField(startField[i]));
				endComparison = tmpRecord.getField(endField[i]).compareTo(tmp.getField(endField[i]));
			}
			if ((startComparison < 0 || (startComparison == 0 && !startInclude[i])) ||
				(endComparison > 0    || (endComparison == 0   && !endInclude[i])) ) {
				return getNext();
			}
		}
		numFound++;
		return tmp;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#getNumFound()
	 */
	public int getNumFound() {
		int alreadyFound = numFound;
		while (getNext() != null) {}
		int tmp = numFound;
		subTableIterator = subTable.iterator();
		for (int i=0;i<alreadyFound;i++){
			getNext();
		}
		return tmp;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#put(java.lang.Object, org.jetel.data.DataRecord)
	 */
	public boolean put(Object key, DataRecord data) {
		lookupTable.add(data.duplicate());
		return true;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#remove(java.lang.Object)
	 */
	public boolean remove(Object key) {
        if (key instanceof DataRecord) {
            return lookupTable.remove(key);
        }else{
            throw new IllegalArgumentException("Requires key parameter of type "+DataRecord.class.getName());
        }
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#setLookupKey(java.lang.Object)
	 */
	public void setLookupKey(Object key) {
		if (key instanceof RecordKey){
	        this.lookupKey=((RecordKey)key);
	        keyFields = lookupKey.getKeyFields();
	    }else{
	        throw new RuntimeException("Incompatible Object type specified as lookup key: "+key.getClass().getName());
	    }
	}

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<DataRecord> iterator() {
		return lookupTable.iterator();
	}
	
	public static RangeLookupTable fromXML(TransformationGraph graph, Element nodeXML) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(nodeXML, graph);
        RangeLookupTable lookupTable = null;
        String id;
        String type;
        String metadata;
        String[] startFields;
        String[] endFields;
        
        //reading obligatory attributes
        try {
            id = xattribs.getString(XML_ID_ATTRIBUTE);
            type = xattribs.getString(XML_TYPE_ATTRIBUTE);
            metadata = xattribs.getString(XML_METADATA_ID);
            startFields = xattribs.getString(XML_START_FIELDS).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
            endFields = xattribs.getString(XML_END_FIELDS).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX);
        } catch(AttributeNotFoundException ex) {
            throw new XMLConfigurationException("Can't create lookup table - " + ex.getMessage(),ex);
        }
        
        //check type
        if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_RANGE_LOOKUP)) {
            throw new XMLConfigurationException("Can't create range lookup table from type " + type);
        }
        
        lookupTable = new RangeLookupTable(id, metadata, startFields, endFields);
        
        try {
			lookupTable.setUseI18N(xattribs.getBoolean(XML_USE_I18N, false));
			if (xattribs.exists(XML_LOCALE)){
				lookupTable.setLocale(XML_LOCALE);
			}
			if (xattribs.exists(XML_LOOKUP_DATA_TYPE)) {
				lookupTable.setDataType(xattribs.getString(XML_LOOKUP_DATA_TYPE));
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
			
			lookupTable.setByteMode(xattribs.getBoolean(XML_BYTEMODE_ATTRIBUTE, false));
			
			return lookupTable;
			
		} catch (AttributeNotFoundException e) {
            throw new XMLConfigurationException("can't create simple lookup table",e);
		}
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

	
	/**
	 * Comparator for special records (defining range lookup table). 
	 * It compares odd and even fields of two records using RecordComparator class.
	 * 
	 * @see RecordComparator
	 *
	 */
	private class IntervalRecordComparator implements Comparator<DataRecord>{
		
		RecordComparator[] startComparator;//comparators for odd fields
		int[] startFields;
		RecordComparator[] endComparator;//comparators for even fields
		int[] endFields;
		int startComparison;
		int endComparison;

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
			startComparator = new RecordComparator[startFields.length];
			endComparator = new RecordComparator[endFields.length];
			for (int i=0;i<startComparator.length;i++){
				startComparator[i] = new RecordComparator(new int[]{startFields[i]},collator);
				endComparator[i] = new RecordComparator(new int[]{endFields[i]},collator);
			}
		}

		public IntervalRecordComparator(DataRecordMetadata metadata, int[] startFields,
				int[] endFields) {
			this(metadata, startFields, endFields, null);
		}
		
		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 * 
		 * Intervals are equal if their start and end points are equal.
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
		public int compare(DataRecord o1, DataRecord o2) {
			for (int i=0;i<startComparator.length;i++){
				startComparison = startComparator[i].compare(o1, o2);
				endComparison = endComparator[i].compare(o1, o2);
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

	public boolean isByteMode() {
		return byteMode;
	}

	public void setByteMode(boolean byteMode) {
		this.byteMode = byteMode;
	}

	public String getCharset() {
		return charset;
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public String getFileURL() {
		return fileURL;
	}

	public void setFileURL(String fileURL) {
		this.fileURL = fileURL;
	}


}

