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
import org.jetel.data.parser.DelimitedDataParser;
import org.jetel.data.parser.FixLenCharDataParser;
import org.jetel.data.parser.Parser;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.GraphElement;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.FileUtils;
import org.jetel.util.MiscUtils;
import org.w3c.dom.Element;

/**
 * Range lookup table contains records, which defines intervals. It means that they must
 * have special structure: first field is the name of the interval, odd fields marks starts of
 * intervals, even fields (from 2) means corresponding ends of intervals, eg: Lookup table defined 
 * as follows:<br>
 * low_slow,0,10,0,50<br>
 * low_fast,0,10,50,100<br>
 * high_slow,10,20,0,50<br>
 * high_fast,10,20,50,100<br>
 * has 4 intervals with 2 searching parameters: first from interval 0-10, and second from interval 0-100.<br>
 * Intervals can overlap. By default start point is included and end point is excluded.
 * It is possible to change this settings during construction or by by proper set method.
 */
public class RangeLookupTable extends GraphElement implements LookupTable {
	
	
    private static final String XML_LOOKUP_TYPE_RANGE_LOOKUP = "rangeLookup";
    private static final String XML_METADATA_ID ="metadata";
    private static final String XML_LOOKUP_DATA_TYPE = "dataType";
    private static final String XML_FILE_URL = "fileURL";
    private static final String XML_DATA_TYPE_DELIMITED ="delimited";
    private static final String XML_CHARSET = "charset";
    private static final String XML_USE_I18N = "useI18N";
    private static final String XML_LOCALE = "locale";
    private static final String XML_START_INCLUDE = "startInclude";
    private static final String XML_END_INCLUDE = "endInclude";

    protected DataRecordMetadata metadata;//defines lookup table
	protected Parser dataParser;
	protected TreeSet<DataRecord> lookupTable;//set of intervals
	protected SortedSet<DataRecord> subTable;
	protected int numFound;
	protected RecordKey lookupKey;
	protected DataRecord tmpRecord;
	protected IntervalRecordComparator comparator;
	protected int[] keyFields = null;
	protected Iterator<DataRecord> subTableIterator;
	protected RuleBasedCollator collator = null;
	protected boolean[] startInclude;
	protected boolean[] endInclude;
	
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
	public RangeLookupTable(String id, DataRecordMetadata metadata, Parser parser, 
			RuleBasedCollator collator, boolean[] startInclude, boolean[] endInclude){
		super(id);
		this.metadata = metadata;
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
	public RangeLookupTable(String id, DataRecordMetadata metadata, Parser parser, 
			RuleBasedCollator collator){
		this(id,metadata,parser,collator,new boolean[(metadata.getNumFields() - 1)/2],
				new boolean[(metadata.getNumFields() - 1)/2]);
		Arrays.fill(startInclude, true);
		Arrays.fill(endInclude, false);
	}

	public RangeLookupTable(String id, DataRecordMetadata metadata, Parser parser, 
			boolean[] startInclude, boolean[] endInclude){
		this(id,metadata,parser,null,startInclude,endInclude);
	}
	
	public RangeLookupTable(String id, DataRecordMetadata metadata, Parser parser){
		this(id,metadata,parser,null);
	}

	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public synchronized void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		comparator = new IntervalRecordComparator(metadata,collator);
		
		lookupTable = new TreeSet<DataRecord>(comparator);

	    tmpRecord=new DataRecord(metadata);
	    tmpRecord.init();
	    //read records from file
        if (dataParser != null) {
            dataParser.init(metadata);
            try {
                while (dataParser.getNext(tmpRecord) != null) {
                    lookupTable.add(tmpRecord.duplicate());
                }
            } catch (JetelException e) {
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
			tmpRecord.getField(2*i+1).setValue(keys[i]);
			tmpRecord.getField(2*(i+1)).setValue(keys[i]);
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
		for (int i=0;i<lookupKey.getLenght();i++){
			tmpRecord.getField(2*i+1).setValue(keyRecord.getField(keyFields[i]));
			tmpRecord.getField(2*(i+1)).setValue(keyRecord.getField(keyFields[i]));
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
		for (int i=1;i<tmp.getNumFields();i+=2){
			if (collator != null){
				if (tmpRecord.getField(i).getMetadata().getType() == DataFieldMetadata.STRING_FIELD){
					startComparison = ((StringDataField)tmpRecord.getField(i)).compareTo(
							tmp.getField(i), collator);
					endComparison = ((StringDataField)tmpRecord.getField(i + 1)).compareTo(
							tmp.getField(i + 1),collator);
				}else{
					startComparison = tmpRecord.getField(i).compareTo(tmp.getField(i));
					endComparison = tmpRecord.getField(i + 1).compareTo(tmp.getField(i + 1));
				}
			}else{
				startComparison = tmpRecord.getField(i).compareTo(tmp.getField(i));
				endComparison = tmpRecord.getField(i + 1).compareTo(tmp.getField(i + 1));
			}
			if ((startComparison < 0 || (startComparison == 0 && !startInclude[(i-1)/2])) ||
				(endComparison > 0    || (endComparison == 0   && !endInclude[(i-1)/2])) ) {
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
        
        //reading obligatory attributes
        try {
            id = xattribs.getString(XML_ID_ATTRIBUTE);
            type = xattribs.getString(XML_TYPE_ATTRIBUTE);
        } catch(AttributeNotFoundException ex) {
            throw new XMLConfigurationException("Can't create lookup table - " + ex.getMessage(),ex);
        }
        
        //check type
        if (!type.equalsIgnoreCase(XML_LOOKUP_TYPE_RANGE_LOOKUP)) {
            throw new XMLConfigurationException("Can't create range lookup table from type " + type);
        }
        
        //create simple lookup table
        try{
        	RuleBasedCollator collator = null;
        	if (xattribs.exists(XML_USE_I18N) && xattribs.getBoolean(XML_USE_I18N)) {
        		if (xattribs.exists(XML_LOCALE)) {
        			collator = (RuleBasedCollator)Collator.getInstance(MiscUtils.createLocale(XML_LOCALE));
        		}else{
        			collator = (RuleBasedCollator)Collator.getInstance();
        		}
        	}
            DataRecordMetadata metadata = graph.getDataRecordMetadata(xattribs.getString(XML_METADATA_ID));
            Parser parser;
            String dataTypeStr = xattribs.getString(XML_LOOKUP_DATA_TYPE);
            
            // which data parser to use
            if(dataTypeStr.equalsIgnoreCase(XML_DATA_TYPE_DELIMITED)) {
                parser = new DelimitedDataParser(xattribs.getString(XML_CHARSET, Defaults.DataParser.DEFAULT_CHARSET_DECODER));
            } else {
                parser = new FixLenCharDataParser(xattribs.getString(XML_CHARSET, Defaults.DataParser.DEFAULT_CHARSET_DECODER));
            }
            if (xattribs.exists(XML_FILE_URL)) {
            	parser.setDataSource(FileUtils.getReadableChannel(graph.getProjectURL(), xattribs.getString(XML_FILE_URL)));
            }else{
            	parser = null;
            }
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
            
            if (startInclude != null) {
            	lookupTable =  new RangeLookupTable(id, metadata, parser, collator, startInclude, endInclude);
            }else{
            	lookupTable =  new RangeLookupTable(id, metadata, parser, collator);
            }
            return lookupTable;
            
         }catch(Exception ex){
             throw new XMLConfigurationException("can't create simple lookup table",ex);
         }
	}
	
	public boolean[] getEndInclude() {
		return endInclude;
	}

	public void setEndInclude(boolean[] endInclude) {
		if (endInclude.length != (metadata.getNumFields() - 1)/2) {
			throw new InvalidParameterException("endInclude parameter has wrong number " +
					"of elements: " + endInclude.length + " (should be " + 
					(metadata.getNumFields() - 1)/2 + ")");
		}
		this.endInclude = endInclude;
	}
	
	public void setEndInclude(boolean endInclude){
		setEndInclude(new boolean[]{endInclude});
	}

	public boolean[] getStartInclude() {
		return startInclude;
	}

	public void setStartInclude(boolean[] startInclude) {
		if (startInclude.length != (metadata.getNumFields() - 1)/2) {
			throw new InvalidParameterException("startInclude parameter has wrong number " +
					"of elements: " + startInclude.length + " (should be " + 
					(metadata.getNumFields() - 1)/2 + ")");
		}
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
		RecordComparator[] endComparator;//comparators for even fields
		int startComparison;
		int endComparison;

		/**
		 * Costructor
		 * 
		 * @param metadata metadata of records, which defines lookup table
		 * @param collator collator for comparing string data fields
		 */
		public IntervalRecordComparator(DataRecordMetadata metadata, RuleBasedCollator collator) {
			startComparator = new RecordComparator[(metadata.getNumFields()-1)/2];
			endComparator = new RecordComparator[(metadata.getNumFields()-1)/2];
			for (int i=0;i<startComparator.length;i++){
				startComparator[i] = new RecordComparator(new int[]{2*i+1},collator);
				endComparator[i] = new RecordComparator(new int[]{2*(i+1)},collator);
			}
		}

		public IntervalRecordComparator(int[] keyFields) {
			this(metadata,null);
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


}

