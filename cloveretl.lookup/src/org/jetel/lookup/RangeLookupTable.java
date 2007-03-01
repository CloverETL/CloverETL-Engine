package org.jetel.lookup;

import java.text.RuleBasedCollator;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordComparator;
import org.jetel.data.RecordKey;
import org.jetel.data.lookup.LookupTable;
import org.jetel.data.parser.Parser;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.JetelException;
import org.jetel.graph.GraphElement;
import org.jetel.metadata.DataRecordMetadata;

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
 * Intervals can overlap, but then to get all resulting intervals it is necessery to go through all defined.
 */
public class RangeLookupTable extends GraphElement implements LookupTable {
	
	
	protected DataRecordMetadata metadata;//defines lookup table
	protected Parser dataParser;
	protected TreeSet<DataRecord> lookupTable;//set of intervals
	protected SortedSet<DataRecord> subTable;
	protected int numFound;
	protected RecordKey lookupKey;
	protected DataRecord tmpRecord;
	private DataRecord tmp;
	protected IntervalRecordComaprator comparator;
	protected int[] keyFields = null;
	protected Iterator<DataRecord> subTableIterator;
	protected RuleBasedCollator collator = null;
	protected boolean intervalOverlap = true;
	
	/**
	 * Constructor for most general range lookup table 
	 * 
	 * @param id id
	 * @param metadata metadata defining this lookup table
	 * @param parser parser for reading defining records
	 * @param collator collator for comparing string fields
	 * @param intervalOverlap indicates if intervals can overlap
	 */
	public RangeLookupTable(String id, DataRecordMetadata metadata, Parser parser, 
			RuleBasedCollator collator, boolean intervalOverlap){
		super(id);
		this.metadata = metadata;
		this.dataParser = parser;
		this.collator = collator;
		this.intervalOverlap = intervalOverlap;
	}

	public RangeLookupTable(String id, DataRecordMetadata metadata, Parser parser, 
			RuleBasedCollator collator){
		this(id,metadata,parser,collator,true);
	}

	public RangeLookupTable(String id, DataRecordMetadata metadata, Parser parser, boolean intervalOverlap){
		this(id,metadata,parser,null, intervalOverlap);
	}
	
	public RangeLookupTable(String id, DataRecordMetadata metadata, Parser parser){
		this(id,metadata,parser,null,true);
	}


	/* (non-Javadoc)
	 * @see org.jetel.graph.GraphElement#init()
	 */
	@Override
	public synchronized void init() throws ComponentNotReadyException {
        if(isInitialized()) return;
		super.init();
		
		comparator = new IntervalRecordComaprator(metadata,collator);
		
		lookupTable = new TreeSet<DataRecord>(comparator);

	    tmpRecord=new DataRecord(metadata);
	    tmpRecord.init();
	    //read records from file
        if (dataParser != null) {
            dataParser.init(metadata);
            try {
                while (dataParser.getNext(tmpRecord) != null) {
                    lookupTable.add(tmpRecord);
                }
            } catch (JetelException e) {
                throw new ComponentNotReadyException(this, e.getMessage(), e);
            }
            dataParser.close();
        }
		numFound=0;

}
	
	public DataRecord get(String keyString) {
		// TODO Auto-generated method stub
		return null;
	}

	public DataRecord get(Object[] keys) {
		// TODO Auto-generated method stub
		return null;
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
		
		if (!intervalOverlap) {
			subTable = lookupTable.tailSet(tmpRecord);
			numFound = subTable.size() > 0 ? 1 : 0;
			subTableIterator = subTable.iterator();
			return numFound > 0 ? subTableIterator.next() : null;
		}		
		//when intervals overlap we have to go throgh all records
		subTableIterator = lookupTable.iterator();
		return getNext();
	}

	public DataRecordMetadata getMetadata() {
		return metadata;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#getNext()
	 */
	public DataRecord getNext() {
		if (!intervalOverlap) return null;
		
		if (subTableIterator.hasNext()) {
			tmp = subTableIterator.next();
		}else{
			return null;
		}
		int comparison = comparator.compare(tmp, tmpRecord);
		//if key has more than one field we have to try all:
		//for record 10,68 (tmp = 10-10,68-68)
		//records in tailSet:
		//0-10,0-100  - OK
		//0-10,100-200 - bad
		//10-20,0-100 - OK
		//10-20,100-200 - bad
		if (comparison == 0) {
			return tmp;
		}
		return getNext();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#getNumFound()
	 */
	public int getNumFound() {
		if (!intervalOverlap) {
			return numFound;
		}
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#put(java.lang.Object, org.jetel.data.DataRecord)
	 */
	public boolean put(Object key, DataRecord data) {
		lookupTable.add(data);
		return true;
	}

	public boolean remove(Object key) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see org.jetel.data.lookup.LookupTable#setLookupKey(java.lang.Object)
	 */
	public void setLookupKey(Object key) {
        this.lookupKey=((RecordKey)key);
        keyFields = lookupKey.getKeyFields();
	}

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	public Iterator<DataRecord> iterator() {
		return lookupTable.iterator();
	}

	
	/**
	 * Comparator for special records (defining range lookup table). 
	 * It compares odd and even fields of two records using RecordComparator class.
	 * 
	 * @see RecordComparator
	 *
	 */
	private class IntervalRecordComaprator implements Comparator<DataRecord>{
		
		RecordComparator[] startComparator;//comparator for odd fields
		RecordComparator[] endComparator;//comparator for even fields
		int comparison;
		boolean point;//indicates if comparing record is interval or "value", see RangeLookupTable.get(DataRecord) - tmpRecord

		/**
		 * Costructor
		 * 
		 * @param metadata metadata of records, which defines lookup table
		 * @param collator collator for comparing string data fields
		 */
		public IntervalRecordComaprator(DataRecordMetadata metadata, RuleBasedCollator collator) {
			startComparator = new RecordComparator[(metadata.getNumFields()-1)/2];
			endComparator = new RecordComparator[(metadata.getNumFields()-1)/2];
			for (int i=0;i<startComparator.length;i++){
				startComparator[i] = new RecordComparator(new int[]{2*i+1},collator);
				endComparator[i] = new RecordComparator(new int[]{2*(i+1)},collator);
			}
		}

		public IntervalRecordComaprator(int[] keyFields) {
			this(metadata,null);
		}
		
		public int compare(DataRecord o1, DataRecord o2) {
			for (int i=0;i<startComparator.length;i++){
				point = o1.getField(startComparator[i].getKeyFields()[0]).getValue().equals(
							o1.getField(endComparator[i].getKeyFields()[0]).getValue()) ||
						o2.getField(startComparator[i].getKeyFields()[0]).getValue().equals(
							o2.getField(endComparator[i].getKeyFields()[0]).getValue());
				comparison = startComparator[i].compare(o1, o2) + endComparator[i].compare(o1, o2);
				if (!point) {//comparing to intervals
					//if one is not subinterval of the second shuld not return 0
					if (comparison < 0) {
						return -1;
					}
					if (comparison > 0) {
						return 1;
					}
				//when one of o1,o2 is "value" can be inside or on the edge	
				}else if (comparison == -2){
					return -1;
				}else if (comparison == 2){
					return 1;
				}
			}
			return 0;
		}
	}
	
}

