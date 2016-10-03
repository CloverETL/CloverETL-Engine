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
package org.jetel.component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.buffer.CircularFifoBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.Defaults;
import org.jetel.data.HashKey;
import org.jetel.data.RecordKey;
import org.jetel.data.RingRecordBuffer;
import org.jetel.enums.OrderEnum;
import org.jetel.exception.AttributeNotFoundException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationProblem;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.TransformException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.OutputPortDirect;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.ExceptionUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.string.StringUtils;
import org.w3c.dom.Element;

/**
 *  <h3>Dedup Component</h3>
 *
 * <!-- Removes duplicates (based on specified key) from data flow of sorted records-->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Dedup</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Dedup (remove duplicate records) from sorted incoming records based on specified key.<br>
 *  The key is name (or combination of names) of field(s) from input record.
 *  It keeps either First or Last record from the group based on the parameter <emp>{keep}</emp> specified.
 *  All duplicated records are rejected to the second optional port.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>[0]- result of deduplication</td></tr>
 * <td>[0]- all rejected records</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"DEDUP"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>dedupKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe} or can be empty, then all records belong to one group</td>
 *  <tr><td><b>keep</b></td><td>one of "First|Last|Unique" {the fist letter is sufficient, if not defined, then First}</td></tr>
 *  <tr><td><b>equalNULL</b><br><i>optional</i></td><td>specifies whether two fields containing NULL values are considered equal. Default is TRUE.</td></tr>
 *  <tr><td><b>noDupRecord</b><br><i>optional</i></td><td>number of duplicate record to be written to out port. Default is 1.</td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="DISTINCT" type="DEDUP" dedupKey="Name" keep="First"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       April 4, 2002
 */
public class Dedup extends Node {
    private static final Log logger = LogFactory.getLog(Dedup.class);

	private static final String XML_KEEP_ATTRIBUTE = "keep";
	private static final String XML_DEDUPKEY_ATTRIBUTE = "dedupKey";
	private static final String XML_EQUAL_NULL_ATTRIBUTE = "equalNULL";
	private static final String XML_NO_DUP_RECORD_ATTRIBUTE = "noDupRecord";
	private static final String XML_SORTED_ATTRIBUTE = "sorted";
	
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "DEDUP";

	private final static int READ_FROM_PORT = 0;

    private final static int WRITE_TO_PORT = 0;
    private final static int REJECTED_PORT = 1;
    
	private final static String UNKNOWN_KEEP_VALUE = "Unkown type of selection of held record within unique group.";
    
    /** Trigger for selection of held records within deduplication group.
     *
     */
    private enum Keep { KEEP_FIRST, KEEP_LAST, KEEP_UNIQUE };
    	
	private final static int DEFAULT_NO_DUP_RECORD = 1;

	private static final int SINGLE_RECORD = 1;
	
	private Keep keep = Keep.KEEP_FIRST;
	private String[] dedupKeys;
	private OrderEnum[] dedupOrderings;
	private DedupComparator[] orderingComparators;
	private boolean bAutoExists;
	private boolean equalNULLs = true;
	// number of duplicate record to be written to out port
	private int noDupRecord = DEFAULT_NO_DUP_RECORD;
	private boolean sorted = true;
	
	//runtime variables
    int current;
    int previous;
    boolean isFirst;
    InputPort inPort;
    OutputPortDirect outPort;
    OutputPortDirect rejectedPort;
    DataRecord[] records;

    //'last' dedup operation specific variables
    private RingRecordBuffer ringBuffer;
    
    // input metadata
	private DataRecordMetadata metadata;

	// error informations
	private int recordNumber;
	private int fieldNumber;
	
	// record key used to lookup incoming records from cache (hashmap) - only for unsorted input
	private RecordKey recordKey;
	// record key used for HashKey persisted in the cache (hashmap) - only for unsorted input
	private RecordKey recordKeyReduced;
	
	/**
	 *Constructor for the Dedup object
	 *
	 * @param  id         unique id of the component
	 * @param  dedupKeys  definition of key fields used to compare records
	 */
	public Dedup(String id, String[] dedupKeys) {
		super(id);
		this.dedupKeys = dedupKeys;
		
		if (dedupKeys != null) {
			this.dedupOrderings = new OrderEnum[dedupKeys.length];
			Arrays.fill(dedupOrderings, OrderEnum.IGNORE);
			
			Pattern pat = Pattern.compile("^(.*)\\((.*)\\)$");
			
			for (int i = 0; i < dedupKeys.length; i++) {
				Matcher matcher = pat.matcher(dedupKeys[i]);
				if (matcher.find()) {
			    	String keyPart = dedupKeys[i].substring(matcher.start(1), matcher.end(1));
			    	if (matcher.groupCount() > 1) {
			    		String orderShort = (dedupKeys[i].substring(matcher.start(2), matcher.end(2)));
			    		if (orderShort.matches("^[Aa].*")) dedupOrderings[i] = OrderEnum.ASC;
			    		else if (orderShort.matches("^[Dd].*")) dedupOrderings[i] = OrderEnum.DESC;
			    		else if (orderShort.matches("^[Ii].*")) dedupOrderings[i] = OrderEnum.IGNORE;
			    		else dedupOrderings[i] = OrderEnum.AUTO;
			    	}
			    	dedupKeys[i] = keyPart;
				}
			}
		}
	}


	/**
	 *  Gets the change attribute of the Dedup object
	 *
	 * @param  a  Description of the Parameter
	 * @param  b  Description of the Parameter
	 * @return    The change value
	 * @throws TransformException - indicates that input is not sorted as expected.
	 */
	private boolean isChange(DataRecord currentRecord, DataRecord prevRecord) throws TransformException {
		if (orderingComparators == null) {
			return false;
		}
		// compare records
		boolean result = false;
		int iLastEquals = -1;
		for (int i=0; i<orderingComparators.length; i++) {
			// different records
			if (!orderingComparators[i].equals(prevRecord, currentRecord)) {
				// if there is not a change, validate the order
				if (!result) {
					fieldNumber = i;
					orderingComparators[i].validateOrder();
				}
				result = true;
				
			// equals records
			} else if (!result) {
				iLastEquals = i;
			}
		}
		if (bAutoExists && result) resolveAuto(iLastEquals);		
		return result;
	}

	/**
	 * Resolve unknown order comparators.
	 * @param iLastEquals
	 */
	private void resolveAuto(int iLastEquals) {
		// only if auto verification still exits -> try to decide the auto verification 
		DedupComparator orderingComparatorNew;
		bAutoExists = false;
		for (int i=0; i<orderingComparators.length; i++) {
			// only for auto comparator
			if (orderingComparators[i] instanceof DedupAutoComparator) {
				// get asc or desc comparator (or null)
				orderingComparatorNew = ((DedupAutoComparator)orderingComparators[i]).getResolvedComparator();
				
				// replace comparator if it is possible
				if (i <= iLastEquals+1 && orderingComparatorNew != null) {
					orderingComparators[i] = orderingComparatorNew;
				} else {
					bAutoExists = true;
				}
			}
		}
	}
	
	    
    @Override
    public void preExecute() throws ComponentNotReadyException {
    	super.preExecute();
    	if (firstRun()) {//a phase-dependent part of initialization
    		//all necessary elements have been initialized in init()
    	}
    	else {
    		//operation specific reset
    		switch(keep) {
    	    case KEEP_FIRST:
    	        break;
    	    case KEEP_LAST:
    	        resetLast();
    	        break;
    	    case KEEP_UNIQUE:
    	        break;
    	    }
    	}
    }    


    private void executeSorted() throws Exception {
    	records = new DataRecord[2];
        records[0] = DataRecordFactory.newRecord(inPort.getMetadata());
        records[1] = DataRecordFactory.newRecord(inPort.getMetadata());
        isFirst = true; // special treatment for 1st record
        current = 1;
        previous = 0;

        try {
        	switch(keep) {
            case KEEP_FIRST:
                executeFirst();
                break;
            case KEEP_LAST:
                executeLast();
                break;
            case KEEP_UNIQUE:
                executeUnique();
                break;
            }
        	
        } catch(TransformException e) {
        	// set detail error informations
        	e.setRecNo(recordNumber);
        	e.setFieldNo(getErrorFieldNumber());
        	throw e;
        }
    }

	/* (non-Javadoc)
	 * @see org.jetel.graph.Node#execute()
	 */
	@Override
	public Result execute() throws Exception {
		if (sorted || dedupKeys == null) { //use algorithm for sorted input in case no key is specified
			executeSorted();
		} else {
			executeUnsorted();
		}
		return runIt ? Result.FINISHED_OK : Result.ABORTED;
	}

	private void executeUnsorted() throws IOException, InterruptedException {
		recordKey = new RecordKey(dedupKeys, metadata);
		recordKey.setEqualNULLs(equalNULLs);
		
		recordKeyReduced = recordKey.getReducedRecordKey();
		
		switch (keep) {
		case KEEP_FIRST:
			executeUnsortedFirst();
			break;
		case KEEP_LAST:
			executeUnsortedLast();
			break;
		case KEEP_UNIQUE:
			executeUnsortedUnique();
			break;
		default:
			throw new AssertionError(UNKNOWN_KEEP_VALUE);
		}
	}

	private void executeUnsortedFirst() throws IOException, InterruptedException {
		Map<HashKey, int[]> groups = new HashMap<>();
		HashKey hashKey = new HashKey(recordKey, null);
		DataRecord record = DataRecordFactory.newRecord(metadata);
		
		while (inPort.readRecord(record) != null && runIt) {
			hashKey.setDataRecord(record);
			int[] groupSize = groups.get(hashKey);
			if (groupSize == null) {
				groups.put(new HashKey(recordKeyReduced, record.duplicate(recordKey)), new int[] { 1 });
				writeOutRecord(record);
			} else if (groupSize[0] < noDupRecord){
				groupSize[0]++;
				writeOutRecord(record);
			} else {
				writeRejectedRecord(record);
			}
		}
	}

	private static class GroupLast {
		private final CircularFifoBuffer cicularRecordBuffer;
		
		public GroupLast(int noDupRecord, DataRecord record) {
			cicularRecordBuffer = new CircularFifoBuffer(noDupRecord);
			cicularRecordBuffer.add(record);
		}

		public DataRecord writeRecord(DataRecord record) {
			if (cicularRecordBuffer.isFull()) {
				final DataRecord overflowedRecord = (DataRecord) cicularRecordBuffer.remove();
				cicularRecordBuffer.add(record);
				return overflowedRecord;
			} else {
				cicularRecordBuffer.add(record);
				return null;
			}
		}
		
		public DataRecord readRecord() {
			if (!cicularRecordBuffer.isEmpty()) {
				return (DataRecord) cicularRecordBuffer.remove();
			} else {
				return null;
			}
		}
	}
	
	private void executeUnsortedLast() throws IOException, InterruptedException {
		Map<HashKey, GroupLast> groups = new LinkedHashMap<>(16, 0.75f, true); //access order enabled
		DataRecord record = DataRecordFactory.newRecord(metadata);
		HashKey hashKey = new HashKey(recordKey, record);
		DataRecord outputRecord;
		
		while (inPort.readRecord(record) != null && runIt) {
			GroupLast group = groups.get(hashKey);
			DataRecord duplicate = record.duplicate();
			if (group == null) {
				groups.put(new HashKey(recordKeyReduced, record.duplicate(recordKey)), new GroupLast(noDupRecord, duplicate));
			} else {
				if ((outputRecord = group.writeRecord(duplicate)) != null) {
					writeRejectedRecord(outputRecord);
				}
			}
		}
		//pour out the cached groups
		for (GroupLast group : groups.values()) {
			while ((outputRecord = group.readRecord()) != null) {
				writeOutRecord(outputRecord);
			}
		}
	}
	
	private static class GroupUnique {
		private DataRecord record;
		private boolean read = false;
		
		public GroupUnique(DataRecord record) {
			this.record = record;
		}

		public DataRecord readRecord() {
			read = true;
			return record;
		}
		
		public boolean isRead() {
			return read;
		}
	}
	
	private void executeUnsortedUnique() throws IOException, InterruptedException {
		Map<HashKey, GroupUnique> groups = new LinkedHashMap<>();
		HashKey hashKey = new HashKey(recordKey, null);
		DataRecord record = DataRecordFactory.newRecord(metadata);
		
		while (inPort.readRecord(record) != null && runIt) {
			hashKey.setDataRecord(record);
			GroupUnique group = groups.get(hashKey);
			if (group == null) {
				DataRecord duplicate = record.duplicate();
				groups.put(new HashKey(recordKey, duplicate), new GroupUnique(duplicate));
			} else {
				if (!group.isRead()) {
					writeRejectedRecord(group.readRecord());
				}
				writeRejectedRecord(record);
			}
		}

		//pour out the cached groups
		for (GroupUnique group : groups.values()) {
			if (!group.isRead()) {
				writeOutRecord(group.readRecord());
			}
		}
	}

	/**
     * Execution a de-duplication with first function.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws TransformException 
     */
	private void executeFirst() throws IOException, InterruptedException, TransformException {
		int groupItems = 0;

		while (runIt && inPort.readRecord(records[current]) != null) {
			if (isFirst) {
				writeOutRecord(records[current]);
				groupItems++;
				isFirst = false;
			} else {
				if (isChange(records[current], records[previous])) {
					writeOutRecord(records[current]);
					groupItems = 1;
				} else {
					if (groupItems < noDupRecord) {
						writeOutRecord(records[current]);
						groupItems++;
					} else {
						writeRejectedRecord(records[current]);
					}
				}
			}

			// swap indexes
			current = current ^ 1;
          	previous = previous ^ 1;
          	recordNumber++;
		}
    }
	
	/**
	 * Specific initialization for the 'last' dedup operation.
	 * @throws ComponentNotReadyException
	 */
	private void initLast() throws ComponentNotReadyException {
    	ringBuffer = new RingRecordBuffer(noDupRecord);
		ringBuffer.init();
	}
	
    /**
     * Execution a de-duplication with last function.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws TransformException 
     */
	private void executeLast() throws IOException, InterruptedException, TransformException {
    	while (runIt && inPort.readRecord(records[current]) != null) {
            if (isFirst) {
                isFirst = false;
            } else if (isChange(records[current], records[previous])) {
            	while (ringBuffer.popRecord(records[previous]) != null) {
            	    writeOutRecord(records[previous]);
            	}

            	ringBuffer.reset();
            }

            if (ringBuffer.isFull()) {
                writeRejectedRecord(ringBuffer.popRecord(records[previous]));
            }

            ringBuffer.pushRecord(records[current]);

            // swap indexes
            current = current ^ 1;
            previous = previous ^ 1;
          	recordNumber++;
        }

    	while (ringBuffer.popRecord(records[previous]) != null) {
    	    writeOutRecord(records[previous]);
    	}

    	ringBuffer.reset();
    }

	/**
	 * Specific reset method for the 'last' dedup operation.
	 * @throws ComponentNotReadyException
	 */
	private void resetLast() throws ComponentNotReadyException {
		ringBuffer.reset();
	}

	/**
	 * Specific release method for the 'last' dedup operation.
	 */
	private void freeLast() {
		try {
			if (ringBuffer != null) {
				ringBuffer.free();
			}
		} catch (IOException e) {
			logger.warn(e);
		}
	}
	
    /**
     * Execution a de-duplication with unique function.
     * 
     * @throws IOException
     * @throws InterruptedException
     * @throws TransformException 
     */
	private void executeUnique() throws IOException, InterruptedException, TransformException {
        int groupItems = 0;

        while (records[current] != null && runIt) {
            records[current] = inPort.readRecord(records[current]);
            if (records[current] != null) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    if (isChange(records[current], records[previous])) {
                        if (groupItems == 1) {
                            writeOutRecord(records[previous]);
                        } else {
                            writeRejectedRecord(records[previous]);
                        }
                        groupItems = 0;
                    } else {
                        writeRejectedRecord(records[previous]);
                    }
                }
                groupItems++;
                // swap indexes
                current = current ^ 1;
                previous = previous ^ 1;
            } else {
                if (!isFirst) {
                    if(groupItems == 1) {
                        writeOutRecord(records[previous]);
                    } else {
                        writeRejectedRecord(records[previous]);
                    }
                }
            }
          	recordNumber++;
        }
    }
    
    /**
     * Tries to write given record to the rejected port, if is connected.
     */
    private void writeRejectedRecord(DataRecord record) throws IOException, InterruptedException {
        if(rejectedPort != null) {
            rejectedPort.writeRecord(record);
        }
    }

    /**
     * Writes given record to the out port.
     */
    private void writeOutRecord(DataRecord record) throws IOException, InterruptedException {
        outPort.writeRecord(record);
    }

	/**
	 * Description of the Method
	 * 
	 * @exception ComponentNotReadyException
	 *                Description of the Exception
	 * @since April 4, 2002
	 */
	@Override
	public void init() throws ComponentNotReadyException {
        if (isInitialized()) return;
		super.init();

		inPort = getInputPort(READ_FROM_PORT);
		if (inPort == null) {
			throw new ComponentNotReadyException(this, 
					"Input port (number " + READ_FROM_PORT + ") must be defined.");
		}
		
		outPort = getOutputPortDirect(WRITE_TO_PORT);
		if (outPort == null) {
			throw new ComponentNotReadyException(this, 
					"Output port (number " + WRITE_TO_PORT + ") must be defined.");
		}
		
		rejectedPort = getOutputPortDirect(REJECTED_PORT);
		
        if (dedupKeys != null) {
			metadata = getInputPort(READ_FROM_PORT).getMetadata();
        	createOrderingComparators();
        }
        
        if (noDupRecord < 1) {
        	throw new ComponentNotReadyException(this, XML_NO_DUP_RECORD_ATTRIBUTE,
        			StringUtils.quote(XML_NO_DUP_RECORD_ATTRIBUTE) 
        			+ " must be positive number.");
        }

        //operation specific initialization 
    	switch(keep) {
        case KEEP_FIRST:
            break;
        case KEEP_LAST:
        	initLast();
            break;
        case KEEP_UNIQUE:
            break;
        }
	}

	/**
	 * Creates a record key.
	 * @param orderEnum
	 */
	private void createOrderingComparators() {
		List<DedupComparator> lOrderingComparators = new ArrayList<DedupComparator>(); 
		for (int i=0; i<dedupOrderings.length; i++) {
			RecordKey recordKey = new RecordKey(new String[] {dedupKeys[i]}, metadata);
			if (dedupOrderings[i] == OrderEnum.ASC) {
				lOrderingComparators.add(new DedupAscComparator(recordKey));
			} else if (dedupOrderings[i] == OrderEnum.DESC) {
				lOrderingComparators.add(new DedupDescComparator(recordKey));
			} else if (dedupOrderings[i] == OrderEnum.IGNORE) {
				lOrderingComparators.add(new DedupIgnoreComparator(recordKey));
			} else if (dedupOrderings[i] == OrderEnum.AUTO) {
				lOrderingComparators.add(new DedupAutoComparator(recordKey));
				bAutoExists = true;
			}  

		    // for DEDUP component, specify whether two fields with NULL
		    // value indicator set are considered equal
		    recordKey.setEqualNULLs(equalNULLs);
		}
		orderingComparators = new DedupComparator[lOrderingComparators.size()];
		lOrderingComparators.toArray(orderingComparators);
	}
	
	@Override
	public synchronized void reset() throws ComponentNotReadyException {
		super.reset();
	}

	@Override
	public synchronized void free() {
		super.free();
		
		//operation specific free
		switch(keep) {
        case KEEP_FIRST:
            break;
        case KEEP_LAST:
            freeLast();
            break;
        case KEEP_UNIQUE:
            break;
        }
	}
	
	/**
	 *  Description of the Method
	 *
	 * @param  nodeXML  Description of Parameter
	 * @return          Description of the Returned Value
	 * @throws AttributeNotFoundException 
	 * @since           May 21, 2002
	 */
     public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException, AttributeNotFoundException {
		ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
		Dedup dedup;
        String dedupKey = xattribs.getString(XML_DEDUPKEY_ATTRIBUTE, null);
        
		dedup = new Dedup(xattribs.getString(XML_ID_ATTRIBUTE),
				dedupKey != null ? dedupKey.split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX) : null);
		if (xattribs.exists(XML_KEEP_ATTRIBUTE)){
		    dedup.setKeep(xattribs.getString(XML_KEEP_ATTRIBUTE).matches("^[Ff].*") ? Keep.KEEP_FIRST :
			    xattribs.getString(XML_KEEP_ATTRIBUTE).matches("^[Ll].*") ? Keep.KEEP_LAST : Keep.KEEP_UNIQUE);
		}
		if (xattribs.exists(XML_EQUAL_NULL_ATTRIBUTE)){
		    dedup.setEqualNULLs(xattribs.getBoolean(XML_EQUAL_NULL_ATTRIBUTE));
		}
		if (xattribs.exists(XML_NO_DUP_RECORD_ATTRIBUTE)){
		    dedup.setNumberRecord(xattribs.getInteger(XML_NO_DUP_RECORD_ATTRIBUTE));
		}
		if (xattribs.exists(XML_SORTED_ATTRIBUTE)){
		    dedup.setSorted(xattribs.getBoolean(XML_SORTED_ATTRIBUTE));
		}
		return dedup;
	}


	/**  Description of the Method */
     @Override
     public ConfigurationStatus checkConfig(ConfigurationStatus status) {
         super.checkConfig(status);
         
         if(!checkInputPorts(status, 1, 1) 
        		 || !checkOutputPorts(status, 1, 2)) {
        	 return status;
         }
         
         checkMetadata(status, getInPorts(), getOutPorts());

         try {
             init();
         } catch (ComponentNotReadyException e) {
             ConfigurationProblem problem = new ConfigurationProblem(ExceptionUtils.getMessage(e), ConfigurationStatus.Severity.ERROR, this, ConfigurationStatus.Priority.NORMAL);
             if(!StringUtils.isEmpty(e.getAttributeName())) {
                 problem.setAttributeName(e.getAttributeName());
             }
             status.add(problem);
         } finally {
         	free();
         }
         
         return status;
     }
	
	public void setKeep(Keep keep) {
		this.keep = keep;
	}
	
	public void setEqualNULLs(boolean equal){
	    this.equalNULLs = equal;
	}

	public void setNumberRecord(int numberRecord) {
		this.noDupRecord = numberRecord;
	}
	
	public boolean isSorted() {
		return sorted;
	}

	public void setSorted(boolean sorted) {
		this.sorted = sorted;
	}
	
	/**
	 * Abstract dedup comparator.
	 */
	private abstract class DedupComparator {
		// record key
		protected RecordKey recordKey;
		
		// equals result state
		protected boolean bEquals;
		
		// compare result status
		protected int cmpResult;

		/**
		 * Constructor.
		 * @param recordKey
		 */
		public DedupComparator(RecordKey recordKey) {
			this.recordKey = recordKey;
		}

		/**
		 * Gets record key.
		 * @return
		 */
		public RecordKey getRecordKey() {
			return recordKey;
		}
		
		/** 
		 * Gets an error message.
		 */
		protected String getErrorMessage() {
			return "Input is not sorted as specified by component attributes";
		}

		/**
		 * Equals records.
		 * @param prevRecord
		 * @param currentRecord
		 * @return
		 */
		public abstract boolean equals(DataRecord prevRecord, DataRecord currentRecord);
		
		/**
		 * Validate record order. Use after equals processing. 
		 * @throws TransformException
		 */
		public abstract void validateOrder() throws TransformException;
	}
	
	/**
	 * Dedup asc. comparator.
	 */
	private class DedupAscComparator extends DedupComparator {
		public DedupAscComparator(RecordKey recordKey) {
			super(recordKey);
		}

		@Override
		public boolean equals(DataRecord prevRecord, DataRecord currentRecord) {
			bEquals = (cmpResult = recordKey.compare(prevRecord, currentRecord)) == 0;
			return bEquals;
		}

		@Override
		public void validateOrder() throws TransformException {
			if (cmpResult == 1) throw new TransformException(getErrorMessage());
		}
	}

	/**
	 * Dedup desc. comparator.
	 */
	private class DedupDescComparator extends DedupComparator {
		public DedupDescComparator(RecordKey recordKey) {
			super(recordKey);
		}

		@Override
		public boolean equals(DataRecord prevRecord, DataRecord currentRecord) {
			bEquals = (cmpResult = recordKey.compare(prevRecord, currentRecord)) == 0;
			return bEquals;
		}

		@Override
		public void validateOrder() throws TransformException {
			if (cmpResult == -1 && !recordKey.isComparedNulls()) throw new TransformException(getErrorMessage());
		}
	}

	/**
	 * Dedup ignore comparator.
	 */
	private class DedupIgnoreComparator extends DedupComparator {
		public DedupIgnoreComparator(RecordKey recordKey) {
			super(recordKey);
		}

		@Override
		public boolean equals(DataRecord prevRecord, DataRecord currentRecord) {
			return recordKey.compare(prevRecord, currentRecord) == 0;
		}

		@Override
		public void validateOrder() throws TransformException {
		}
	}

	/**
	 * Dedup auto. comparator.
	 */
	private class DedupAutoComparator extends DedupComparator {
		public DedupAutoComparator(RecordKey recordKey) {
			super(recordKey);
		}

		@Override
		public boolean equals(DataRecord prevRecord, DataRecord currentRecord) {
			return (cmpResult = recordKey.compare(prevRecord, currentRecord)) == 0;
		}
		
		@Override
		public void validateOrder() throws TransformException {
		}
		
		/**
		 * Gets resolved comparator - ascending or descending or null.
		 * @return
		 */
		public DedupComparator getResolvedComparator() {
			if (cmpResult == -1) return new DedupAscComparator(recordKey);
			else if (cmpResult == 1) return new DedupDescComparator(recordKey);
			return null;
		}
	}

	/**
	 * Returns error field number.
	 * @return
	 */
	private int getErrorFieldNumber() {
		if (orderingComparators == null || orderingComparators.length <= fieldNumber) return -1;
		RecordKey recordKey = orderingComparators[fieldNumber].getRecordKey();
		if (recordKey == null) return -1;
		int[] numbers;
		try {
			numbers = metadata.fieldsIndices(recordKey.getKeyFieldNames());
		} catch(RuntimeException e) {
			return -1;
		}
		return numbers.length == 0 ? -1 : numbers[0];
	}
	


}

