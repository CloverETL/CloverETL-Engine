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
package org.jetel.component;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.RecordKey;
import org.jetel.data.SortDataRecordInternal;
import org.jetel.data.tape.DataRecordTape;
import org.jetel.data.tape.TapeCarousel;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.InputPort;
import org.jetel.graph.Node;
import org.jetel.graph.TransformationGraph;
import org.jetel.util.ComponentXMLAttributes;
import org.jetel.util.StringUtils;
import org.jetel.util.SynchronizeUtils;
import org.w3c.dom.Element;
/**
 *  <h3>Sort Component</h3>
 *
 * <!-- Sorts the incoming records based on specified key -->
 *
 * <table border="1">
 *  <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Sort</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td></td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Sorts the incoming records based on specified key.<br>
 *  The key is name (or combination of names) of field(s) from input record.
 *  The sort order is either Ascending (default) or Descending.<br>
 * In case there is not enough room in internal sort buffer, it performs
 * external sorting - thus any number of internal records can be sorted.</td></tr>
 * <tr><td><h4><i>Inputs:</i></h4></td>
 * <td>[0]- input records</td></tr>
 * <tr><td><h4><i>Outputs:</i></h4></td>
 * <td>At least one connected output port.</td></tr>
 * <tr><td><h4><i>Comment:</i></h4></td>
 * <td></td></tr>
 * </table>
 *  <br>
 *  <table border="1">
 *  <th>XML attributes:</th>
 *  <tr><td><b>type</b></td><td>"EXT_SORT"</td></tr>
 *  <tr><td><b>id</b></td><td>component identification</td>
 *  <tr><td><b>sortKey</b></td><td>field names separated by :;|  {colon, semicolon, pipe}</td>
 *  <tr><td><b>sortOrder</b><br><i>optional</i></td><td>one of "Ascending|Descending" {the fist letter is sufficient, if not defined, then Ascending}</td>
 *  <tr><td><b>numberOfTapes</b><br><i>optional</i></td><td>even number greater than 2 - denotes how many tapes (temporary files) will be used when external sorting data.
 *  <i>Default is 6 tapes.</i></td>
 *  <tr><td><b>sorterInitialCapacity</b><br><i>optional</i></td><td>the initial capacity of internal sorter used for in-memory sorting records. If the
 *   system has plenty of memory, specify high number here (5000 or more). If the system is short on memory, use low number (100).<br>
 *   The final capacity is based on following formula:<br><code>sorter_initial_capacity * (1 - grow_factor^max_num_collections)/(1 - grow_factor)</code><br>
 *   where:<br><code>grow_factor=1.6<br>max_num_collections=8<br>sorterInitialCapacity=2000<br></code><br>With the parameters above, the default total capacity roughly is <b>140000</b> records. The
 *   total capacity is approximately <code>69,91 * sorterInitialCapacity</code>.<br><br>
 *   Following tables shows Total Capacities of internal buffer for various Initial Capacity values:
 *   <table border="1">
 *   <tr><th>Initial Capacity</th><th>Total Capacity</th></tr>
 *    <tr><td>10</td><td>1000</td></tr>
 *    <tr><td>100</td><td>7000</td></tr>
 *    <tr><td>1000</td><td>70000</td></tr>
 *    <tr><td>2000</td><td>140000</td></tr>
 *    <tr><td>5000</td><td>350000</td></tr>
 *    <tr><td>10000</td><td>700000</td></tr>
 *    <tr><td>20000</td><td>1399000</td></tr>
 *    <tr><td>50000</td><td>3496000</td></tr>
 *    </table>
 *  </tr>
 *  <tr><td><b>bufferCapacity</b><br><i>optional</i></td><td>What is the maximum number of records
 *  which are sorted in-memory. If number of records exceed this size, external sorting is performed.</td></tr>
 *  <tr><td><b>tmpDirs</b><br><i>optional</i></td><td>Semicolon (;) delimited list of directories which should be
 *  used for creating tape files - used when external sorting is performed. Default value is equal to Java's <code>java.io.tmpdir</code> system property.</td></tr>
 *  </table>
 *
 *  <h4>Example:</h4>
 *  <pre>&lt;Node id="SORT_CUSTOMER" type="EXT_SORT" sortKey="Name:Address" sortOrder="A"/&gt;</pre>
 *
 * @author      dpavlis
 * @since       April 4, 2002
 * @revision    $Revision$
 */
public class ExtSort extends Node {

	private static final String XML_NUMBEROFTAPES_ATTRIBUTE = "numberOfTapes";
	private static final String XML_SORTERINITIALCAPACITY_ATTRIBUTE = "sorterInitialCapacity";
	private static final String XML_SORTORDER_ATTRIBUTE = "sortOrder";
	private static final String XML_SORTKEY_ATTRIBUTE = "sortKey";
    private static final String XML_BUFFER_CAPACITY_ATTRIBUTE = "bufferCapacity";
    private static final String XML_TEMPORARY_DIRS = "tmpDirs";
    
	/**  Description of the Field */
	public final static String COMPONENT_TYPE = "EXT_SORT";

	private final static int WRITE_TO_PORT = 0;
	private final static int READ_FROM_PORT = 0;

	private SortDataRecordInternal sorter;
	private TapeCarousel tapeCarousel;
	private boolean sortOrderAscending;
	private String[] sortKeysNames;
	private ByteBuffer recordBuffer;
	private boolean carouselInitialized;
    private String[] tmpDirs;
	
	private InputPort inPort;
	private DataRecord inRecord;
	
	private RecordKey sortKey;
	
	private int numberOfTapes;
	private int internalSorterCapacity;

	private final static boolean DEFAULT_ASCENDING_SORT_ORDER = true; 
	private final static int DEFAULT_NUMBER_OF_TAPES = 6;
	
	static Log logger = LogFactory.getLog(ExtSort.class);

	/**
     * Constructor for the Sort object
     * 
     * @param id
     *            Description of the Parameter
     * @param sortKeysNames
     *            Description of the Parameter
     * @param sortOrder
     *            Description of the Parameter
     */
    public ExtSort(String id, String[] sortKeys, boolean sortOrder) {
        super(id);
        this.sortOrderAscending = sortOrder;
        this.sortKeysNames = sortKeys;
        carouselInitialized = false;
        numberOfTapes=DEFAULT_NUMBER_OF_TAPES;
        internalSorterCapacity=-1;
    }

    /**
     * Constructor for the Sort object
     * 
     * @param id
     *            Description of the Parameter
     * @param sortKeysNames
     *            Description of the Parameter
     */
    public ExtSort(String id, String[] sortKeys) {
        this(id, sortKeys, DEFAULT_ASCENDING_SORT_ORDER);
    }

    /**
     * Main processing method for the SimpleCopy object
     * 
     * @since April 4, 2002
     */
    public void run() {
        boolean doMerge = false;
        inPort = getInputPort(READ_FROM_PORT);
        inRecord = new DataRecord(inPort.getMetadata());
        inRecord.init();
        DataRecord tmpRecord = inRecord;
        /*
         * PHASE SORT --
         * 
         * we read records from input till internal buffer is full, then we sort
         * internal buffer and write it to tape. Then we continue till EOF and
         * MERGING occures afterwards.
         * 
         * If we reach EOF on input before internal buffer is full, we just sort
         * them and send directly to output
         */
        // --- store input records into internal buffer
        while (tmpRecord != null && runIt) {
            try {
                tmpRecord = inPort.readRecord(inRecord);
                if (tmpRecord != null) {
                    if (!sorter.put(inRecord)) {
                        // we need to sort & flush buffer on to tape and merge it later
                        doMerge = true;
                        sorter.sort();
                        flushToTape();
                        sorter.reset();
                        if (!sorter.put(inRecord)) {
                            throw new RuntimeException(
                                    "Can't store record into sorter !");
                        }
                        tmpRecord = inRecord;
                    }
                }
            } catch (IOException ex) {
                resultMsg = ex.getMessage();
                resultCode = Node.RESULT_ERROR;
                closeAllOutputPorts();
                return;
            } catch (Exception ex) {
                resultMsg = ex.getClass().getName() + " : " + ex.getMessage();
                resultCode = Node.RESULT_FATAL_ERROR;
                //closeAllOutputPorts();
                return;
            }
            SynchronizeUtils.cloverYield();
        }
        /*
         * PHASE MERGE ------------
         */
        if (runIt) {
            if (doMerge) {
                // sort whatever remains in sorter
                sorter.sort();
                try {
                    flushToTape();
                    // we don't need sorter any more - free all its resources
                    sorter.free();
                    // flush to disk whatever remains in tapes' buffers
                    // tapeCarousel.flush();
                    // merge partially sorted data from tapes
                    phaseMerge();
                } catch (IOException ex) {
                    resultMsg = ex.getMessage();
                    resultCode = Node.RESULT_ERROR;
                    closeAllOutputPorts();
                    return;
                } catch (Exception ex) {
                    resultMsg = ex.getClass().getName() + " : "
                            + ex.getMessage();
                    resultCode = Node.RESULT_FATAL_ERROR;
                    //closeAllOutputPorts();
                    return;
                }
            }
            /*
             * SEND RECORDS FROM SORTER DIRECTLY --------
             */
            else {
                sorter.sort();
                sorter.rewind();
                recordBuffer.clear();
                // --- read sorted records
                while (sorter.get(recordBuffer) && runIt) {
                    try {
                        writeRecordBroadcastDirect(recordBuffer);
                        recordBuffer.clear();
                    } catch (IOException ex) {
                        resultMsg = ex.getMessage();
                        resultCode = Node.RESULT_ERROR;
                        closeAllOutputPorts();
                        return;
                    } catch (Exception ex) {
                        resultMsg = ex.getClass().getName() + " : "
                                + ex.getMessage();
                        resultCode = Node.RESULT_FATAL_ERROR;
                        //closeAllOutputPorts();
                        return;
                    }
                    SynchronizeUtils.cloverYield();
                }
                sorter.free();
            }
        }

        broadcastEOF();
        if (runIt) {
            resultMsg = "OK";
        } else {
            resultMsg = "STOPPED";
        }
        resultCode = Node.RESULT_OK;
    }


    /**
     * Description of the Method
     * 
     * @exception ComponentNotReadyException
     *                Description of the Exception
     * @since April 4, 2002
     */
    public void init() throws ComponentNotReadyException {
        // test that we have at least one input port and one output
        if (inPorts.size() < 1) {
            throw new ComponentNotReadyException(
                    "At least one input port has to be defined!");
        } else if (outPorts.size() < 1) {
            throw new ComponentNotReadyException(
                    "At least one output port has to be defined!");
        }
        recordBuffer = ByteBuffer
                .allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
        if (recordBuffer == null) {
            throw new ComponentNotReadyException(
                    "Can NOT allocate internal record buffer ! Required size:"
                            + Defaults.Record.MAX_RECORD_SIZE);
        }
        // create sorter
        if (internalSorterCapacity>0){
            sorter = new SortDataRecordInternal(getInputPort(READ_FROM_PORT)
                    .getMetadata(), sortKeysNames, sortOrderAscending,internalSorterCapacity);
            
        }else{
        sorter = new SortDataRecordInternal(getInputPort(READ_FROM_PORT)
                .getMetadata(), sortKeysNames, sortOrderAscending);
        }
        
    }

    /**
     * Performs merge of partially sorted data records stored on tapes 
     * (in external files).
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    private void phaseMerge() throws IOException, InterruptedException {
        int index;
        DataRecordTape targetTape;
        TapeCarousel targetCarousel = new TapeCarousel(tapeCarousel.numTapes());
        DataRecord[] sourceRecords = new DataRecord[tapeCarousel.numTapes()];
        boolean[] sourceRecordsFlags = new boolean[tapeCarousel.numTapes()];

        // initialize sort key which will be used when merging data
        sortKey = new RecordKey(sortKeysNames, inRecord.getMetadata());
        sortKey.init();

        // initial creation & initialization of source records
        for (int i = 0; i < sourceRecords.length; i++) {
            sourceRecords[i] = new DataRecord(inRecord.getMetadata());
            sourceRecords[i].init();
        }

        // rewind carousel with source data - so we can start reading it
        tapeCarousel.rewind();
        // open carousel into which we will merge data
        targetCarousel.open();
        // get first free tape from target carousel
        targetTape = targetCarousel.getFirstTape();

        /* 
         * MAIN MERGING loop
         */
        do {
            // if we need to perform only final merging (one lewel of chunks on source tapes)
            // skip to final merge
            if (tapeCarousel.getFirstTape().getNumChunks()==1) break;
            /*
             * semi-merging of one level of data chunks
             */
            do {
                loadUpRecords(tapeCarousel, sourceRecords, sourceRecordsFlags);
                if (hasAnyData(sourceRecordsFlags)) {
                    targetTape.addDataChunk();
                } else {
                    break;
                }
                while (hasAnyData(sourceRecordsFlags)) {
                    if (sortOrderAscending) {
                        index = getLowestIndex(sourceRecords,
                                sourceRecordsFlags);
                    } else {
                        index = getHighestIndex(sourceRecords,
                                sourceRecordsFlags);
                    }
                    // write record to target tape
                    recordBuffer.clear();
                    sourceRecords[index].serialize(recordBuffer);
                    recordBuffer.flip();
                    targetTape.put(recordBuffer);
                    // read in next record from tape from which we read last
                    // record
                    if (!tapeCarousel.getTape(index).get(sourceRecords[index])) {
                        sourceRecordsFlags[index] = false;
                    }
                    SynchronizeUtils.cloverYield();
                }
                targetTape.flush(false);
                targetTape = targetCarousel.getNextTape();
                if (targetTape == null)
                    targetTape = targetCarousel.getFirstTape();
            } while (hasMoreChunks(tapeCarousel));
            // switch source tapes and target tapes, then continue with merging
            targetCarousel.rewind();
            tapeCarousel.clear();
            TapeCarousel tmp = tapeCarousel;
            tapeCarousel = targetCarousel;
            targetCarousel = tmp;
            targetTape = targetCarousel.getFirstTape();

        } while (tapeCarousel.getFirstTape().getNumChunks() > 1);

        // we don't need target carousel - merged records will be sent to output port
        targetCarousel.free();

        // DEBUG START
//        if (logger.isDebugEnabled()) {
//		    logger.debug("*** Merged data: ***");
//		    logger.debug("****** FINAL TAPE CAROUSEL REVIEW ***********");
//		
//		    DataRecordTape tape = tapeCarousel.getFirstTape();
//		    while (tape != null) {
//		    	logger.debug(tape);
//		        tape = tapeCarousel.getNextTape();
//		    }
//        }
        // DEBUG END
        
        /* 
         * send data to output - final merge
         */
        tapeCarousel.rewind();
        loadUpRecords(tapeCarousel, sourceRecords, sourceRecordsFlags);
        while (hasAnyData(sourceRecordsFlags)) {
            if (sortOrderAscending) {
                index = getLowestIndex(sourceRecords, sourceRecordsFlags);
            } else {
                index = getHighestIndex(sourceRecords, sourceRecordsFlags);
            }
            // write record to out port
            writeRecordBroadcast(sourceRecords[index]);
            if (!tapeCarousel.getTape(index).get(sourceRecords[index])) {
                sourceRecordsFlags[index] = false;
            }
            SynchronizeUtils.cloverYield();
        }
        // end-of-story
        tapeCarousel.free();
    }

    /**
     * Populates source records array with records from individual tapes (included in
     * tape carousel). Sets flags in flags array for those records which contain valid data.
     * 
     * @param tapeCarousel
     * @param sourceRecords
     * @param sourceRecordsFlags
     * @throws IOException
     */
    private final void loadUpRecords(TapeCarousel tapeCarousel,
            DataRecord[] sourceRecords, boolean[] sourceRecordsFlags)
            throws IOException {
        for (int i = 0; i < tapeCarousel.numTapes(); i++) {
            DataRecordTape tape = tapeCarousel.getTape(i);
            if (tape.get(sourceRecords[i])) {
                sourceRecordsFlags[i] = true;
            } else {
                sourceRecordsFlags[i] = false;
            }
        }
    }

    /**
     * Checks whether tapes within tape carousel contains more data chunks
     * to be processed.
     * @param tapeCarousel
     * @return true if more chunks are available
     */
    private final static boolean hasMoreChunks(TapeCarousel tapeCarousel) {
        boolean hasMore = false;
        for (int i = 0; i < tapeCarousel.numTapes(); i++) {
            if (tapeCarousel.getTape(i).nextDataChunk()) {
                hasMore = true;
            }
        }
        return hasMore;
    }

    /**
     * Returns index of the lowest record from the specified record array
     * 
     * @param sourceRecords array of source records
     * @param flags array indicating which source records contain valid data
     * @return index of the lowest record within source records array of -1 if no such record
     * exists - i.e. there is no valid record
     */
    private final int getLowestIndex(DataRecord[] sourceRecords, boolean[] flags) {
        int lowest = -1;
        for (int i = 0; i < flags.length; i++) {
            if (flags[i]) {
                lowest = i;
                break;
            }
        }
        for (int i = lowest + 1; i < sourceRecords.length; i++) {
            if (flags[i]
                    && sortKey.compare(sourceRecords[lowest], sourceRecords[i]) == 1) {
                lowest = i;
            }
        }
        return lowest;
    }

    /**
     * Returns index of the highest record from the specified record array
     * 
     * @param sourceRecords array of source records
     * @param flags array indicating which source records contain valid data
     * @return index of the highest record within source records array of -1 if no such record
     * exists - i.e. there is no valid record
     */
    private final int getHighestIndex(DataRecord[] sourceRecords,
            boolean[] flags) {
        int highest = 1;
        for (int i = 0; i < flags.length; i++) {
            if (flags[i]) {
                highest = i;
                break;
            }
        }
        for (int i = highest + 1; i < sourceRecords.length; i++) {
            if (flags[i]
                    && sortKey
                            .compare(sourceRecords[highest], sourceRecords[i]) == -1) {
                highest = i;
            }
        }
        return highest;
    }

    /**
     * Checks that at least one valid record exists
     * @param flags
     * @return true if flahs indicate that at least one valid record exists
     */
    private final static boolean hasAnyData(boolean[] flags) {
        for (int i = 0; i < flags.length; i++) {
            if (flags[i] == true)
                return true;
        }
        return false;
    }

    private void flushToTape() throws IOException {
        DataRecordTape tape;
        if (!carouselInitialized) {
            tapeCarousel = new TapeCarousel(numberOfTapes);
            tapeCarousel.open();
            tape = tapeCarousel.getFirstTape();
            carouselInitialized = true;
        } else {
            tape = tapeCarousel.getNextTape();
            if (tape == null)
                tape = tapeCarousel.getFirstTape();
        }
        tape.addDataChunk();

        sorter.rewind();
        recordBuffer.clear();
        // --- read sorted records
        while (sorter.get(recordBuffer) && runIt) {
            tape.put(recordBuffer);
            recordBuffer.clear();
        }
        tape.flush(false);
    }

    /**
     * Sets the sortOrderAscending attribute of the Sort object
     * 
     * @param ascending
     *            The new sortOrderAscending value
     */
    public void setSortOrderAscending(boolean ascending) {
        sortOrderAscending = ascending;
    }
    
    /**
     * How many tapes will be used for merging
     * @param numberOfTapes The numberOfTapes to set.
     */
    public void setNumberOfTapes(int numberOfTapes) {
        if (numberOfTapes>2 && (numberOfTapes%2==0)){
            this.numberOfTapes = numberOfTapes;
        }
    }
    
    /**
     * What is the initial capacity (in num of records) internal
     * sorter (SortDataRecordInternal)  will start with.
     * 
     * @param internalSorterCapacity The internalSorterCapacity to set.
     * @see org.jetel.data.SortDataRecordInternal
     */
    public void setInternalSorterInitialCapacity(int internalSorterCapacity) {
        if (internalSorterCapacity>10){
            this.internalSorterCapacity = internalSorterCapacity;
        }
    }
    
    /**
     * What is the maximum capacity of internal buffer used for
     * in-memory sorting.
     * 
     * @param size maximum buffer capacity
     */
    public void setBufferCapacity(int size){
        setInternalSorterInitialCapacity((int)(((double)size)/69.91d));
        
    }
    
    /**
     *  Description of the Method
     *
     * @return    Description of the Returned Value
     * @since     May 21, 2002
     */
    public void toXML(org.w3c.dom.Element xmlElement) {
       super.toXML(xmlElement);
       
       // sortKey attribute
       String sortKeys = this.sortKeysNames[0];
       for (int i=1; i < this.sortKeysNames.length; i++) {
       		sortKeys += Defaults.Component.KEY_FIELDS_DELIMITER + sortKeysNames[i];
       }
       xmlElement.setAttribute(XML_SORTKEY_ATTRIBUTE, sortKeys);
       
       // sortOrder attribute
       if (this.sortOrderAscending == false) {
       		xmlElement.setAttribute(XML_SORTORDER_ATTRIBUTE,"Descending");
       }else{
    	   xmlElement.setAttribute(XML_SORTORDER_ATTRIBUTE,"Ascending");
       }
       
       // numberOfTapes attribute
       if (this.numberOfTapes != 2) {
       		xmlElement.setAttribute(XML_NUMBEROFTAPES_ATTRIBUTE,String.valueOf(this.numberOfTapes));
       }
       
       // sorterInitialCapacity
       if (this.internalSorterCapacity > 10) {
       		xmlElement.setAttribute(XML_SORTERINITIALCAPACITY_ATTRIBUTE, 
       				String.valueOf(this.internalSorterCapacity));
       }
       
       if (this.tmpDirs!=null){
           xmlElement.setAttribute(XML_TEMPORARY_DIRS,StringUtils.stringArraytoString(tmpDirs,';') );
       }
       
       
    }

    /**
     *  Description of the Method
     *
     * @param  nodeXML  Description of Parameter
     * @return          Description of the Returned Value
     * @since           May 21, 2002
     */
    public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
        ComponentXMLAttributes xattribs = new ComponentXMLAttributes(xmlElement, graph);
        ExtSort sort;
        try {
            sort = new ExtSort(xattribs.getString(XML_ID_ATTRIBUTE), xattribs.getString(
                    XML_SORTKEY_ATTRIBUTE).split(
                    Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
            if (xattribs.exists(XML_SORTORDER_ATTRIBUTE)) {
                sort.setSortOrderAscending(xattribs.getString(XML_SORTORDER_ATTRIBUTE)
                        .matches("^[Aa].*"));
            }
            if (xattribs.exists(XML_SORTERINITIALCAPACITY_ATTRIBUTE)){
                sort.setInternalSorterInitialCapacity(xattribs.getInteger(XML_SORTERINITIALCAPACITY_ATTRIBUTE));
            }
            if (xattribs.exists(XML_NUMBEROFTAPES_ATTRIBUTE)){
                sort.setNumberOfTapes(xattribs.getInteger(XML_NUMBEROFTAPES_ATTRIBUTE));
            }
            if (xattribs.exists(XML_BUFFER_CAPACITY_ATTRIBUTE)){
                sort.setBufferCapacity(xattribs.getInteger(XML_BUFFER_CAPACITY_ATTRIBUTE));
            }
            
            if (xattribs.exists(XML_TEMPORARY_DIRS)){
                sort.setTmpDirs(xattribs.getString(XML_TEMPORARY_DIRS).split(Defaults.Component.KEY_FIELDS_DELIMITER_REGEX));
            }
            
        } catch (Exception ex) {
	           throw new XMLConfigurationException(COMPONENT_TYPE + ":" + xattribs.getString(XML_ID_ATTRIBUTE," unknown ID ") + ":" + ex.getMessage(),ex);
        }
        return sort;
    }

    /**
     *  Description of the Method
     *
     * @return    Description of the Return Value
     */
    public boolean checkConfig() {
        return true;
    }

    /* (non-Javadoc)
     * @see org.jetel.graph.Node#getType()
     */
    public String getType() {
        return COMPONENT_TYPE;
    }

    public String[] getTmpDirs() {
        return tmpDirs;
    }

    public void setTmpDirs(String[] tmpDirs) {
        this.tmpDirs = tmpDirs;
    }
}

