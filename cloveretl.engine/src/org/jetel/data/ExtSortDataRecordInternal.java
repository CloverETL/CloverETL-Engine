package org.jetel.data;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jetel.data.tape.DataRecordTape;
import org.jetel.data.tape.TapeCarousel;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;

/**
 *  Class for external sorting of data records.<br>
 *  Incoming data are stored in in-memory buffer(s), buffers
 *  are allocated in the fly - instance of SortDataRecordInternal 
 *  is used for this. When capacity is finished, data are flushed to 
 *  the disk (each chunk is sorted).
 *  
 *  When storing phase is finished, sort method must be called - it 
 *  assures to sort and flush all in-memory data, so we are ready
 *  for merging phase.
 *  
 *  Now reading can start - during this phase, data are read
 *  from the disk from tapes and merged together.
 *  
 *  If size of incoming data is not bigger than defined limit,
 *  in-memory sorting occurs.
 *  
 *  Standard way of working with ExtSortDataRecordInternal (same as ExtSortDataRecordInternal):<br>
 *  <ol>
 *  <li>put() n-times
 *  <li>sort()
 *  <li>get() n-times
 *  </ol>
 *@author     dpavlis, jlehotsky
 *@see	      org.jetel.data.RecordKey
 */

public class ExtSortDataRecordInternal {

	private boolean doMerge = false;
	private SortDataRecordInternal sorter;
	private TapeCarousel tapeCarousel;
	private boolean carouselInitialized;
	private int numberOfTapes;
	private String[] tmpDirs;
	private String[] sortKeysNames;
	private boolean sortOrderAscending;
	private RecordKey sortKey;
	DataRecordMetadata inMetadata;
	private ByteBuffer recordBuffer;
	private boolean[] sourceRecordsFlags;
	private DataRecord[] sourceRecords;
	
	public ExtSortDataRecordInternal() {
		super();
        carouselInitialized = false;
	}

	/**
	 * Constructor for the ExtSortDataRecordInternal
	 * 
	 * @param metadata	Metadata describing records stored in internal buffer
	 * @param keyItems	Names of fields which compose the key used for sorting data
	 * @param sortAscending	True if required sort order is Ascending, otherwise False
	 * @param internalBufferCapacity Internal maximum capacity of a buffer
	 * @param numberOfTapes	Number of tapes to be used
	 * @param tmpDirs	List of names of temporary directories to be used for external sorting buffer on disk
	 */
	public ExtSortDataRecordInternal(DataRecordMetadata metadata, String[] keyItems, boolean sortAscending, int internalBufferCapacity,
			int numberOfTapes, String[] tmpDirs) {
	
		this.sortKeysNames = keyItems;		
		this.sortOrderAscending = sortAscending;
		this.numberOfTapes = numberOfTapes;
		this.tmpDirs = tmpDirs;
		
		inMetadata = metadata;
		
		if (internalBufferCapacity>0){	
            sorter = new SortDataRecordInternal(metadata, keyItems, sortAscending, false, internalBufferCapacity);
        } else {
            sorter = new SortDataRecordInternal(metadata, keyItems, sortAscending, false);
        }
		
		recordBuffer = ByteBuffer
        	.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
		
		if (recordBuffer == null) {
			throw new RuntimeException("Can NOT allocate internal record buffer ! Required size:"
                    + Defaults.Record.MAX_RECORD_SIZE);
		}
        
	}

	/**
	 *  Stores additional record into internal buffer for sorting
	 *
	 *@param  record  DataRecord to be stored
	 */
	public void put(DataRecord record) throws IOException {
		if (!sorter.put(record)) {
			// we need to sort & flush buffer on to tape and merge it
			// later
			doMerge = true;
			sorter.sort();
			flushToTapeSynchronously();			
			sorter.reset();
			if (!sorter.put(record)) {
				throw new RuntimeException(
						"Can't store record into sorter !");
			}			
		}
	}
	
	/**
	 *  Sorts internal array and flush it to disk, thus reading can start
	 *  Note: in the case of external sorting, when merge is necessary, other name would be suitable,
	 *  		it is named sort() to maintain consistency with SortDataRecordInternal
	 */
	public void sort() throws IOException, InterruptedException {
		if (doMerge) {
			// sort whatever remains in sorter
			sorter.sort();
			flushToTapeSynchronously();
			// we don't need sorter any more - free all its resources
			sorter.free();
			phaseMerge();
		} else {
			sorter.sort();
			sorter.rewind();
		}
	}
	
	/**
	 *  Gets the next data record in sorted order
	 *
	 *@param  recordData  ByteBuffer into which copy next record's data
	 *@return             True if there was next record or False
	 */
	DataRecord get() throws IOException {		
		
		int index;
		
		if (doMerge) {
			
			if (hasAnyData(sourceRecordsFlags)) {
				if (sortOrderAscending) {
                	index = getLowestIndex(sourceRecords, sourceRecordsFlags);
            	} else {
                	index = getHighestIndex(sourceRecords, sourceRecordsFlags);
            	}

				if (!tapeCarousel.getTape(index).get(sourceRecords[index])) {
            		sourceRecordsFlags[index] = false;
            	}
            	            	
            	return sourceRecords[index];
            	
			} else { 
				tapeCarousel.free();
				return null;
			}
		} else {			
			return sorter.get();
		}
	}
	
	/**
	 *  Gets the next data record in sorted order
	 *
	 *@param  recordData  ByteBuffer into which copy next record's data
	 *@return             True if there was next record or False
	 */
	public boolean get(ByteBuffer recordDataBuffer) throws IOException {		
		DataRecord record=get();
		if (record!=null){
		    record.serialize(recordDataBuffer);
		    recordDataBuffer.flip();
		    return true;
		}else{
		    return false;
		}		
	}

	/**
	 * Frees all resources (buffers, collections of internal sorter, etc) 
	 */
	public void free() {		
		sorter.free();
	}
	
	
	
	private void flushToTapeSynchronously() throws IOException {
        DataRecordTape tape;
        if (!carouselInitialized) {
            tapeCarousel = new TapeCarousel(numberOfTapes, tmpDirs);
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
        
        // --- read sorted records
        while (sorter.get(recordBuffer)) {
            tape.put(recordBuffer);
            recordBuffer.clear();
        }
        tape.flush(false);
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
        TapeCarousel targetCarousel = new TapeCarousel(tapeCarousel.numTapes(), tmpDirs);
        sourceRecords = new DataRecord[tapeCarousel.numTapes()];
        sourceRecordsFlags = new boolean[tapeCarousel.numTapes()];

        // initialize sort key which will be used when merging data
        sortKey = new RecordKey(sortKeysNames, inMetadata);
        sortKey.init();

        // initial creation & initialization of source records
        for (int i = 0; i < sourceRecords.length; i++) {
            sourceRecords[i] = new DataRecord(inMetadata);
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

}
