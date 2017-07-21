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
package org.jetel.data;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jetel.data.tape.DataRecordTape;
import org.jetel.data.tape.TapeCarousel;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.bytes.CloverBuffer;

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

public class ExternalSortDataRecord implements ISortDataRecord {

	private boolean doMerge = false;
	private InternalSortDataRecord sorter;
	private TapeCarousel tapeCarousel;
	private boolean carouselInitialized;
	private int numberOfTapes;
	private String[] sortKeysNames;
	private boolean[] sortOrderings;
	private RecordOrderedKey sortKey;
	DataRecordMetadata inMetadata;
	private CloverBuffer recordBuffer;
	private boolean[] sourceRecordsFlags;
	private DataRecord[] sourceRecords;
	int prevIndex;
	
	public ExternalSortDataRecord() {
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
	 * @param localeStr	String name of locale to use for collation. If null, no collator is used
	 */
	public ExternalSortDataRecord(DataRecordMetadata metadata, String[] keyItems, boolean[] sortOrderings, int internalBufferCapacity,
			int numberOfTapes) {
		this(metadata, keyItems, sortOrderings, internalBufferCapacity, numberOfTapes, null);
	}
	
	@Deprecated
	public ExternalSortDataRecord(DataRecordMetadata metadata, String[] keyItems, boolean[] sortOrderings, int internalBufferCapacity,
			int numberOfTapes, String localeStr) {
		this(metadata, keyItems, sortOrderings, internalBufferCapacity, numberOfTapes, null, false);
	}
	
	@Deprecated
	public ExternalSortDataRecord(DataRecordMetadata metadata, String[] keyItems, boolean[] sortOrderings, int internalBufferCapacity,
				int numberOfTapes, String localeStr, boolean caseSensitive) {

		this.sortKeysNames = keyItems;		
		this.sortOrderings = sortOrderings;
		this.numberOfTapes = numberOfTapes;
		this.prevIndex = -1;
		inMetadata = metadata;
		if (internalBufferCapacity>0){	
            sorter = new InternalSortDataRecord(metadata, keyItems, sortOrderings, false, internalBufferCapacity);
        } else {
            sorter = new InternalSortDataRecord(metadata, keyItems, sortOrderings, false);
        }
		
		// create collators
		if (localeStr != null) {
			sorter.setCollatorLocale(localeStr);
			sorter.setCaseSensitive(caseSensitive);
		}
		
		recordBuffer = CloverBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
	}

	@Override
	public boolean put(DataRecord record) throws IOException, InterruptedException {
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
		return true;
	}
	
	@Override
	public void sort() throws IOException, InterruptedException {
		if (doMerge) {
			// sort whatever remains in sorter
			sorter.sort();
			flushToTapeSynchronously();		
			phaseMerge();
		} else {
			sorter.sort();
			sorter.rewind();
		}
	}
	
	@Override
	public DataRecord get() throws IOException, InterruptedException {		
		
		int index;
		
		if (doMerge) {

			if (prevIndex > -1) {
				if (!tapeCarousel.getTape(prevIndex).get(sourceRecords[prevIndex])) {
	                sourceRecordsFlags[prevIndex] = false;
	            }
			}
			
	        if (hasAnyData(sourceRecordsFlags)) {
	        	
	            index = getLowestIndex(sourceRecords, sourceRecordsFlags);

	            prevIndex = index;
	            
	            SynchronizeUtils.cloverYield();
	            return sourceRecords[index];
	        } else {
				return null;
			}
		} else {			
			return sorter.get();
		}
	}
	
	@Override
	public boolean get(CloverBuffer recordDataBuffer) throws IOException, InterruptedException {		
		DataRecord record=get();
		if (record!=null){
		    record.serialize(recordDataBuffer);
		    recordDataBuffer.flip();
		    return true;
		}else{
		    return false;
		}		
	}

	@Override
	@Deprecated
	public boolean get(ByteBuffer recordDataBuffer) throws IOException, InterruptedException {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(recordDataBuffer);
		boolean result = get(wrappedBuffer);
		if (wrappedBuffer.buf() != recordDataBuffer) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
		return result;
	}

	@Override
	public void reset() {
		sorter.reset();
		if (carouselInitialized && tapeCarousel != null) {
			tapeCarousel.clear();
		}
		recordBuffer.clear();
		this.prevIndex = -1;

	}
	
	@Override
	public void postExecute(){
		if (carouselInitialized && (tapeCarousel!=null)) {
			try {
				tapeCarousel.free();
				carouselInitialized = false;
			} catch (InterruptedException e) {
				// DO NOTHING
			}
		}
		sorter.postExecute();
	}
	
	@Override
	public void free() {
		sorter.free();
	}
	
	private void flushToTapeSynchronously() throws IOException, InterruptedException {
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
        TapeCarousel targetCarousel = new TapeCarousel(tapeCarousel.numTapes());
        sourceRecords = new DataRecord[tapeCarousel.numTapes()];
        sourceRecordsFlags = new boolean[tapeCarousel.numTapes()];

        // initialize sort key which will be used when merging data
        sortKey = new RecordOrderedKey(sortKeysNames, sortOrderings, inMetadata, sorter.getComparator().getCollators());
        sortKey.setEqualNULLs(true);
        sortKey.init();

        // initial creation & initialization of source records
        for (int i = 0; i < sourceRecords.length; i++) {
            sourceRecords[i] = DataRecordFactory.newRecord(inMetadata);
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
                    index = getLowestIndex(sourceRecords,
                                sourceRecordsFlags);
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
     * Populates source records array with records from individual tapes (included in
     * tape carousel). Sets flags in flags array for those records which contain valid data.
     * 
     * @param tapeCarousel
     * @param sourceRecords
     * @param sourceRecordsFlags
     * @throws IOException
     * @throws InterruptedException 
     */
    private final void loadUpRecords(TapeCarousel tapeCarousel,
            DataRecord[] sourceRecords, boolean[] sourceRecordsFlags)
            throws IOException, InterruptedException {
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
     * @throws InterruptedException 
     * @throws IOException 
     */
    private final static boolean hasMoreChunks(TapeCarousel tapeCarousel) throws InterruptedException, IOException {
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
