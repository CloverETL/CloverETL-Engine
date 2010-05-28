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
package org.jetel.graph;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.RingRecordBuffer;
import org.jetel.data.tape.DataRecordTape;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangExecutor;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.ASTnode.CLVFStartExpression;
import org.jetel.interpreter.data.TLBooleanValue;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This class perform buffering data flowing through the edge.
 * If the edge has defined 'debugFile' attribute, this debugger save data into defined file.
 * 
 * @author Martin Zatopek
 *
 */
public class EdgeDebuger {

	private static Log logger = LogFactory.getLog(EdgeDebuger.class);

    private final String debugFile;
    private final boolean readMode;

    private final int debugMaxRecords; // max number of debugged records; 0 -> infinite
    private final boolean debugLastRecords;
    private final String filterExpression;
    private final DataRecordMetadata metadata;
    private final boolean sampleData;

    /** used to store the ordinal of the data record currently processed (read or written) */
    private DataRecord recordOrdinal;
    private DataRecordTape dataTape;
    private RingRecordBuffer recordBuffer;
    private Filter filter;
    private Sampler sampler;

    /** the number of data records processed so far */
    private int recordsCounter = 0;
    /** the number of debugged (stored) records */
    private int debuggedRecords = 0; 

    public EdgeDebuger(String debugFile, boolean readMode) {
    	this(debugFile, readMode, 0, true, null, null, false);
    }

    public EdgeDebuger(String debugFile, boolean readMode, int debugMaxRecords, boolean debugLastRecords, 
    		String filterExpression, DataRecordMetadata metadata, boolean sampleData) {
        this.debugFile = debugFile;
        this.readMode = readMode;
        this.debugMaxRecords = debugMaxRecords;
        this.debugLastRecords = debugLastRecords;
        this.filterExpression = filterExpression;
        this.metadata = metadata;
        this.sampleData = sampleData;
    }
    
    public void init() throws ComponentNotReadyException, IOException, InterruptedException {
    	DataRecordMetadata recordOrdinalMetadata = new DataRecordMetadata("recordOrdinal", DataRecordMetadata.DELIMITED_RECORD);
    	recordOrdinalMetadata.addField(new DataFieldMetadata("ordinal", DataFieldMetadata.INTEGER_FIELD, ";"));

    	recordOrdinal = new DataRecord(recordOrdinalMetadata);
    	recordOrdinal.init();

    	dataTape = new DataRecordTape(debugFile, !readMode, false);
        dataTape.open();
        dataTape.addDataChunk();

        if (readMode) {
        	dataTape.rewind();
        }

        if (debugMaxRecords > 0 && debugLastRecords) {
        	// we need to store the record with its ordinal, so double the size of the buffer
        	recordBuffer = new RingRecordBuffer(2 * debugMaxRecords);
        	recordBuffer.init();
        }

        if (filterExpression != null) {
        	filter = new Filter(metadata, filterExpression);
        }

        if (sampleData) {
        	sampler = new Sampler();
        }
    }

	public void reset() throws ComponentNotReadyException {
		//TODO DataRecordTape should be able to reset itself
        try { //TODO dataTape doesn't have reset method?
			dataTape.close();
		} catch (Exception ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
		}

		dataTape = new DataRecordTape(debugFile, !readMode, false);

		try {
			dataTape.open();
	        dataTape.addDataChunk();
	        if(readMode) dataTape.rewind();
		} catch (Exception e) {
			throw new ComponentNotReadyException("Edge debugging cannot be reseted, IO exception occured.", e);
		}

        if (recordBuffer != null) {
        	recordBuffer.reset();
        }

        if (filter != null) {
        	filter.reset();
        }

        if (sampler != null) {
        	sampler.reset();
        }

        recordsCounter = 0;
        debuggedRecords = 0;
	}

    public void writeRecord(DataRecord record) throws IOException, InterruptedException {
        if (readMode) {
			throw new IllegalStateException("Error: Mixed read/write operation on DataRecordTape!");
		}

        recordsCounter++;
        recordOrdinal.getField(0).setValue(recordsCounter);

        if (recordBuffer != null) {
        	if (checkRecordToWrite(record)) {
        		recordBuffer.pushRecord(recordOrdinal);
        		recordBuffer.pushRecord(record);
        	}
        } else if (checkNoOfDebuggedRecords() && checkRecordToWrite(record)) {
        	dataTape.put(recordOrdinal);
        	dataTape.put(record);

        	debuggedRecords++;
        }
    }

    private boolean checkRecordToWrite(DataRecord record) {
    	return ((filter == null || filter.check(record)) && (sampler == null || sampler.sample()));
    }
    
    public void writeRecord(ByteBuffer byteBuffer) throws IOException, InterruptedException {
        if (readMode) {
			throw new IllegalStateException("Error: Mixed read/write operation on DataRecordTape!");
		}

        recordsCounter++;
        recordOrdinal.getField(0).setValue(recordsCounter);

        if (recordBuffer != null) {
        	if (checkRecordToWrite(byteBuffer)) {
        		recordBuffer.pushRecord(recordOrdinal);
        		recordBuffer.pushRecord(byteBuffer);
        	}
        } else if (checkNoOfDebuggedRecords() && checkRecordToWrite(byteBuffer)) {
        	dataTape.put(recordOrdinal);
        	dataTape.put(byteBuffer);

        	debuggedRecords++;
        }
    }
    
    private boolean checkRecordToWrite(ByteBuffer byteBuffer) {
    	return ((filter == null || filter.check(byteBuffer)) && (sampler == null || sampler.sample()));
    }

    private boolean checkNoOfDebuggedRecords() {
    	return (debugMaxRecords == 0 || debuggedRecords < debugMaxRecords);
    }

    /**
     * Reads previously stored debug record into the given record reference.
     *
     * @param record the record that will be filled with data
     *
     * @return the (1-based) ordinal of the data record, or -1 if there are no more records
     *
     * @throws IOException if any I/O error occurs
     * @throws InterruptedException
     */
    public int readRecord(DataRecord record) throws IOException, InterruptedException {
		if (!readMode) {
			return -1;
		}

		if (dataTape.get(recordOrdinal) && dataTape.get(record)) {
			return (Integer) recordOrdinal.getField(0).getValue();
		}

		return -1;
	}

    public void close() {
		try {
			if (recordBuffer != null) {
				DataRecord dataRecord = new DataRecord(metadata);
				dataRecord.init();

				while (recordBuffer.popRecord(recordOrdinal) != null && recordBuffer.popRecord(dataRecord) != null) {
					dataTape.put(recordOrdinal);
					dataTape.put(dataRecord);
				}
			}

			if (!readMode) {
				dataTape.flush(true);
			}

			dataTape.close();
		} catch (IOException exception) {
			logger.error("Error writing debug records.");
		} catch (Exception ex) {
			logger.warn("Can't flush/rewind DataRecordTape.");
		}
	}
    
    /**
	 * Class for filtering data record.
	 * 
	 * @author 		Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
	 *				(c) Javlin Consulting (www.javlinconsulting.cz)
	 * @since 20.12.2007
	 */
    private static class Filter {
    	private CLVFStartExpression recordFilter;
    	private TransformLangExecutor executor;
    	private DataRecord tmpRecord;

    	public Filter(DataRecordMetadata metadata, String filterExpression) throws ComponentNotReadyException {
    		TransformLangParser parser = new TransformLangParser(metadata, filterExpression);
    		if (parser != null) {
    			try {
    				recordFilter = parser.StartExpression();
    			} catch (ParseException pe) {
                    throw new ComponentNotReadyException("Parser error when parsing expression: " + pe.getMessage());
                } catch (Exception e) {
    				throw new ComponentNotReadyException("Error when parsing expression: " + e.getMessage());
    			}
                try{
                    recordFilter.init();
                } catch (Exception e) {
                    throw new ComponentNotReadyException("Error when initializing expression executor: " + e.getMessage());
                }
    		} else {
    			throw new ComponentNotReadyException("Can't create filter expression parser !"); 
    		}
    		
            executor = new TransformLangExecutor();
            
            tmpRecord = new DataRecord(metadata);
            tmpRecord.init();
    	}
	
    	public void reset() {
    		//DO NOTHING
    	}
    	
		public boolean check(DataRecord record) {
			executor.setInputRecords(new DataRecord[] {record});
			executor.visit(recordFilter, null);
			if (executor.getResult() == TLBooleanValue.TRUE) {
				return true;
			}
			
			return false;
		}
		
		public boolean check(ByteBuffer record) {
			tmpRecord.deserialize(record);
			record.rewind();
			return check(tmpRecord);
		}
    }
    
    /**
	 * Class for sampling data record.
	 * 
	 * @author 		Miroslav Haupt (Mirek.Haupt@javlinconsulting.cz)
	 *				(c) Javlin Consulting (www.javlinconsulting.cz)
	 * @since 25.12.2007
	 */
    private static class Sampler {
    	private Random random;
        private int nextSample;
        private int sampleAdeptCounter; //currently processes records
        
        // 3 -> first, second or third record will be chosen as first sample
        private final static int FIRST_SAMPLE_RANGE = 3;
        
    	public Sampler() {
    		random = new Random();
    		reset();
    	}
    	
    	public void reset() {
        	nextSample = random.nextInt(FIRST_SAMPLE_RANGE) + 1;
    		sampleAdeptCounter = 0;
    	}
    	
        /**
         * Decides if current record will be chosen as a sample to debug.
         * The more records is debugged the less often this method returns true.
         * @return true if current record should be debugged; else false
         */
        public boolean sample() {
        	sampleAdeptCounter++;
        	if (sampleAdeptCounter >= nextSample) {
    			nextSample += Math.sqrt(random.nextInt(sampleAdeptCounter)) + 1;
    			return true;
    		}
        	return false;
        }
        
    }
}
