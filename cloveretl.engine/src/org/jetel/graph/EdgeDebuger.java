/*
 * Copyright (c) 2004-2005 Javlin Consulting s.r.o. All rights reserved.
 * 
 * $Header$
 */
package org.jetel.graph;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import org.jetel.data.DataRecord;
import org.jetel.data.tape.DataRecordTape;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.interpreter.ParseException;
import org.jetel.interpreter.TransformLangExecutor;
import org.jetel.interpreter.TransformLangParser;
import org.jetel.interpreter.ASTnode.CLVFStartExpression;
import org.jetel.interpreter.data.TLValue;
import org.jetel.metadata.DataRecordMetadata;

/**
 * This class perform buffering data flowing through the edge.
 * If the edge has defined 'debugFile' attribute, this debugger save data into defined file.
 * 
 * @author Martin Zatopek
 *
 */
public class EdgeDebuger {

    private boolean isReadMode;
    private DataRecordTape dataTape;
    
    private int debugMaxRecords; // max number of debugged records; 0 -> infinite
    private int debuggedRecords = 0; // currently number of debugged records
    private boolean sampleMode;
    
    private String filterExpression;
    private DataRecordMetadata metadata;
    private Filter filter;
    private Sampler sampler;
    
    /**
     * Constructor.
     * @param debugFile
     */
    public EdgeDebuger(String debugFile, boolean isReadMode) {
        this.isReadMode = isReadMode;
        dataTape = new DataRecordTape(debugFile, !isReadMode, false);
    }

    public EdgeDebuger(String debugFile, boolean isReadMode, int debugMaxRecords, 
    		String filterExpression, DataRecordMetadata metadata, boolean sampleMode) {
        this(debugFile, isReadMode);
        this.debugMaxRecords = debugMaxRecords;
        this.filterExpression = filterExpression;
        this.metadata = metadata;
        this.sampleMode = sampleMode;
    }
    
    public void init() throws IOException {
        dataTape.open();
        dataTape.addDataChunk();
        if(isReadMode) dataTape.rewind();
        
        if (filterExpression != null) {
            try {
            	filter = new Filter(metadata, filterExpression);
    		} catch (ComponentNotReadyException cnre) {
    			throw new IOException(cnre.getMessage());
    		}
        }
        
        if (sampleMode) {
        	sampler = new Sampler();
        }
    }

    public void writeRecord(DataRecord record) throws IOException {
        if (isReadMode){
            throw new RuntimeException("Error: Mixed read/write operation on DataRecordTape !");
        }
        
        if (checkRecordToWrite(record)) {
        	dataTape.put(record);
        	debuggedRecords++;
        }
    }

    public void writeRecord(ByteBuffer record) throws IOException {
        if (isReadMode){
            throw new RuntimeException("Error: Mixed read/write operation on DataRecordTape !");
        }
        
        if (checkRecordToWrite(record)) {
        	dataTape.put(record);
        	debuggedRecords++;
        }
    }
    
    /**
     * Decides if record will be debugged.
     * @param record record
     * @return true if current record should be debugged; else false
     */
    private boolean checkRecordToWrite(DataRecord record) {
    	return checkNoOfDebuggedRecords() && 
    		(filter == null || filter.check(record)) &&
    		(!sampleMode || sampler.sample());
    }
    
    /**
     * Decides if record will be debugged.
     * @param record record
     * @return true if current record should be debugged; else false
     */
    private boolean checkRecordToWrite(ByteBuffer record) {
    	return checkNoOfDebuggedRecords() && 
    		(filter == null || filter.check(record)) &&
    		(!sampleMode || sampler.sample());
    }

    /**
     * Check if number of debugged records is lower 
     * than maximum number of debugged records.
     * @return false when max number of records was debugged; else true
     */
    private boolean checkNoOfDebuggedRecords() {
    	if (debugMaxRecords == 0 || debuggedRecords < debugMaxRecords) {
        	return true;
        }
    	return false;
    }
    
    public DataRecord readRecord(DataRecord record) throws IOException {
        if (!isReadMode) {
            return null;
        }
        if (dataTape.get(record)){
            return record;
        }else{
            return null;
        }
    }

    public void close() {
        try {
            if(!isReadMode) {
                dataTape.flush(true);
            }
            dataTape.close();
        }catch(IOException ex){
            throw new RuntimeException("Can't flush/rewind DataRecordTape: " + ex.getMessage());
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
	
		public boolean check(DataRecord record) {
			executor.setInputRecords(new DataRecord[] {record});
			executor.visit(recordFilter, null);
			if (executor.getResult() == TLValue.TRUE_VAL) {
				return true;
			}
			
			return false;
		}
		
		public boolean check(ByteBuffer record) {
			tmpRecord.deserialize(record);
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
        private int sampleAdeptCounter = 0; //currently processes records
        
        // 3 -> first, second or third record will be chosen as first sample
        private final static int FIRST_SAMPLE_RANGE = 3;
        
    	public Sampler() {
    		random = new Random();
        	nextSample = random.nextInt(FIRST_SAMPLE_RANGE) + 1;
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
