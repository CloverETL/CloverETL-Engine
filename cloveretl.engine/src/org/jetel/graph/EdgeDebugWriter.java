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
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jetel.component.RecordFilter;
import org.jetel.component.RecordFilterFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.data.RingRecordBuffer;
import org.jetel.data.formatter.BinaryDataFormatter;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.TransformException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.metadata.MetadataUtils;
import org.jetel.util.bytes.CloverBuffer;
import org.jetel.util.file.FileUtils;

/**
 * This class writes data records to an {@link OutputStream}, the data records can be read by {@link EdgeDebugReader}.
 * {@link EdgeDebugWriter} and {@link EdgeDebugReader} classes are
 * used to store requested data records flowing through an edge to temporary files.
 * 
 * @author Kokon (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created 18. 11. 2013
 */
public class EdgeDebugWriter {

	private static final int MINIMUM_DELAY_BETWEEN_FLUSHES = 5000;
	
	private static Log logger = LogFactory.getLog(EdgeDebugWriter.class);

    private final String debugFile;

    private long debugStartRecord;
    private long debugMaxRecords; // max number of debugged records; 0 -> infinite
    private boolean debugLastRecords;
    private String filterExpression;
    private boolean sampleData;

	private final DataRecordMetadata metadata;

    /** used to store the ordinal of the data record currently processed (read or written) */
    private WritableByteChannel outputChannel;
    private BinaryDataFormatter formatter;
    private RingRecordBuffer ringRecordBuffer;
    private DataRecord recordOrdinal;
    private RecordFilter filter;
    private DataRecord tempRecord;
    private Sampler sampler;

    private Edge parentEdge; //can be null
    
    /** the number of data records processed so far */
    private int recordsCounter = 0;
    /** the number of debugged (stored) records */
    private int debuggedRecords = 0;
    
    private long lastFlushTime = 0L;

    public EdgeDebugWriter(OutputStream outputStream, DataRecordMetadata metadata) {
    	outputChannel = Channels.newChannel(outputStream);
    	this.metadata = metadata;
    	this.debugFile = null;
    }
    
    public EdgeDebugWriter(Edge parentEdge, String debugFile, DataRecordMetadata metadata) {
    	this.parentEdge = parentEdge;
        this.debugFile = debugFile;
        this.metadata = metadata;
    }
    
    public void init() {
    	try {
	    	tempRecord = DataRecordFactory.newRecord(metadata);
	    	tempRecord.init();
	    	
	    	if (outputChannel == null) {
	    		outputChannel = FileUtils.getWritableChannel(getContextURL(), debugFile, false);
	    	}
	    	
	    	formatter = new BinaryDataFormatter(outputChannel);
	    	formatter.setUnitarySerialization(true);
	    	formatter.init(metadata);
	
	        if (debugMaxRecords > 0 && debugLastRecords) {
	        	ringRecordBuffer = new RingRecordBuffer(debugMaxRecords);
	        	ringRecordBuffer.init();
	        	
	        	DataRecordMetadata recordOrdinalMetadata = new DataRecordMetadata("recordOrdinal");
	        	recordOrdinalMetadata.addField(new DataFieldMetadata("ordinal", DataFieldType.INTEGER, null));
	
	        	recordOrdinal = DataRecordFactory.newRecord(recordOrdinalMetadata);
	        	recordOrdinal.init();
	        }
	
	        if (filterExpression != null) {
	        	filter = RecordFilterFactory.createFilter(filterExpression, metadata, getGraph(), getEdgeId(), logger);
	        }
	
	        if (sampleData) {
	        	sampler = new Sampler();
	        }
	        
	        MetadataUtils.writeMetadata(metadata, outputChannel);
    	} catch (Exception e) {
    		throw new JetelRuntimeException("Initialization of edge debug writer failed.", e);
    	}
    }

    private URL getContextURL() {
    	if (parentEdge != null) {
    		return parentEdge.getGraph().getRuntimeContext().getContextURL();
    	} else {
    		return ContextProvider.getContextURL();
    	}
    }
    
    public void writeRecord(DataRecord record) throws IOException, InterruptedException {
        recordsCounter++;

        if (ringRecordBuffer != null) {
        	if (checkRecordToWrite(record)) {
	    		recordOrdinal.getField(0).setValue(recordsCounter);
	    		ringRecordBuffer.pushRecord(recordOrdinal);
	    		ringRecordBuffer.pushRecord(record);
            }
        } else if (checkNoOfDebuggedRecords() && checkRecordToWrite(record)) {
        	formatter.writeInt(recordsCounter);
        	formatter.write(record);
        	flushIfNeeded();
        	debuggedRecords++;
        }
    }
    
    private void flushIfNeeded() throws IOException, InterruptedException {
    	if (formatter != null && (isJobflow() || (System.currentTimeMillis() - lastFlushTime) > MINIMUM_DELAY_BETWEEN_FLUSHES)) {
    		flush();
    	}
    }

    private void flush() throws IOException, InterruptedException {
		formatter.flush();
		lastFlushTime = System.currentTimeMillis();
    }
    
    private boolean checkRecordToWrite(DataRecord record) {
    	return ((filter == null || isValid(record)) && (sampler == null || sampler.sample()));
    }
    
    private boolean isValid(DataRecord record) {
    	try {
			return filter.isValid(record);
		} catch (TransformException e) {
			throw new RuntimeException("Edge (" + getEdgeId() + ") debugging failed in filter expression.", e); 
		}
    }
    
    public void writeRecord(CloverBuffer byteBuffer) throws IOException, InterruptedException {
        recordsCounter++;

        tempRecord.deserialize(byteBuffer);
        byteBuffer.rewind();

        if (recordsCounter >= debugStartRecord) {
	        if (ringRecordBuffer != null) {
	        	if (checkRecordToWrite(tempRecord)) {
	                recordOrdinal.getField(0).setValue(recordsCounter);
	        		ringRecordBuffer.pushRecord(recordOrdinal);
	        		ringRecordBuffer.pushRecord(byteBuffer);
	        	}
	        } else if (checkNoOfDebuggedRecords() && checkRecordToWrite(tempRecord)) {
	        	formatter.writeInt(recordsCounter);
	        	//write the deserialized record instead of raw data record
	        	//the raw data record could be a token, but only regular records should be written
	        	formatter.write(tempRecord); 
	        	flushIfNeeded();
	        	debuggedRecords++;
	        }
        }
    }
    
    public boolean acceptMoreRecords() {
    	if (ringRecordBuffer != null) {
    		return true;
    	} else {
    		return checkNoOfDebuggedRecords();
    	}
    }
    
    private boolean checkNoOfDebuggedRecords() {
    	return (debugMaxRecords == 0 || debuggedRecords < debugMaxRecords);
    }

    public void eof() throws IOException {
    	formatter.flush();
    }
    
	/**
	 * Closes the EdgeDebuger (if buffer used, it writes it to the tape). In writing mode it also writes end flag (
	 * <code>-1</code>) to the tape to indicate that all data has been written (equivalent of EOF). View Data
	 * "Load more" functionality relies on this.
	 */
	public void close() {
		try {
			if (ringRecordBuffer != null) {
				DataRecord dataRecord = DataRecordFactory.newRecord(metadata);
				dataRecord.init();

				while (ringRecordBuffer.popRecord(recordOrdinal) != null && ringRecordBuffer.popRecord(dataRecord) != null) {
					formatter.writeInt((Integer) recordOrdinal.getField(0).getValue());
					formatter.write(dataRecord);
				}
			}

			formatter.writeInt(-1);
			formatter.close();
		} catch (IOException exception) {
			logger.error("Error writing debug records.");
		} catch (Exception ex) {
			logger.warn("Can't flush/rewind DataRecordTape.");
		}
	}
    
    private String getEdgeId() {
    	return parentEdge != null ? parentEdge.getId() : "";
    }
    
    private TransformationGraph getGraph() {
    	return parentEdge != null ? parentEdge.getGraph() : ContextProvider.getGraph();
    }
    
    private boolean isJobflow() {
    	if (getGraph() != null) {
    		return getGraph().getRuntimeJobType().isJobflow();
    	} else {
    		return false;
    	}
    }
    /**
	 * @return the debugStartRecord
	 */
	public long getDebugStartRecord() {
		return debugStartRecord;
	}

	/**
	 * @param debugStartRecord the debugStartRecord to set
	 */
	public void setDebugStartRecord(long debugStartRecord) {
		this.debugStartRecord = debugStartRecord;
	}

	/**
	 * @return the debugMaxRecords
	 */
	public long getDebugMaxRecords() {
		return debugMaxRecords;
	}

	/**
	 * @param debugMaxRecords the debugMaxRecords to set
	 */
	public void setDebugMaxRecords(long debugMaxRecords) {
		this.debugMaxRecords = debugMaxRecords;
	}

	/**
	 * @return the debugLastRecords
	 */
	public boolean isDebugLastRecords() {
		return debugLastRecords;
	}

	/**
	 * @param debugLastRecords the debugLastRecords to set
	 */
	public void setDebugLastRecords(boolean debugLastRecords) {
		this.debugLastRecords = debugLastRecords;
	}

	/**
	 * @return the filterExpression
	 */
	public String getFilterExpression() {
		return filterExpression;
	}

	/**
	 * @param filterExpression the filterExpression to set
	 */
	public void setFilterExpression(String filterExpression) {
		this.filterExpression = filterExpression;
	}

	/**
	 * @return the sampleData
	 */
	public boolean isSampleData() {
		return sampleData;
	}

	/**
	 * @param sampleData the sampleData to set
	 */
	public void setSampleData(boolean sampleData) {
		this.sampleData = sampleData;
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
