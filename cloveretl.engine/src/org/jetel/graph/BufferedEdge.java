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

import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.DynamicRecordBuffer;
import org.jetel.graph.runtime.ExecutionType;
import org.jetel.util.bytes.CloverBuffer;

/**
 * A class that represents Edge - data connection between two NODEs.
 * <p>
 * This EDGE buffers data in-memory and if this buffer is exhausted then
 * on disk to allow unlimited buffering for writer.
 * <p>
 * It internally allocates two buffers (for reading, writing) of 
 * <code>BUFFERED_EDGE_INTERNAL_BUFFER_SIZE</code>.
 *
 * @author David Pavlis, Javlin a.s. &lt;david.pavlis@javlin.eu&gt;
 *
 * @see InputPort
 * @see OutputPort
 *
 */
//TODO refactor this edge implementation using PersistentBufferQueue
public class BufferedEdge extends EdgeBase {

    private long outputRecordCounter;
    private long inputRecordCounter;
    private long byteCounter;
    private int internalBufferSize;

    private DynamicRecordBuffer recordBuffer;

    /**
     * Constructs an <code>Edge</code> with default internal buffer size.
     *
     * @param proxy edge proxy object
     */
	public BufferedEdge(Edge proxy) {
		this(proxy, Defaults.Graph.BUFFERED_EDGE_INTERNAL_BUFFER_SIZE);
	}

	/**
     * Constructs an <code>Edge</code> with desired internal buffer size.
	 *
     * @param proxy edge proxy object
	 * @param internalBufferSize the desired size of the internal buffer used for buffering data records
	 */
	public BufferedEdge(Edge proxy, int internalBufferSize) {
		super(proxy);
		this.internalBufferSize = internalBufferSize;
	}

	@Override
	public long getOutputRecordCounter() {
		return outputRecordCounter;
	}

    @Override
	public long getInputRecordCounter() {
        return inputRecordCounter;
    }

    @Override
	public long getOutputByteCounter(){
        return byteCounter;
    }

    @Override
	public long getInputByteCounter(){
        return byteCounter;
    }

    @Override
	public int getBufferedRecords(){
        return recordBuffer.getBufferedRecords();
    }
    
    @Override
    public int getUsedMemory() {
    	return recordBuffer.getBufferSize();
    }
    
	@Override
	public void init() throws IOException {
		recordBuffer = new DynamicRecordBuffer(internalBufferSize);
		if (proxy != null && proxy.getGraph() != null) {
			//for single thread execution we can say that the usage of record buffer is sequential - first all write operation and then all read operations
			recordBuffer.setSequentialReading(proxy.getGraph().getRuntimeContext().getExecutionType() == ExecutionType.SINGLE_THREAD_EXECUTION);
		}
		recordBuffer.init();
	}

	@Override
	public void preExecute() {
		super.preExecute();
		
		outputRecordCounter = 0;
		inputRecordCounter = 0;
		byteCounter = 0;
		recordBuffer.setVerbose(verbose);
		recordBuffer.preExecute();
	}
	
	@Override
	public void postExecute() {
		super.postExecute();

		recordBuffer.reset();
	}
	
	@Override
	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
        DataRecord ret = recordBuffer.readRecord(record);

        if (ret != null) {
			inputRecordCounter++;
		}

		return ret;
    }

	@Override
	public boolean readRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
        boolean ret = recordBuffer.readRecord(record);

        if (ret) {
			inputRecordCounter++;
		}

        return ret;
    }

	@Override
	public void writeRecord(DataRecord record) throws IOException, InterruptedException {
        byteCounter += recordBuffer.writeRecord(record);
        outputRecordCounter++;
	}

	@Override
	public void writeRecordDirect(CloverBuffer record) throws IOException, InterruptedException {
	    byteCounter += recordBuffer.writeRecord(record);
        outputRecordCounter++;
    }

	@Override
	public void eof() throws InterruptedException {
        try {
			recordBuffer.setEOF();
		} catch (IOException ex) {
			throw new RuntimeException("Error when closing BufferedEdge", ex);
		}
	}

    @Override
    public boolean isEOF() {
        return recordBuffer.isClosed();
    }

    @Override
	public void free() {
		try {
			recordBuffer.close();
		} catch (IOException ex) {
			LogFactory.getLog(getClass()).warn("Error closing the record buffer!", ex);
		}
	}

	@Override
	public boolean hasData() {
		return recordBuffer.hasData();
	}

    public void setInternalBufferSize(int internalBufferSize) {
		if (internalBufferSize > Defaults.Graph.BUFFERED_EDGE_INTERNAL_BUFFER_SIZE) {
			this.internalBufferSize = internalBufferSize;
		}
	}

    @Override
    public long getReaderWaitingTime() {
    	return recordBuffer.getReaderWaitingTime();
    }
    
    @Override
    public long getWriterWaitingTime() {
    	return recordBuffer.getWriterWaitingTime();
    }
    
    @Override
    public void waitForEOF() throws InterruptedException {
    	recordBuffer.waitForEOF();
    }
    
}
