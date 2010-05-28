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

import org.apache.commons.logging.LogFactory;
import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.DynamicRecordBuffer;

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
 * @revision $Revision$
 */
public class BufferedEdge extends EdgeBase {

    private int outputRecordCounter;
    private int inputRecordCounter;
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

	public int getOutputRecordCounter() {
		return outputRecordCounter;
	}

    public int getInputRecordCounter() {
        return inputRecordCounter;
    }

    public long getOutputByteCounter(){
        return byteCounter;
    }

    public long getInputByteCounter(){
        return byteCounter;
    }

    public int getBufferedRecords(){
        return recordBuffer.getBufferedRecords();
    }
    
	public void init() throws IOException {
		recordBuffer = new DynamicRecordBuffer(internalBufferSize);
		recordBuffer.init();
		outputRecordCounter = 0;
		inputRecordCounter = 0;
		byteCounter = 0;
	}

	@Override
	public void reset() {
        recordBuffer.reset();
		outputRecordCounter = 0;
		inputRecordCounter = 0;
		byteCounter = 0;
	}

	public DataRecord readRecord(DataRecord record) throws IOException, InterruptedException {
        DataRecord ret = recordBuffer.readRecord(record);

        if (ret != null) {
			inputRecordCounter++;
		}

		return ret;
    }

	public boolean readRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
        boolean ret = recordBuffer.readRecord(record);

        if (ret) {
			inputRecordCounter++;
		}

        return ret;
    }

	public void writeRecord(DataRecord record) throws IOException, InterruptedException {
        byteCounter += recordBuffer.writeRecord(record);
        outputRecordCounter++;
	}

	public void writeRecordDirect(ByteBuffer record) throws IOException, InterruptedException {
	    byteCounter += recordBuffer.writeRecord(record);
        outputRecordCounter++;
    }

	public void eof() throws InterruptedException {
        try {
			recordBuffer.setEOF();
		} catch (IOException ex) {
			throw new RuntimeException("Error when closing BufferedEdge: " + ex.getMessage(), ex);
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

	public boolean hasData() {
		return recordBuffer.hasData();
	}

    public void setInternalBufferSize(int internalBufferSize) {
		if (internalBufferSize > Defaults.Graph.BUFFERED_EDGE_INTERNAL_BUFFER_SIZE) {
			this.internalBufferSize = internalBufferSize;
		}
	}

}
