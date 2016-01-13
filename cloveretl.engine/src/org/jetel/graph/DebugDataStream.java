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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.util.bytes.CloverBuffer;

/**
 * This unit wraps provided debug file {@link InputStream} and offers debug data to read according to provided
 * count/offset settings and filter expression.
 * 
 * @author jan.michalica (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 11.8.2014
 */
public class DebugDataStream extends InputStream {
	
	private static final Logger log = Logger.getLogger(DebugDataStream.class);
	
	private InputStream in;
	
	private EdgeDebugReader reader;
	private EdgeDebugWriter writer;
	private CloverBuffer buffer;
	private ByteArrayBidirectionalStream sink;
	
	private boolean allRead;
	
	private int recordStart;
	private int recordCount;
	private String filterExpression;
	private String fieldSelection[];
	
	private static class ByteArrayBidirectionalStream extends ByteArrayOutputStream {
		
		public ByteArrayBidirectionalStream() {
			super(4096);
		}
		
	    /**
	     * Reads the next byte of data from this input stream. The value
	     * byte is returned as an <code>int</code> in the range
	     * <code>0</code> to <code>255</code>. If no byte is available
	     * because the end of the stream has been reached, the value
	     * <code>-1</code> is returned.
	     * <p>
	     * This <code>read</code> method
	     * cannot block.
	     *
	     * @return  the next byte of data, or <code>-1</code> if the end of the
	     *          stream has been reached.
	     *          
		 * @see java.io.ByteArrayInputStream#read()
	     */
		public synchronized int read() {
			if (count == 0) {
				return -1;
			}
			// CLO-5868: see java.io.ByteArrayInputStream.read()
			int value = buf[0] & 0xff; // 0xff ensures that the returned int is in the range 0..255
			
			count--;
			System.arraycopy(buf, 1, buf, 0, count);
			return value;
		}
		
		public synchronized int read(byte buf[], int offset, int length) {
			
			final int count = Math.min(this.count, length);
			System.arraycopy(this.buf, 0, buf, 0, count);
			System.arraycopy(this.buf, count, this.buf, 0, this.count - count);
			this.count -= count;
			return count;
		}
	}
	
	public DebugDataStream(InputStream dataStream, int recordStart, int recordCount,
		String filterExepression, String fieldSelection[]) {
		if (dataStream == null) {
			throw new NullPointerException();
		}
		this.in = dataStream;
		this.recordStart = recordStart;
		this.recordCount = recordCount;
		this.filterExpression = filterExepression;
		this.fieldSelection = fieldSelection;
	}
	
	public void init() throws ComponentNotReadyException, IOException, InterruptedException {
		
		reader = new EdgeDebugReader(in);
		reader.init();
		
		sink = new ByteArrayBidirectionalStream();
		
		writer = new EdgeDebugWriter(sink, reader.getMetadata());
		
		// compute excluded fields (if any)
		if (fieldSelection != null && fieldSelection.length > 0) {
			List<String> excluded = new ArrayList<>();
			List<String> included = Arrays.asList(fieldSelection);
			for (String field : reader.getMetadata().getFieldNamesArray()) {
				if (!included.contains(field)) {
					excluded.add(field);
				}
			}
			writer.setExcludedFields(excluded.toArray(new String[excluded.size()]));
		}
		writer.setFilterExpression(filterExpression);
		writer.setDebugStartRecord(recordStart);
		writer.setDebugMaxRecords(recordCount);
		writer.init();
		
		buffer = DataRecordFactory.newRecordCloverBuffer();
	}
	
	@Override
	public int read() throws IOException {
		if (!hasData()) {
			return -1;
		}
		return sink.read();
	}
	
	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}
	
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (!hasData()) {
			return -1;
		}
		return sink.read(b, off, len);
	}
	
	@Override
	public void close() {
		closeAll();
	}
	
	protected void closeAll() {
		try {
			reader.close();
		} catch (Exception e) {
			log.warn("Error while closing debug reader.", e);
		}
		try {
			writer.close();
		} catch (Exception e) {
			log.warn("Error while closing debug writer.", e);
		}
		try {
			in.close();
		} catch (Exception e) {
			log.warn("Error while closing underlying data stream.", e);
		}
	}
	
	/**
	 * Returns <code>true</code> if at least one byte can be read from the stream.
	 * 
	 * @return
	 * @throws IOException
	 */
	private boolean hasData() throws IOException {
		if (sink.size() > 0) {
			return true;
		}
		if (allRead) {
			return false;
		}
		try {
			while (!allRead && (sink.size() == 0)) {
				if (!fetchNextRecord()) {
					allRead = true;
				}
			}
			if (allRead) {
				writer.close(); // enforce flush
			}
			return sink.size() > 0;
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
	
	private boolean fetchNextRecord() throws IOException, InterruptedException {
		
		final long status = reader.readRecord(buffer);
		if (status > 0 && writer.acceptMoreRecords()) {
			writer.writeRecord(buffer);
			writer.flush();
			return true;
		} else {
			writer.close();
			return false;
		}
	}
}
