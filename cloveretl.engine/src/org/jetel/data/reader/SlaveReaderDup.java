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
package org.jetel.data.reader;

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.Defaults;
import org.jetel.data.FileRecordBuffer;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;
import org.jetel.util.bytes.CloverBuffer;

/**
 * Slave reader with duplicates support. Uses file buffer to store duplicate records. Support rewind operation.
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class SlaveReaderDup implements InputReader {
	private static final int CURRENT = 0;
	private static final int NEXT = 1;

	private InputPort inPort;
	protected RecordKey key;
	private DataRecord[] rec = new DataRecord[2];
	private FileRecordBuffer recBuf;
	private CloverBuffer rawRec = CloverBuffer.allocateDirect(Defaults.Record.INITIAL_RECORD_SIZE);
	private boolean firstRun;
	private boolean getFirst;
	DataRecord deserializedRec;
	
	private InputOrdering inputOrdering = InputOrdering.UNDEFINED;

	public SlaveReaderDup(InputPort inPort, RecordKey key) {
		this.inPort = inPort;
		this.key = key;
		this.deserializedRec = new DataRecord(inPort.getMetadata());
		this.deserializedRec.init();
		this.rec[CURRENT] = new DataRecord(inPort.getMetadata());
		this.rec[NEXT] = new DataRecord(inPort.getMetadata());
		this.rec[CURRENT].init();
		this.rec[NEXT].init();
		this.recBuf = new FileRecordBuffer(null);
		this.firstRun = true;
	}
	
	public void reset() throws ComponentNotReadyException {
		this.rec[CURRENT] = new DataRecord(inPort.getMetadata());
		this.rec[NEXT] = new DataRecord(inPort.getMetadata());
		this.rec[CURRENT].init();
		this.rec[NEXT].init();
		recBuf.clear();
		this.firstRun = true;
	}
	
	public void free() {
		inPort = null;
	}
	
	private void swap() {
		DataRecord tmp = rec[CURRENT];
		rec[CURRENT] = rec[NEXT];
		rec[NEXT] = tmp;
	}

	public boolean loadNextRun() throws InterruptedException, IOException {
		getFirst = true;
		if (inPort == null) {
			rec[CURRENT] = rec[NEXT] = null;
			return false;
		} 
		if (firstRun) {	// first call of this function
			firstRun = false;
			// load first record of the run
			if (inPort.readRecord(rec[NEXT]) == null) {
				rec[CURRENT] = rec[NEXT] = null;
				return false;
			}
		}
		recBuf.clear();
		swap();
		while (true) {
//			rec[NEXT].reset();
			if (inPort.readRecord(rec[NEXT]) == null) {
				rec[NEXT] = null;
				return true;
			}
			int comparison = key.compare(rec[CURRENT], rec[NEXT]);
			if (comparison != 0) {	// beginning of new run
				inputOrdering = SlaveReader.updateOrdering(comparison, inputOrdering);
				return true;
			}
			// move record to buffer
			rawRec.clear();
			rec[NEXT].serialize(rawRec);
			rawRec.flip();
			recBuf.push(rawRec);				
		}
	}

	public void rewindRun() {
		getFirst = true;
		recBuf.rewind();
	}

	public DataRecord getSample() {
		if (firstRun) {
			return null;
		}
		return rec[CURRENT];
	}

	public DataRecord next() throws IOException {
		if (firstRun) {
			return null;
		}
		if (getFirst) {
			getFirst = false;
			return rec[CURRENT];
		}
		rawRec.clear();
		if (recBuf.shift(rawRec) == null) {
			return null;
		}
		rawRec.flip();
		deserializedRec.deserialize(rawRec);
		return deserializedRec;
	}

	public RecordKey getKey() {
		return key;
	}
	
	public int compare(InputReader other) {
		DataRecord rec1 = getSample();
		DataRecord rec2 = other.getSample();
//		if (rec1 == null) {
//			return rec2 == null ? 0 : -1;
//		} else if (rec2 == null) {
//			return 1;
//		}
		if (rec1 == null) {
			return 1;// null is greater than any other reader (as in DriverReader)
//			return rec2 == null ? 0 : 1;	
		} else if (rec2 == null) {
			return -1;
		}
		return key.compare(other.getKey(), rec1, rec2);
	}
	
	public boolean hasData() {
		return rec[NEXT] != null;
	}		

	@Override
	public String toString() {
		return getSample().toString();
	}

	public InputOrdering getOrdering() {
		return inputOrdering;
	}

}