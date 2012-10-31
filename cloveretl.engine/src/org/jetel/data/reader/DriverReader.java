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
import org.jetel.data.DataRecordFactory;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;

/**
 * Reader for driver input. Doesn't use record buffer but also doesn't support rewind operation.
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class DriverReader extends InputReader {
	private InputPort inPort;
	private boolean blocked;
	
	private InputOrdering inputOrdering = InputOrdering.UNDEFINED;
	protected int lastCompare;
	
	public DriverReader(InputPort inPort, RecordKey key) {
		this.inPort = inPort;
		this.key = key;
		this.rec[CURRENT] = DataRecordFactory.newRecord(inPort.getMetadata());
		this.rec[NEXT] = DataRecordFactory.newRecord(inPort.getMetadata());
		this.rec[CURRENT].init();
		this.rec[NEXT].init();
		recCounter = 0;
		blocked = false;
	}
	
	@Override
	public void reset() throws ComponentNotReadyException {
		this.rec[CURRENT] = DataRecordFactory.newRecord(inPort.getMetadata());
		this.rec[NEXT] = DataRecordFactory.newRecord(inPort.getMetadata());
		this.rec[CURRENT].init();
		this.rec[NEXT].init();
		recCounter = 0;
		blocked = false;
	}
	
	@Override
	public void free() {
		inPort = null;
	}

	@Override
	public boolean loadNextRun() throws InterruptedException, IOException {
		if (inPort == null) {
			return false;
		}
		if (recCounter == 0) {	// first call of this function
			// load first record of the run
			if (inPort.readRecord(rec[NEXT]) == null) {
				rec[NEXT] = rec[CURRENT] = null;
				return false;
			}
			recCounter = 1;
			return true;
		}

		if (blocked) {
			blocked = false;
			return true;
		}
		while (true){
			swap();
			if (inPort.readRecord(rec[NEXT]) == null) {
				rec[NEXT] = rec[CURRENT] = null;
				return false;
			}
			recCounter++;
			lastCompare = key.compare(rec[CURRENT], rec[NEXT]);
			if (lastCompare != 0){
				inputOrdering = InputReader.updateOrdering(lastCompare, inputOrdering);
				break;
			}
		}
		return true;
	}

	@Override
	public void rewindRun() {
		throw new UnsupportedOperationException();
	}

	@Override
	public DataRecord getSample() {
		return blocked ? rec[CURRENT] : rec[NEXT];
	}

	@Override
	public DataRecord next() throws IOException, InterruptedException {
		if (blocked || inPort == null) {
			return null;
		}
		swap();
		if (inPort.readRecord(rec[NEXT]) == null) {
			rec[NEXT] = null;
			blocked = false;
		} else {
			recCounter++;
			lastCompare = key.compare(rec[CURRENT], rec[NEXT]);
			inputOrdering = InputReader.updateOrdering(lastCompare, inputOrdering);
			blocked = lastCompare != 0;
		}
		return rec[CURRENT];
	}
	
	public DataRecord last() throws IOException, InterruptedException{
		DataRecord last = next();
		DataRecord tmp;
		while ((tmp = next()) != null) {
			last = tmp;
		}
		return last;
	}
	
	private void swap() {
		DataRecord tmp = rec[CURRENT];
		rec[CURRENT] = rec[NEXT];
		rec[NEXT] = tmp;
	}

	@Override
	public RecordKey getKey() {
		return key;
	}

	@Override
	public int compare(IInputReader other) {
		DataRecord rec1 = getSample();
		DataRecord rec2 = other.getSample();
		if (rec1 == null) {
			return 1;// null is greater than any other reader
//			return rec2 == null ? 0 : 1;	
		} else if (rec2 == null) {
			return -1;
		}
		return lastCompare = key.compare(other.getKey(), rec1, rec2);
	}

	@Override
	public boolean hasData() {
		return rec[NEXT] != null;
	}		

	@Override
	public String toString() {
		return getSample().toString();
	}

	@Override
	public InputOrdering getOrdering() {
		return inputOrdering;
	}
}