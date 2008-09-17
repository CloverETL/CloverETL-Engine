package org.jetel.data.reader;

import java.io.IOException;

import org.jetel.data.DataRecord;
import org.jetel.data.RecordKey;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.graph.InputPort;

/**
 * Reader for driver input. Doesn't use record buffer but also doesn't support rewind operation.
 * @author Jan Hadrava, Javlin Consulting (www.javlinconsulting.cz)
 *
 */
public class DriverReader implements InputReader {
	private static final int CURRENT = 0;
	private static final int NEXT = 1;

	private InputPort inPort;
	private RecordKey key;
	private DataRecord[] rec = new DataRecord[2];
	private int recCounter;
	private boolean blocked;
	
	private InputOrdering inputOrdering = InputOrdering.UNDEFINED;
	
	public DriverReader(InputPort inPort, RecordKey key) {
		this.inPort = inPort;
		this.key = key;
		this.rec[CURRENT] = new DataRecord(inPort.getMetadata());
		this.rec[NEXT] = new DataRecord(inPort.getMetadata());
		this.rec[CURRENT].init();
		this.rec[NEXT].init();
		recCounter = 0;
		blocked = false;
	}
	
	public void reset() throws ComponentNotReadyException {
		inPort.reset();
		this.rec[CURRENT] = new DataRecord(inPort.getMetadata());
		this.rec[NEXT] = new DataRecord(inPort.getMetadata());
		this.rec[CURRENT].init();
		this.rec[NEXT].init();
		recCounter = 0;
		blocked = false;
	}
	
	public void free() {
		inPort = null;
	}

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
			int comparison = key.compare(rec[CURRENT], rec[NEXT]);
			if (comparison != 0){
				inputOrdering = SlaveReader.updateOrdering(comparison, inputOrdering);
				break;
			}
		}
		return true;
	}

	public void rewindRun() {
		throw new UnsupportedOperationException();
	}

	public DataRecord getSample() {
		return blocked ? rec[CURRENT] : rec[NEXT];
	}

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
			blocked = key.compare(rec[CURRENT], rec[NEXT]) != 0;
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

	public RecordKey getKey() {
		return key;
	}
	
	public int compare(InputReader other) {
		DataRecord rec1 = getSample();
		DataRecord rec2 = other.getSample();
		if (rec1 == null) {
			return 1;// null is greater than any other reader
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