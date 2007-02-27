package org.jetel.util;

import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 * Channel iterator class. Returns channel for first call, then null. 
 * It is used in MultiFileWriter.
 * 
 * @author ausperger
 */
public class WritableByteChannelIterator implements Iterator<WritableByteChannel> {
	private WritableByteChannel writableByteChannel;
	private boolean hasNext = true;
	
	public WritableByteChannelIterator(WritableByteChannel writableByteChannel) {
		this.writableByteChannel = writableByteChannel;
	}
	
	public boolean hasNext() {
		return hasNext;
	}

	public WritableByteChannel next() {
		if(hasNext) {
			hasNext = false;
			return writableByteChannel;
		} else { 
			return null;
		}
	}

	public void remove() {}
}
