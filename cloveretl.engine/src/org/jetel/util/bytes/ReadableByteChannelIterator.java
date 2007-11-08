package org.jetel.util.bytes;

import java.nio.channels.ReadableByteChannel;
import java.util.Iterator;

/**
 * Readable byte channel iterator class. Returns channel for first call, then null. 
 * 
 * @author ausperger
 */
public class ReadableByteChannelIterator implements Iterator<ReadableByteChannel> {
	private ReadableByteChannel readableByteChannel;
	private boolean hasNext = true;
	
	public ReadableByteChannelIterator(ReadableByteChannel readableByteChannel) {
		this.readableByteChannel = readableByteChannel;
	}
	
	public boolean hasNext() {
		return hasNext;
	}

	public ReadableByteChannel next() {
		if(hasNext) {
			hasNext = false;
			return readableByteChannel;
		} else { 
			return null;
		}
	}

	public void remove() {}
}
