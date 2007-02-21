package org.jetel.util;

import java.nio.channels.WritableByteChannel;
import java.util.Iterator;

/**
 * Channel iterator class.
 * It is used in MultiFileWriter.
 * 
 * @author ausperger
 */
public class WritableByteChannelIterator implements Iterator<WritableByteChannel> {
	private WritableByteChannel writableByteChannel;
	
	public WritableByteChannelIterator(WritableByteChannel writableByteChannel) {
		this.writableByteChannel = writableByteChannel;
	}
	
	public boolean hasNext() {
		return true;
	}

	public WritableByteChannel next() {
		return writableByteChannel;
	}

	public void remove() {}
}
