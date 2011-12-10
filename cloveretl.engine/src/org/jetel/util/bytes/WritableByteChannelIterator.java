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
package org.jetel.util.bytes;

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
	
	@Override
	public boolean hasNext() {
		return hasNext;
	}

	@Override
	public WritableByteChannel next() {
		if(hasNext) {
			hasNext = false;
			return writableByteChannel;
		} else { 
			return null;
		}
	}

	@Override
	public void remove() {}
}
