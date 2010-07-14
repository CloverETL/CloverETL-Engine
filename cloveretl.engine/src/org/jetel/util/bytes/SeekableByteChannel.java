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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * In future this class should be replaced by SeekableByteChannel from JDK 1.7.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 2.12.2009
 */
public interface SeekableByteChannel extends ReadableByteChannel, WritableByteChannel {

	/**
	 * Returns this channel's position.
	 * @return
	 */
	public long position() throws IOException;
	 
	/**
	 * Sets this channel's position.
	 * @param newPosition
	 * @return
	 */
	public SeekableByteChannel position(long newPosition) throws IOException;
	
	/**
	 * Returns the current size of entity to which this channel is connected.
	 * @return
	 */
	public long size() throws IOException; 
     
	/**
	 * Truncates the entity, to which this channel is connected, to the given size.
	 * @param size
	 * @return
	 */
	public SeekableByteChannel truncate(long size) throws IOException;
	
}
