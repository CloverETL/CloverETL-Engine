/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2005-05  Javlin Consulting <info@javlinconsulting.cz>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
*/

package org.jetel.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * This class provides static methods for working with ByteBuffer in association
 *  with Channels
 * 
 * @author avackova (agata.vackova@javlinconsulting.cz) ; 
 * (c) JavlinConsulting s.r.o.
 *  www.javlinconsulting.cz
 *
 * @since Oct 20, 2006
 *
 */
public class ByteBufferUtils {
	
	/**
	 * This method flushes the buffer to the Channel and prepares it for next
	 *  reading
	 * 
	 * @param buffer
	 * @param writer
	 * @return The number of bytes written, possibly zero
	 * @throws IOException
	 */
	public static int flush(ByteBuffer buffer, WritableByteChannel writer)
			throws IOException {
		int write;
		buffer.flip();
		write = writer.write(buffer);
		buffer.clear();
		return write;
	}
	
	/**
	 * This method reads new data to the buffer 
	 * 
	 * @param buffer
	 * @param reader
	 * @return The number of bytes written, possibly zero
	 * @throws IOException
	 */
	public static int reload(ByteBuffer buffer, ReadableByteChannel reader) throws IOException{
		int read;
		if (buffer.position() != 0) {
			buffer.compact();
		}
		read = reader.read(buffer);
		buffer.flip();
		return read;
	}

}
