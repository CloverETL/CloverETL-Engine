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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.Defaults;

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
public final class ByteBufferUtils {
	
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
		if (buffer.position() != 0) {
			buffer.flip();
		}
		write = writer.write(buffer);
		buffer.clear();
		return write;
	}
	
	/**
	 * This method reads new data to the buffer 
	 * 
	 * @param buffer
	 * @param reader
	 * @return The number of bytes read, possibly zero
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

    /**
     * This method rewrites bytes from input stream to output stream
     * 
     * @param in
     * @param out
     * @throws IOException
     */
    public static void rewrite(InputStream in, OutputStream out)throws IOException{
    	ByteBuffer buffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
    	ReadableByteChannel reader = Channels.newChannel(in);
    	WritableByteChannel writer = Channels.newChannel(out);
    	while (reload(buffer,reader) > 0){
    		flush(buffer, writer);
    	}
    }
	
    /**
     * This method rewrites maximum "bytes" bytes from input stream to output stream
     * 
     * @param in
     * @param out
     * @param bytes number of bytes to rewrite
     * @throws IOException
     */
    public static void rewrite(InputStream in, OutputStream out, long bytes)throws IOException{
    	if (Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE == 0){
    		Defaults.init();
    	}
    	ByteBuffer buffer = ByteBuffer.allocateDirect(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE);
    	ReadableByteChannel reader = Channels.newChannel(in);
    	WritableByteChannel writer = Channels.newChannel(out);
    	long b = 0;
    	int r;
     	while ( (r = reload(buffer,reader)) > 0 && b < bytes){
    		b += r;
    		if (r == buffer.capacity()) {
    			flush(buffer, writer);
    		}else{
    			buffer.limit((int) (bytes % buffer.capacity()));
    			flush(buffer, writer);
    		}
    	}
    }
}
