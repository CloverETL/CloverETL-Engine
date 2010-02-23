/*
 *    jETeL/Clover - Java based ETL application framework.
 *    Copyright (c) Opensys TM by Javlin, a.s. (www.opensys.com)
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
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
 *
 */
package org.jetel.util.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Simple routines for streams manipulating.
 *
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) Opensys TM by Javlin, a.s. (www.cloveretl.com)
 *
 * @created 23.11.2009
 */
public class StreamUtils {

	/**
	 * Private constructor.
	 */
	private StreamUtils() {
		//unreachable code
	}
	
    /**
     * Read data from an InputStream and convert it to a String.
     * @param is data source
     * @param charset charset for input byte stream
     * @param closeStream should be the given stream closed on exit?
     * @return string representation of given input stream
     * @throws IOException
     */
    public static String convertStreamToString(InputStream is, String charset, boolean closeStream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset));
        StringBuilder sb = new StringBuilder();

        int ch;
        try {
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
        } finally {
        	if (closeStream) {
        		is.close();
        	}
        }

        return sb.toString();
    }
    
    /**
     * Read all available bytes from one channel and copy them to the other.
     * @param in
     * @param out
     * @throws IOException
     */
    public static void copy(ReadableByteChannel in, WritableByteChannel out) throws IOException {
    	// First, we need a buffer to hold blocks of copied bytes.
    	ByteBuffer buffer = ByteBuffer.allocateDirect(32 * 1024);

    	// Now loop until no more bytes to read and the buffer is empty
    	while (in.read(buffer) != -1 || buffer.position() > 0) {
    		// The read() call leaves the buffer in "fill mode". To prepare
    		// to write bytes from the bufferwe have to put it in "drain mode"
    		// by flipping it: setting limit to position and position to zero
    		buffer.flip();

    		// Now write some or all of the bytes out to the output channel
    		out.write(buffer);

    		// Compact the buffer by discarding bytes that were written,
    		// and shifting any remaining bytes. This method also
    		// prepares the buffer for the next call to read() by setting the
    		// position to the limit and the limit to the buffer capacity.
    		buffer.compact();
    	}
    }

    private static final int IO_BUFFER_SIZE = 4 * 1024;  

    /**
     * Read all available bytes from one stream and copy them to the other.
     * @param in
     * @param out
     * @throws IOException
     */
    public static void copy(InputStream in, OutputStream out) throws IOException {
    	byte[] b = new byte[IO_BUFFER_SIZE];
    	int read;
    	while ((read = in.read(b)) != -1) {
    		out.write(b, 0, read);
    	}
    }

}
