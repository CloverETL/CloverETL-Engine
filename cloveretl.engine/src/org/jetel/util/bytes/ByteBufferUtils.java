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
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

	/** Max size of integer encoded via encodeLength in bytes. */
	public static final int SIZEOF_INT = 5;
	
	/**
	 * This method flushes the buffer (data from the begging of buffer to position) 
	 * 	to the Channel and prepares it for next reading
	 * 
	 * @param buffer
	 * @param writer
	 * @return The number of bytes written, possibly zero
	 * @throws IOException
	 */
	public static int flush(ByteBuffer buffer, WritableByteChannel writer)
			throws IOException {
		int write = 0;
		buffer.flip();
		write = writer.write(buffer);
		buffer.clear();
		return write;
	}
	
	/**
	 * This method reads new data to the buffer. The bytes between the buffer's 
	 * current position and its limit are copied to the beginning of the buffer 
	 * and new bytes are read after them. Upon return the buffer's position will 
	 * be equal to p + n, where p is number of compacted bytes an n - number of 
	 * read bytes; its limit will be set to capacity. 
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
//    	if (Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE == 0){
//    	    EngineInitializer.initEngine(null, null, null);
//    	}
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
    
    /**
     * Encodes length (int value) into set of bytes occupying
     * least space
     * 
     * @param buffer    ByteBuffer to which encode length
     * @param length    value which should be encoded
     * @return          number of bytes (in buffer) needed to encode the length
     * @since 21.11.2006
     */
    
    public static final void encodeLength(ByteBuffer buffer,int length){
        if (length <= Byte.MAX_VALUE) {
            buffer.put((byte) length);
        } else {

            do {
                buffer.put((byte) (0x80 | (byte) length));
                length = length >> 7;
            } while ((length >> 7) > 0);
            buffer.put((byte) length);
        }
    }
    
    /**
     * Decode previously encoded length (int value)
     * 
     * @param buffer    ByteBuffer from which decode values
     * @return
     * @since 21.11.2006
     * @see org.jetel.util.ByteBufferUtils.encodeLength()
     */
    public static final int decodeLength(ByteBuffer buffer){
        int length=0; 
        int size;
        int offset = 0;
        
        size = buffer.get();
        if (size>0){
            return size;
        }
        
        while(size<0) {
           length = length | ((size & 0x7F) << (offset));
           offset+=7;
           size = buffer.get();
        }
        length = length | ((size & 0x7F) << (offset));
        
       return length;
    }
    
    /**
     * Returns how many bytes are needed to encode
     * length (value) using algorithm above
     * 
     * @param length
     * @return
     * @since 8.12.2006
     */
    public static final int lengthEncoded(int length){
        int count=0; 
            do {
                count++;
                length = length >> 7;
            } while (length > 0);
        return count;
    }
    
    /**
     * Decodes the value stored in the byte buffer using the specified byte order.
     * 
     * @param byteBuffer source buffer
     * @param byteOrder
     * 
     * @return decoded value of the byte buffer
     */
    public static BigInteger decodeValue(ByteBuffer byteBuffer, ByteOrder byteOrder) {
    	byte[] bytes = new byte[byteBuffer.remaining()];
		if (byteOrder == ByteOrder.BIG_ENDIAN) {
			 // no backing array, read all the remaining bytes
			byteBuffer.get(bytes);
		} else {
			// read the bytes backwards
			int lastPosition = byteBuffer.limit() - 1;
			for(int i = 0; i < bytes.length; i++) {
				bytes[i] = byteBuffer.get(lastPosition - i);
			}
		}
    	
    	return new BigInteger(bytes);
    }
    
    /**
     * Convenience method, see {@link #encodeValue(ByteBuffer, BigInteger, ByteOrder, int)}. 
     */
    public static int encodeValue(ByteBuffer byteBuffer, long value, ByteOrder byteOrder, int minLength) {
    	return encodeValue(byteBuffer, BigInteger.valueOf(value), byteOrder, minLength);
    }
    
    /**
     * Encodes the value into the byte buffer using the specified byte order.
     * <code>minLength</code> can be used to specify padding, otherwise set it to 0.
     * No check for maximum length is performed. 
     * 
     * @param byteBuffer target buffer
     * @param value the value to convert to bytes
     * @param byteOrder {@link ByteOrder#BIG_ENDIAN} or {@link ByteOrder#LITTLE_ENDIAN}
     * @param minLength minimum length, used for padding 
     * 
     * @return number of bytes encoding the value
     */
    public static int encodeValue(ByteBuffer byteBuffer, BigInteger value, ByteOrder byteOrder, int minLength) {
    	byte[] bytes = value.toByteArray();
    	
    	byte paddingByte = (value.signum() < 0) ? (byte) 0xFF : 0x00;
    	
    	if (byteOrder == ByteOrder.BIG_ENDIAN) {
    		// padding bits first
    		for (int i = bytes.length; i < minLength; i++) {
    			byteBuffer.put(paddingByte);
    		}
    		// then value bits
    		byteBuffer.put(bytes);
    	} else {
    		// value bits first 
    		for (int i = bytes.length - 1; i >= 0; i--) {
    			byteBuffer.put(bytes[i]);
    		}
    		// then padding bits
    		for (int i = bytes.length; i < minLength; i++) {
    			byteBuffer.put(paddingByte);
    		}
    	}
    	
    	return Math.max(minLength, bytes.length); 
    }
    
}
