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
import java.nio.CharBuffer;
import java.nio.InvalidMarkException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;

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
	public static int flush(ByteBuffer buffer, WritableByteChannel writer) throws IOException {
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
    public static void rewrite(InputStream in, OutputStream out) throws IOException {
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
    
    public static final int encodeLength(CloverBuffer buffer,int length) {
//    	buffer.putInt(length);
    	int bytes=0;
        if (length <= Byte.MAX_VALUE) {
            buffer.put((byte) length);
            bytes++;
        } else {

            do {
                buffer.put((byte) (0x80 | (byte) length));
                bytes++;
                length = length >> 7;
            } while ((length >> 7) > 0);
            buffer.put((byte) length);
            bytes++;
        }
        return bytes;
    }

    /**
     * Encodes length (positive int value) into set of bytes occupying
     * least space. Bytes are written directly to provided stream.
     * 
     * @param stream
     * @param length
     * @return number of bytes needed to encode the length and written to the stream 
     * @throws IOException
     */
    public static final int encodeLength(OutputStream stream,int length) throws IOException{
    	int bytes=0;
        if (length <= Byte.MAX_VALUE) {
        	stream.write((byte) length);
            bytes++;
        } else {

            do {
            	stream.write((byte) (0x80 | (byte) length));
                bytes++;
                length = length >> 7;
            } while ((length >> 7) > 0);
            stream.write((byte) length);
            bytes++;
        }
        return bytes;
    }
    
    /**
     * @deprecated use {@link #encodeLength(CloverBuffer, int)} instead
     */
    @Deprecated
    public static final void encodeLength(ByteBuffer buffer, int length) {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(buffer);
		encodeLength(wrappedBuffer, length);
		if (wrappedBuffer.buf() != buffer) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
    }

    /**
     * Encode the given string to the buffer. This method is symmetric with {@link #decodeString(CloverBuffer)}.
     * @param buffer
     * @param str
     */
    public static final void encodeString(CloverBuffer buffer, String str) {
		// encode nulls as zero, increment length of non-null values by one
    	if (str == null) {
    		ByteBufferUtils.encodeLength(buffer, 0);
    	} else {
    		final int length = str.length();
			ByteBufferUtils.encodeLength(buffer, length + 1);
	
			for (int counter = 0; counter < length; counter++) {
				buffer.putChar(str.charAt(counter));
			}
    	}
    }

    /**
     * Decode a string from the buffer. This method is symmetric with {@link #encodeString(CloverBuffer)}.
     * @param buffer
     * @return
     */
    public static final String decodeString(CloverBuffer buffer) {
		int length = ByteBufferUtils.decodeLength(buffer);
		if (length == 0) {
			return null;
		} else {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < length - 1; i++) {
				sb.append(buffer.getChar());
			}
			return sb.toString();
		}
    }
    
    /**
     * Returns how many bytes are needed to encode a string using algorithm 
     * in {@link #encodeString(CloverBuffer, String)} method.
     */
    public static final int lengthEncoded(String str) {
    	if (str == null) {
    		return 1;
    	} else {
    		final int length = str.length();
			return ByteBufferUtils.lengthEncoded(length + 1) + (2 * length);
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
    public static final int decodeLength(CloverBuffer buffer) {
//    	return buffer.getInt();
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
     * Decodes length (positive integer) from stream of bytes.
     * 
     * @param stream
     * @return Decoded length or -1 if end of stream was reached
     * @throws IOException
     */
    public static final int decodeLength(InputStream stream) throws IOException {
        int length=0; 
        byte size;
        int offset = 0;
        
        int value = stream.read();
        if (value==-1) return -1;
        
        size=(byte)value;
        if (size>0){
            return size;
        }
        
        while(size<0) {
           length = length | ((size & 0x7F) << (offset));
           offset+=7;
            value = stream.read();
            if (value==-1) return -1;
            size = (byte)value;
        }
        length = length | ((size & 0x7F) << (offset));
        
       return length;
    }
    
    
    /**
     * @deprecated use {@link #decodeLength(CloverBuffer)} instead
     */
    @Deprecated
    public static final int decodeLength(ByteBuffer buffer) {
		CloverBuffer wrappedBuffer = CloverBuffer.wrap(buffer);
		int result = decodeLength(wrappedBuffer);
		if (wrappedBuffer.buf() != buffer) {
			throw new JetelRuntimeException("Deprecated method invocation failed. Please use CloverBuffer instead of ByteBuffer.");
		}
		return result;
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
//    	return 4;
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
     * @param buffer source buffer
     * @param byteOrder
     * 
     * @return decoded value of the byte buffer
     */
    public static BigInteger decodeValue(CloverBuffer buffer, ByteOrder byteOrder) {
    	byte[] bytes = new byte[buffer.remaining()];
		if (byteOrder == ByteOrder.BIG_ENDIAN) {
			 // no backing array, read all the remaining bytes
			buffer.get(bytes);
		} else {
			// read the bytes backwards
			int lastPosition = buffer.limit() - 1;
			for(int i = 0; i < bytes.length; i++) {
				bytes[i] = buffer.get(lastPosition - i);
			}
		}
    	
    	return new BigInteger(bytes);
    }
    
    /**
     * Convenience method, see {@link #encodeValue(CloverBuffer, BigInteger, ByteOrder, int)}. 
     */
    public static int encodeValue(CloverBuffer buffer, long value, ByteOrder byteOrder, int minLength) {
    	return encodeValue(buffer, BigInteger.valueOf(value), byteOrder, minLength);
    }
    
    /**
     * Encodes the value into the byte buffer using the specified byte order.
     * <code>minLength</code> can be used to specify padding, otherwise set it to 0.
     * No check for maximum length is performed. 
     * 
     * @param buffer target buffer
     * @param value the value to convert to bytes
     * @param byteOrder {@link ByteOrder#BIG_ENDIAN} or {@link ByteOrder#LITTLE_ENDIAN}
     * @param minLength minimum length, used for padding 
     * 
     * @return number of bytes encoding the value
     */
    public static int encodeValue(CloverBuffer buffer, BigInteger value, ByteOrder byteOrder, int minLength) {
    	byte[] bytes = value.toByteArray();
    	
    	byte paddingByte = (value.signum() < 0) ? (byte) 0xFF : 0x00;
    	
    	if (byteOrder == ByteOrder.BIG_ENDIAN) {
    		// padding bits first
    		for (int i = bytes.length; i < minLength; i++) {
    			buffer.put(paddingByte);
    		}
    		// then value bits
    		buffer.put(bytes);
    	} else {
    		// value bits first 
    		for (int i = bytes.length - 1; i >= 0; i--) {
    			buffer.put(bytes[i]);
    		}
    		// then padding bits
    		for (int i = bytes.length; i < minLength; i++) {
    			buffer.put(paddingByte);
    		}
    	}
    	
    	return Math.max(minLength, bytes.length); 
    }

    /**
     * Fills the ByteBuffer with defined value. Does not change buffer's current position.
     * @param buffer buffer to be filled
     * @param value byte valued to be used
     * @param count how many bytes to be filled
     * @param advance whether buffer's current position should be moved
     */
    public static void fill(ByteBuffer buffer, byte value,int count, boolean advance){
    	final int pos = buffer.position();
    	while(buffer.hasRemaining() && count>0){
    		buffer.put(value);
    		count--;
    	}
    	if (!advance) buffer.position(pos);
    }
    
    public static void fill(ByteBuffer buffer, byte value,int count){
    	fill(buffer,value,count,false);
    }
    
    /**
     * Creates deep copy of the given {@link CharBuffer} with at least requested capacity and at most maximum capacity.
     * The resulted deep copy has same internal state (position, limit, mark) as the given buffer. 
     * @param oldBuffer copied buffer
     * @param requestedCapacity the resulted buffer has at least this capacity
     * @param maximumCapacity the resulted buffer has at most this capacity
     * @return the deep copy of the given buffer with expanded capacity
     */
    public static CharBuffer expandCharBuffer(CharBuffer oldBuffer, int requestedCapacity, int maximumCapacity) {
    	if (requestedCapacity > maximumCapacity) {
    		throw new IllegalArgumentException("requested capacity cannot be bigger than maximum capacity");
    	}
        if (oldBuffer.capacity() < requestedCapacity) {
            // Allocate a new buffer and transfer all settings to it.
            //// Save the state.
            int oldPosition = oldBuffer.position();
            int oldLimit = oldBuffer.limit();
            int oldMark = -1;
            try {
            	oldMark = oldBuffer.reset().position();
            } catch (InvalidMarkException e) {
            	//DO NOTHING
            }

            //// Reallocate.
            int newCapacity = Math.min(CloverBuffer.normalizeCapacity(requestedCapacity), maximumCapacity);
            CharBuffer newBuffer = CharBuffer.allocate(newCapacity);
            oldBuffer.clear();
            newBuffer.put(oldBuffer);
            
            //// Restore the state in old buffer
            oldBuffer.limit(oldLimit);
            if (oldMark >= 0) {
                oldBuffer.position(oldMark);
                oldBuffer.mark();
            }
            oldBuffer.position(oldPosition);

            //// Restore the state in new buffer
            newBuffer.limit(oldLimit);
            if (oldMark >= 0) {
                newBuffer.position(oldMark);
                newBuffer.mark();
            }
            newBuffer.position(oldPosition);
        	
            return newBuffer;
        } else {
        	return oldBuffer;
        }
    }
    
    /**
     * Fills the ByteBuffer with defined value. Does not change buffer's current position.
     * @param buffer buffer to be filled
     * @param value byte valued to be used
     */
    public static void fill(ByteBuffer buffer, byte value){
    	final int pos = buffer.position();
    	while(buffer.hasRemaining()) buffer.put(value);
    	buffer.position(pos);
    }
}

