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
/*
 * Javolution - Java(TM) Solution for Real-Time and Embedded Systems
 * Copyright (C) 2012 - Javolution (http://javolution.org/)
 * All rights reserved.
 * 
 * Permission to use, copy, modify, and distribute this software is
 * freely granted, provided that this notice is preserved.
 */
package org.jetel.util.bytes;

import java.io.CharConversionException;
import java.io.IOException;
import java.io.Writer;
import java.nio.CharBuffer;

import javolution.io.UTF8ByteBufferReader;

/**
 * <p> A UTF-8 <code>java.nio.ByteBuffer</code> writer.</p>
 *
 * <p> This writer supports surrogate <code>char</code> pairs (representing
 *     characters in the range [U+10000 .. U+10FFFF]). It can also be used
 *     to write characters from their unicodes (31 bits) directly
 *     (ref. {@link #write(int)}).</p>
 *
 * <p> Instances of this class can be reused for different output streams
 *     and can be part of a higher level component (e.g. serializer) in order
 *     to avoid dynamic buffer allocation when the destination output changes.
 *     Also wrapping using a <code>java.io.BufferedWriter</code> is unnescessary
 *     as instances of this class embed their own data buffers.</p>
 * 
 * <p> Note: This writer is unsynchronized and always produces well-formed
 *           UTF-8 sequences.</p>
 *
 * @author  <a href="mailto:jean-marie@dautelle.com">Jean-Marie Dautelle</a>
 * @version 2.0, December 9, 2004
 * @see     UTF8ByteBufferReader
 * 
 * Adapted by D.Pavlis <info@cloveretl.com> to work with CloverByteBuffer
 */
public final class UTF8CloverBufferWriter extends Writer {

    /**
     * Holds the byte buffer destination.
     */
    private CloverBuffer _byteBuffer;

    /**
     * Default constructor.
     */
    public UTF8CloverBufferWriter() {}

    /**
     * Sets the byte buffer to use for writing until this writer is closed.
     *
     * @param  byteBuffer the destination byte buffer.
     * @return this UTF-8 writer.
     * @throws IllegalStateException if this writer is being reused and 
     *         it has not been {@link #close closed} or {@link #reset reset}.
     */
    public UTF8CloverBufferWriter setOutput(CloverBuffer byteBuffer) {
        if (_byteBuffer != null)
            throw new IllegalStateException("Writer not closed or reset");
        _byteBuffer = byteBuffer;
        return this;
    }

    /**
     * Writes a single character. This method supports 16-bits
     * character surrogates.
     *
     * @param  c <code>char</code> the character to be written (possibly
     *        a surrogate).
     * @throws IOException if an I/O error occurs.
     */
    public void write(char c) throws IOException {
        if ((c < 0xd800) || (c > 0xdfff)) {
            write((int) c);
        } else if (c < 0xdc00) { // High surrogate.
            _highSurrogate = c;
        } else { // Low surrogate.
            int code = ((_highSurrogate - 0xd800) << 10) + (c - 0xdc00)
                    + 0x10000;
            write(code);
        }
    }

    private char _highSurrogate;

    /**
     * Writes a character given its 31-bits Unicode.
     *
     * @param  code the 31 bits Unicode of the character to be written.
     * @throws IOException if an I/O error occurs.
     */
    @Override
	public void write(int code) throws IOException {
        if ((code & 0xffffff80) == 0) {
            _byteBuffer.put((byte) code);
        } else { // Writes more than one byte.
            write2(code);
        }
    }

    private void write2(int c) throws IOException {
        if ((c & 0xfffff800) == 0) { // 2 bytes.
            _byteBuffer.put((byte) (0xc0 | (c >> 6)));
            _byteBuffer.put((byte) (0x80 | (c & 0x3f)));
        } else if ((c & 0xffff0000) == 0) { // 3 bytes.
            _byteBuffer.put((byte) (0xe0 | (c >> 12)));
            _byteBuffer.put((byte) (0x80 | ((c >> 6) & 0x3f)));
            _byteBuffer.put((byte) (0x80 | (c & 0x3f)));
        } else if ((c & 0xff200000) == 0) { // 4 bytes.
            _byteBuffer.put((byte) (0xf0 | (c >> 18)));
            _byteBuffer.put((byte) (0x80 | ((c >> 12) & 0x3f)));
            _byteBuffer.put((byte) (0x80 | ((c >> 6) & 0x3f)));
            _byteBuffer.put((byte) (0x80 | (c & 0x3f)));
        } else if ((c & 0xf4000000) == 0) { // 5 bytes.
            _byteBuffer.put((byte) (0xf8 | (c >> 24)));
            _byteBuffer.put((byte) (0x80 | ((c >> 18) & 0x3f)));
            _byteBuffer.put((byte) (0x80 | ((c >> 12) & 0x3f)));
            _byteBuffer.put((byte) (0x80 | ((c >> 6) & 0x3f)));
            _byteBuffer.put((byte) (0x80 | (c & 0x3f)));
        } else if ((c & 0x80000000) == 0) { // 6 bytes.
            _byteBuffer.put((byte) (0xfc | (c >> 30)));
            _byteBuffer.put((byte) (0x80 | ((c >> 24) & 0x3f)));
            _byteBuffer.put((byte) (0x80 | ((c >> 18) & 0x3f)));
            _byteBuffer.put((byte) (0x80 | ((c >> 12) & 0x3F)));
            _byteBuffer.put((byte) (0x80 | ((c >> 6) & 0x3F)));
            _byteBuffer.put((byte) (0x80 | (c & 0x3F)));
        } else {
            throw new CharConversionException("Illegal character U+"
                    + Integer.toHexString(c));
        }
    }

    /**
     * Writes a portion of an array of characters.
     *
     * @param  cbuf the array of characters.
     * @param  off the offset from which to start writing characters.
     * @param  len the number of characters to write.
     * @throws IOException if an I/O error occurs.
     */
    @Override
	public void write(char cbuf[], int off, int len) throws IOException {
        final int off_plus_len = off + len;
        for (int i = off; i < off_plus_len;) {
            char c = cbuf[i++];
            if (c < 0x80) {
                _byteBuffer.put((byte) c);
            } else {
                write(c);
            }
        }
    }

    /**
     * Writes a portion of a string.
     *
     * @param  str a String.
     * @param  off the offset from which to start writing characters.
     * @param  len the number of characters to write.
     * @throws IOException if an I/O error occurs
     */
    @Override
	public void write(String str, int off, int len) throws IOException {
        final int off_plus_len = off + len;
        for (int i = off; i < off_plus_len;) {
            char c = str.charAt(i++);
            if (c < 0x80) {
                _byteBuffer.put((byte) c);
            } else {
                write(c);
            }
        }
    }

    /**
     * Writes the specified character sequence.
     *
     * @param  csq the character sequence.
     * @throws IOException if an I/O error occurs
     */
    public void write(CharSequence csq) throws IOException {
        final int length = csq.length();
        for (int i = 0; i < length;) {
            char c = csq.charAt(i++);
            if (c < 0x80) {
                _byteBuffer.put((byte) c);
            } else {
                write(c);
            }
        }
    }

    
    /**
     * Writes the specified CharBuffer
     * 
     * @param string
     * @throws IOException
     */
    public void write(CharBuffer string) throws IOException {
    	 while (string.hasRemaining()) {
             char c = string.get();
             if (c < 0x80) {
                 _byteBuffer.put((byte) c);
             } else {
                 write(c);
             }
         }
    }
    
    
    /**
     * Flushes the stream (this method has no effect, the data is 
     * always directly written to the <code>ByteBuffer</code>).
     *
     * @throws IOException if an I/O error occurs.
     */
    @Override
	public void flush() throws IOException {
        if (_byteBuffer == null) { throw new IOException("Writer closed"); }
    }

    /**
     * Closes and {@link #reset resets} this writer for reuse.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
	public void close() throws IOException {
        if (_byteBuffer != null) {
            reset();
        }
    }

    // Implements Reusable.
    public void reset() {
        _byteBuffer = null;
        _highSurrogate = 0;
    }

}