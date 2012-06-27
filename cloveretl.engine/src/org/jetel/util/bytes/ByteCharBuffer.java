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
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.util.HashMap;

import org.jetel.data.Defaults;
import org.jetel.exception.BadDataFormatException;

/**
 * <h1>ByteCharBuffer</h1>
 * 
 * <p>
 * This class represents a character buffer that can seamlessly read characters
 * from underlying byte source (file) and decode them using a CharsetDecoder.
 * </p>
 * 
 * <p>This is very similar to use of InputStreamReader with an explict charset,
 * but uses faster java.nio. This was it can achieve roughly 50-100% performance gain
 * compared to InputStreamReader. Additionally this buffer can be accessed randomly character-wise (seek)
 * </p> 
 * 
 * <p>
 * The underlying implementation uses java.nio file channels, ByteBuffer and
 * CharBuffer
 * </p>
 * 
 * <p>
 * This buffer can be used in two functionally identical ways:
 * </p>
 * <ul>
 * <li><b>ByteCharBuffer.get() methods</b><br/>
 * <p>
 * Using this way is simple and preferred. Each call to get() method reads one
 * (or more) characters from the beginning until the very end of file. User does
 * not need to deal with buffer underflows and other problems.
 * </p>
 * <p>
 * Example:<br/>
 * 
 * <pre>
 * ByteCharBuffer buffer = new ByteCharBuffer();
 * buffer.setSource(Channels.newChannel(new FileInputStream(&quot;file.txt&quot;)), &quot;UTF-8&quot;); // note the charset name in the end
 * 
 * int c;
 * char ch;
 * 
 * while ((c = buffer.get()) != -1) {
 * 	ch = (char) c; // work with ch
 * }
 * // this is EOF
 * </pre>
 * 
 * </li>
 * <li><b>Direct access to CharBuffer with exception handler</b>
 * <p>
 * You can also work directly with underlying CharBuffer and use convenience
 * method handleException to handle buffer underflows.
 * </p>
 * <p>
 * Example:
 * </p>
 * 
 * <pre>
 * ByteCharBuffer buffer = new ByteCharBuffer();
 * buffer.setSource(Channels.newChannel(new FileInputStream(&quot;file.txt&quot;)), &quot;UTF-8&quot;);
 * 
 * CharBuffer chb = buffer.getCharBuffer();
 * boolean terminated = false;
 * while (!terminated) {
 * 	try {
 * 		chb.read(); // read characters directly from CharBuffer
 * 		process(); // process the characters
 * 	} catch (BufferUnderFlowException ex) {
 * 		terminated = !byteCharBuffer.handleException(ex);
 * 	}
 * }
 * </pre>
 * 
 * </li>
 * </ul>
 * 
 * <p>
 * This class has an additional feature in the ability to remember and restore
 * its position in the file. This is done via getPosition()/setPosition()
 * methods using a special position object ByteCharPosition.
 * </p>
 * 
 * @author pnajvar
 * @since May 2009
 * 
 */
public class ByteCharBuffer {

	/*
	 * Underlying buffer that stores current decoded characters
	 */
	CharBuffer charBuffer;
	/*
	 * Underlying buffer that is used for reading bytes from input file
	 */
	ByteBuffer byteBuffer;
	/*
	 * A decoder that decodes bytes (byteBuffer) into characters (charBuffer)
	 */
	CharsetDecoder decoder;
	/*
	 * Input file
	 */
	ReadableByteChannel reader;
	/*
	 * tells whether EOF has been reached
	 */
	boolean eof;
	/*
	 * Internal state of the decoder
	 */
	CoderResult result;
	/*
	 * Number of bytes that need to be skipped to get current charBuffer Used in
	 * getPosition() method
	 */
	long byteBufferBase;
	/*
	 * charBufferBase represents absolute character index of current CharBuffer in the file
	 * Used in getPosition() method
	 */
	long charBufferBase;
	/*
	 * This is a correction shift between real `charBufferBase` (defined by
	 * number of bytes) and current char buffer content - which might be shifted
	 * forwards because of (multiple) compacting
	 */
	int charBufferBaseShift = 0;
	/*
	 * Number of all decoded characters so far
	 */
	long readChars;
	/*
	 * Number of all bytes read and decoded so far
	 */
	long readBytes;
	/*
	 * marked positions
	 */
	HashMap<Object, ByteCharPosition> marks = null;

	public ByteCharBuffer() {
		setDecoder(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
	}

	public ReadableByteChannel getReader() {
		return reader;
	}

	/**
	 * This method should be protected USE setSource() instead!!!!!
	 * 
	 * @param reader
	 */
	public void setReader(ReadableByteChannel reader) {
		this.reader = reader;
		this.eof = false;
		init();
		reset();
		inited = true;
	}

	/**
	 * Sets the decoder by charset name USE setSource() rather
	 * 
	 * @param charsetName
	 *            Name of the charset (see Charset.forName())
	 */
	public void setDecoder(String charsetName) {
		setDecoder(Charset.forName(
				charsetName != null ? charsetName : Charset.defaultCharset()
						.name()).newDecoder());
	}

	/**
	 * Sets the decoder by charset name USE setSource() rather
	 * 
	 * @param decoder
	 *            A decoder
	 */
	public void setDecoder(CharsetDecoder decoder) {
		this.decoder = decoder;
	}

	/**
	 * Sets source file This is a preferred method
	 * 
	 * @param reader
	 *            A ReadableByteChannel object representing the file
	 * @param charset
	 *            Charset name
	 */
	public void setSource(ReadableByteChannel reader, String charset) {
		setReader(reader);
		setDecoder(charset);
	}

	/**
	 * Sets source file This is a preferred method
	 * 
	 * @param reader
	 *            A ReadableByteChannel object representing the file
	 * @param decoder
	 *            A charset decoder
	 */
	public void setSource(ReadableByteChannel reader, CharsetDecoder decoder) {
		setReader(reader);
		setDecoder(decoder);
	}

	/**
	 * Initializes this object with default values
	 */
	public void init() {
		int bufferSize =
			(Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE > 0)
			? Defaults.DEFAULT_INTERNAL_IO_BUFFER_SIZE
			: 8192;  
		init(bufferSize, bufferSize);
	}

	boolean inited = false;

	/**
	 * Initializes this object with custom buffer sizes
	 * 
	 * init() must be called prior to calling setSource() !!
	 * 
	 * Both buffer sizes can be arbitrary (but reasonable). Keeping the sizes at
	 * maximum values improves performance.
	 * 
	 * @param charBufferSize
	 *            Size of the underlying CharBuffer (in characters)
	 * @param byteBufferSize
	 *            Size of the underlying read ByteBuffer (in bytes);
	 */
	public void init(int charBufferSize, int byteBufferSize) {
		if (inited) {
			return;
		}

		if (charBuffer == null || charBuffer.capacity() != charBufferSize) {
			charBuffer = CharBuffer.allocate(charBufferSize);
		}
		if (byteBuffer == null || byteBuffer.capacity() != byteBufferSize) {
			byteBuffer = ByteBuffer.allocateDirect(byteBufferSize);
		}
		reset();
		inited = true;
	}

	/**
	 * This method fills the CharBuffer when it runs out of data. It reads more
	 * bytes from the file if necessary and decodes them
	 * 
	 * @return Number of characters that were read (0 meaing no more characters read)
	 * @throws IOException
	 *             File exception
	 */
	public int readChars() throws IOException, BadDataFormatException {

		if (eof) {
			return 0;
		}
		
		// compacting the charBuffer moves its base by number of discarded chars
		// even if no bytes are decoded
		charBufferBaseShift += charBuffer.position();
		charBuffer.compact();

		if (charBuffer.position() == 0) {
			// the whole charBuffer is consumed
			// we can say, that number of bytes read is number of characters
			// consumed
			this.charBufferBase = this.readChars;
			this.byteBufferBase = this.readBytes;
			this.charBufferBaseShift = 0;
		}

		int cpos = charBuffer.position(); // this is usually 0, but to be safe
											// when readChars() is called
											// redundantly
		int bpos = byteBuffer.position();
		int readChars = 0;

		do {
			// try flushing the decoder, if eof
			if (eof) {
				result = decoder.flush(charBuffer);
				// if something was there to flush, then the result will be
				// positive
				// otherwise zero meaning no characters were read
				cpos = charBuffer.position() - cpos;
				charBuffer.flip();
				return cpos;
			}

			if (result == CoderResult.UNDERFLOW) {
				byteBuffer.compact();
				// read characters info byteBuffer
				eof = reader.read(byteBuffer) == -1;
				byteBuffer.flip();
				bpos = byteBuffer.position();
			}
			cpos = charBuffer.position();
			result = decoder.decode(byteBuffer, charBuffer, eof);

			if (result.isError()) {
				throw new BadDataFormatException("Decoding error at "
						+ getPosition() + ": " + result.toString());
			}
			// `readBytes` represents current number of bytes that have been
			// decoded into characters
			// `readChars` represents current number of decoded characters
			this.readBytes += byteBuffer.position() - bpos;

			readChars += charBuffer.position() - cpos;
			this.readChars += charBuffer.position() - cpos;

		} while (!eof && result == CoderResult.UNDERFLOW);

		charBuffer.flip();

		return readChars;
	}

	/**
	 * Closes the file
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		if (this.reader != null) {
			this.reader.close();
		}
		reset();
	}

	/**
	 * Resets this buffer
	 */
	public void reset() {
		decoder.reset();
		if (charBuffer != null) {
			charBuffer.clear();
			charBuffer.flip();
		}
		if (byteBuffer != null) {
			byteBuffer.clear();
			byteBuffer.flip();
		}
		byteBufferBase = 0;
		charBufferBase = 0;
		charBufferBaseShift = 0;
		readChars = 0;
		readBytes = 0;
		result = CoderResult.UNDERFLOW;
		if (reader != null && (reader instanceof FileChannel)) {
			try {
				if (reader.isOpen()) {
					((FileChannel) reader).position(0);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		inited = false;
		eof = false;
	}

	/**
	 * Returns the offset of current char buffer position to the
	 * `charBufferBase` This offset does not necessarily must be smaller than
	 * capacity of the CharBuffer as the CharBuffer base might be shifted.
	 * 
	 * @return
	 */
	public int getCharOffset() {
		return this.charBuffer.position() + charBufferBaseShift;
	}

	/**
	 * Returns the absolute index of current character
	 * @return
	 */
	public long getCharIndex() {
		return getPosition().getCharIndex();
	}
	
	/**
	 * Returns remaining characters until next read will be necessary
	 * @return
	 */
	public int remaining() {
		return charBuffer.remaining();
	}

	/**
	 * Return current position in CharBuffer
	 * @return
	 */
	public int position() {
		return charBuffer.position();
	}
	
	/**
	 * Sets new position in CharBuffer
	 * @param n
	 */
	public void position(int n) {
		charBuffer.position(n);
	}
	
	/**
	 * Ensures that (at least) n characters is in the CharBuffer for immediate direct access.
	 * Returns the number of characters that could be provided.
	 * 
	 * Return value less than `n` means failure to ensure requested number of characters.
	 * 
	 * @param n Number of characters needed in CharBuffer
	 * @return Number of available characters in CharBuffer
	 */
	public int ensure(int n) { 
		if (n - remaining() > 0) {
			try {
				readChars();
			} catch (BadDataFormatException e) {
				return -1;
			} catch (IOException e) {
				return -1;
			}
		}
		return remaining();
	}
	
	/**
	 * <p>Relative get method. Returns next character in source file/buffer.
	 * Automatically loads and decodes additional characters if needed.</p>
	 * 
	 * <p>This is a method of choice for char-by-char reading of the whole file.</p>
	 * 
	 * <p><pre>
	 * int c;
	 * while( (c = byteCharBuffer.get()) != -1) {
	 *    process( (char)c );
	 * } // eof
	 * </pre></p>
	 * 
	 * @return A char value or -1 for end of file
	 * @throws IOException
	 */
	public int get() throws IOException {
		if (charBuffer.remaining() == 0) {
			if (readChars() <= 0) {
				close();
				return -1;
			}
		}
		return charBuffer.get();
	}

	/**
	 * Relative get method Returns the next character in source. Automatically
	 * loads and decodes additional characters.
	 * 
	 * @param dst
	 *            Destination char[] array
	 * @param dstStart
	 *            start index in destination array
	 * @param length
	 *            Number of characters to put to dst
	 * @return number of actually copies characters
	 * @throws IOException
	 */
	public int get(char[] dst, int dstStart, int length) throws IOException {
		if (dst == null) {
			return 0;
		}

		int cnt = 0;
		int c = 0;

		for (int i = 0; i < Math.min(length, dst.length - dstStart); i++) {
			c = get();
			if (c < 0) {
				return cnt;
			} else {
				dst[dstStart + i] = (char) c;
				cnt++;
			}
		}
		return cnt;
	}

	/**
	 * Shortcut method to read characters into entire `dst` array
	 * 
	 * @param dst
	 *            Destination char[] array
	 * @return number of characters stored into `dst`
	 * @throws IOException
	 */
	public int get(char[] dst) throws IOException {
		return get(dst, 0, dst.length);
	}

	/**
	 * Returns current position in this buffer Returned object can be in future
	 * used to restore the position in input file
	 * 
	 * @return Position object of current character
	 */
	public ByteCharPosition getPosition() {
		return new ByteCharPosition(this.byteBufferBase, this.charBufferBase,
				getCharOffset());
	}

	/**
	 * Moves to exact position denoted by `position` so that next call to get()
	 * method will produce exactly the same character as at the time when
	 * getPosition() was called.
	 * 
	 * @param position
	 *            A position object, usually obtained by getPosition()
	 */
	public void setPosition(ByteCharPosition position) throws IOException {

		if (position.byteBufferBase == this.byteBufferBase
				&& position.charBufferBase == this.charBufferBase
				&& charBuffer.limit() >= position.charOffset) {
			// go there directly
			charBuffer.position((int) position.charOffset-charBufferBaseShift);
			return;
		}
		
		// move reader to byteBufferBase
		if (setBytePosition(position.byteBufferBase) < position.byteBufferBase) {
			throw new IOException("File is too small - byte offset "
					+ position.byteBufferBase + " unreachable");
		}
		this.charBufferBase = readChars = position.charBufferBase;
		if (readChars() <= 0) {
			throw new IOException("File cannot be read at byte offset "
					+ position.byteBufferBase);
		}
		for (int i = 0; i < position.charOffset; i++) {
			// discard
			get();
		}
	}

	/**
	 * Mark current position under default marker
	 */
	public void mark() {
		mark("__DEFAULT_MARKER__");
	}
	
	/**
	 * Mark current position under specific `marker`
	 */
	public void mark(Object marker) {
		if (marks == null) {
			marks = new HashMap<Object, ByteCharPosition>();
		}
		marks.put(marker, getPosition());
	}
	
	/**
	 * Move to specific mark
	 * 
	 * @param marker Marker to move to
	 * @return true if marker is valid, false otherwise
	 */
	public boolean setPosition(Object marker) {
		if (marks == null) {
			return false;
		}
		ByteCharPosition pos = marks.get(marker);
		if (pos == null) {
			return false;
		}
		try {
			setPosition(pos);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	
	/**
	 * Moves to absolute character index position.
	 * 
	 * WARNING - this method might be very slow,
	 * try using setPosition(ByteCharPosition) whenever possible
	 * 
	 * @param charIndex
	 * @throws IOException
	 */
	public void setPosition(long charIndex) throws IOException {
		setPosition(
				charIndex > this.charBufferBase 
				? new ByteCharPosition(this.byteBufferBase, this.charBufferBase, charIndex - this.charBufferBase)
				: new ByteCharPosition(0, 0, charIndex));
	}
	
	/**
	 * Set position to default marker (if set)
	 * @return
	 */
	public boolean setPosition() {
		return setPosition("__DEFAULT_MARKER__");
	}
	
	/**
	 * Sets the byte position in input file.
	 * 
	 * This also resets the whole buffer.
	 * 
	 * Note: There is no (fast enough) way how to specify absolute character
	 * position in the file. Use setPosition(ByteCharPosition) instead.
	 * 
	 * @param n
	 *            Number of bytes to skip
	 * @return Number of actually skipped bytes
	 */
	public long setBytePosition(long n) throws IOException {

		reset();

		if (reader instanceof FileChannel) {
			((FileChannel) reader).position(n);
			this.byteBufferBase = readBytes = n;
			return n;
		}
		int readbytes = 0;
		int skipped = 0;
		while (n > 0) {
			byteBuffer.clear();
			if (n < byteBuffer.capacity()) {
				byteBuffer.limit((int) n);
			}
			readbytes = reader.read(byteBuffer);
			if (readbytes < 0) {
				byteBuffer.clear();
				byteBuffer.flip();
				byteBufferBase = readBytes = skipped;
				return skipped;
			}
			n -= byteBuffer.capacity();
			skipped += readbytes;
		}
		byteBuffer.clear();
		byteBuffer.flip();
		byteBufferBase = readBytes = skipped;

		return skipped;
	}

	/**
	 * Returns the underlying CharBuffer for direct access.
	 * 
	 * See {@link handleException} for more information.
	 * 
	 * @return CharBuffer in its current state
	 */
	public CharBuffer getCharBuffer() {
		return this.charBuffer;
	}

	/**
	 * This method handles exceptions thrown by directly accessing the
	 * CharBuffer
	 * 
	 * Example:
	 * 
	 * CharBuffer chb = byteCharBuffer.getCharBuffer(); boolean terminated =
	 * false; while(! terminated) { try { chb.read(); // read characters
	 * directly from CharBuffer process(); // process the characters }
	 * catch(BufferUnderFlowException ex) { terminated = !
	 * byteCharBuffer.handleException(ex); } }
	 * 
	 * @param ex
	 *            Exception
	 * @return Returns true when more data is available, false on EOF
	 * @throws IOException
	 *             A file exception
	 */
	public boolean handleException(BufferUnderflowException ex)
			throws IOException {
		return readChars() > 0;
	}

//	public static void main(String[] args) {
//
//		try {
//			ByteCharBuffer bb = new ByteCharBuffer();
//			bb.init(50, 50);
//			bb.setSource(Channels
//					.newChannel(new FileInputStream("c:/temp/afd")), "utf-8");
//
//			int c;
//
//			System.out.println("--start22--");
////			ByteCharPosition pos = new ByteCharPosition(0, 0, 9);
//			ByteCharPosition pos = new ByteCharPosition(202, 200, 0);
//			bb.setPosition(pos);
//			while ((c = bb.get()) != -1) {
//				System.out.println(((char) c) + " -- " + bb.getPosition());
//			}
//			System.out.println("--end22--");
//
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//	}

	/**
	 * <h2>ByteCharPosition</h2>
	 * 
	 * <p>This class represents a position of a character in input (file).</p>
	 * 
	 * <p>This is generally tricky as there is no way how to compute character
	 * index from number of bytes because variable byte-length charset can be
	 * used.</p>
	 * 
	 * <p>So, we do a trick too: We identify the character by enumerating bytes
	 * that are surely BEFORE the character so that they can be safely skipped.
	 * This is `byteBufferBase` (or bBase). Reading the following bytes produces a byte
	 * buffer that in turn produces a char buffer after decoding (using the
	 * correct charset). In this char buffer, our character is located at
	 * `charOffset` (cOffset) index. The absolute position of a character is cBase + cOffset.</p>
	 * 
	 * <p>So a particular character can be specified by many different ByteCharPosition objects.
	 * Let's say the 24th character in an utf-8 encoded file with international characters can
	 * be found at byte #32. Such character is correctly idenfied as<br/>
	 * <p>
	 * for example:<br/>
	 * <code>ByteCharPosition[bBase=0;cBase=0;cOffset=23]</code><br/>
	 * <code>ByteCharPosition[bBase=10;cBase=10;cOffset=13]</code> (first ten characters were all ASCII single-byte)<br/>
	 * <code>ByteCharPosition[bBase=12;cBase=10;cOffset=13]</code> (there were two double-byte characters in first ten chars)<br/>
	 * <code>ByteCharPosition[bBase=30;cBase=20;cOffset=3]</code> (lot of i18n characters)</p>
	 * 
	 * <p>Note that byte offset of our character (32) never appears in position designation as it is irrelevant.</p>
	 * <p>Also note, that all correct positions can be used, but the ones that are close to real character position
	 * in the file produce best performance results - i.e. Although position [0, 0, 100 000] is theoretically correct,
	 * it is much slower than e.g. [99 900, 99 900, 100]  because in first case the buffer has to traverse
	 * the file from the beginning until it reaches to the requested character. In second case it will
	 * skip first 99 900 characters directly and traverse only 100 characters.</p> 
	 * 
	 * <p>Two position objects are identical when sum of their character base (cBase) and character offset (cOffset)
	 * produces the same absolute character index and reading (bBase) bytes from input file will produce exactly cBase characters
	 * after decoding with the same decoder.</p> 
	 * 
	 * <p>The position object should also carry the CharsetDecoder as the position objects for different decoders vary.
	 * For convenience the decoder is not included in position object and treated as implicitly correct.</p>
	 * 
	 * @author pnajvar
	 * 
	 */
	public static class ByteCharPosition {
		public long byteBufferBase;
		public long charBufferBase;
		public long charOffset;

		public ByteCharPosition(long byteBufferBase, long charBufferBase,
				long charOffset) {
			this.byteBufferBase = byteBufferBase;
			this.charBufferBase = charBufferBase;
			this.charOffset = charOffset;
		}

		public long getCharBufferBase() {
			return charBufferBase;
		}

		public long getCharOffset() {
			return charOffset;
		}

		public long getByteBufferBase() {
			return byteBufferBase;
		}

		/**
		 * Returns the absolute character index in input file
		 * 
		 * @return Absolute character index
		 */
		public long getCharIndex() {
			return charBufferBase + charOffset;
		}

		@Override
		public String toString() {
			return "ByteCharPosition[bBase=" + this.byteBufferBase + ";cBase="
					+ this.charBufferBase + ";cOffset=" + this.charOffset + "]";
		}

	}

}
