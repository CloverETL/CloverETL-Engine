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
package org.jetel.data.parser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.InvalidMarkException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

import javax.naming.OperationNotSupportedException;

import org.jetel.data.Defaults;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.bytes.ByteBufferUtils;
import org.jetel.util.bytes.CloverBuffer;

/**
 * An abstract class for input readers able to provide mixed char/byte data
 * 
 * @author jhadrava (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Dec 7, 2010
 */
public abstract class CharByteInputReader implements ICharByteInputReader {

	/** minimal size of buffer operation (reading, decoding, ..) */
	protected static final int MIN_BUFFER_OPERATION_SIZE = 512;

	protected ReadableByteChannel channel;
	protected int inputBytesConsumed;

	public CharByteInputReader() {
		channel = null;
		inputBytesConsumed = 0;
	}

	@Override
	public int readChar() throws IOException, OperationNotSupportedException {
		throw new OperationNotSupportedException("Input reader doesn't support readChar() operation. Choose another implementation");
	}

	@Override
	public int readByte() throws IOException, OperationNotSupportedException {
		throw new OperationNotSupportedException("Input reader doesn't support readByte() operation. Choose another implementation");
	}

	@Override
	public int skipBytes(int num) {
		throw new UnsupportedOperationException("Input reader doesn't support skipBytes() operation. Choose another implementation");
	}

	@Override
	public int skipChars(int num) {
		throw new UnsupportedOperationException("Input reader doesn't support skipChars() operation. Choose another implementation");
	}

	@Override
	public void mark() throws OperationNotSupportedException {
		throw new OperationNotSupportedException("Input reader doesn't support mark() operation. Choose another implementation");
	}
	@Override
	public void revert() throws OperationNotSupportedException, InvalidMarkException {
		throw new OperationNotSupportedException("Input reader doesn't support revert() operation. Choose another implementation");
	}

	@Override
	public CharSequence getCharSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
		throw new OperationNotSupportedException("Input reader doesn't support getCharSequence() operation. Choose another implementation");
	}

	@Override
	public CloverBuffer getByteSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
		throw new OperationNotSupportedException("Input reader doesn't support getByteSequence() operation. Choose another implementation");
	}

	@Override
	public void setInputSource(ReadableByteChannel channel) {
		this.channel = channel;
	}
	
	/** used by DoubleMarkCharByteInputReader */
	protected abstract void setMark(int mark);

	/** used by DoubleMarkCharByteInputReader */
	protected abstract int getMark();

	@Override
	public int getPosition() {
		return inputBytesConsumed;
	}

	@Override
	public void setPosition(int position) throws OperationNotSupportedException, IOException {
		if (inputBytesConsumed != 0) {
			throw new OperationNotSupportedException("setPosition() must be called before reading starts");
		}
		if (position < 0) {
			throw new OperationNotSupportedException("Illegal position: " + position);
		}
		if (channel instanceof FileChannel) {
			((FileChannel)channel).position(position);
		} else {
			ByteBuffer buf = ByteBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
			int bytesRemaining = position;
			while (bytesRemaining > 0) {
				buf.clear();
				if (buf.remaining() > bytesRemaining) {
					buf.limit(bytesRemaining);
				}
				int n = channel.read(buf);
				if (n == -1) {
					bytesRemaining = 0;
					break;  // setting position greater than input size is legal but next reading operation will produce end-of-input result
				}
				if (n == 0) {
					throw new OperationNotSupportedException("Asynchronous input source not support");
				}
				bytesRemaining -= n;
			}
			assert bytesRemaining == 0 : "Unexpected internal state occured during code execution";
		}
		inputBytesConsumed = position;				
	}
	
	/**
	 * Creates input reader suitable for parsing specified metadata
	 * @param metadata
	 * @param charset
	 * @param needByteInput
	 * @param needCharInput
	 * @return
	 */
	public static CharByteInputReader createInputReader(DataRecordMetadata metadata, Charset charset, boolean needByteInput, boolean needCharInput) {
		return createInputReader(new DataRecordMetadata[]{metadata}, charset, needByteInput, needCharInput);
	}
	
	/**
	 * Creates input reader suitable for parsing specified metadata
	 * @param metadataArray
	 * @param charset
	 * @param needByteInput
	 * @param needCharInput
	 * @return
	 */
	public static CharByteInputReader createInputReader(DataRecordMetadata[] metadataArray, Charset charset, boolean needByteInput, boolean needCharInput) {
		int maxBackShift = 0;

		for (DataRecordMetadata metadata : metadataArray) {
			if(metadata == null) {
				continue;
			}
			for (DataFieldMetadata field : metadata.getFields()) {
				if (field.isAutoFilled()) {
					continue;
				}
				if (field.isByteBased()) {
					needByteInput = true;
				} else {
					needCharInput = true;
				}
				maxBackShift = Math.max(maxBackShift, -field.getShift());				
			}
		}

		CharByteInputReader reader;

		if (!needCharInput) {
			reader = new CharByteInputReader.ByteInputReader(maxBackShift);
		} else if (!needByteInput) {
			reader = new CharByteInputReader.CharInputReader(charset, maxBackShift);
		} else if (TextParserConfiguration.isSingleByteCharset(charset)) {
			reader = new CharByteInputReader.SingleByteCharsetInputReader(charset, maxBackShift);
		} else {
			reader = new CharByteInputReader.RobustInputReader(charset, maxBackShift);
		}
		
		return reader;
	}

	/**
	 * This input reader can be used only for data inputs which don't contain any delimiters or char-based fields
	 * 
	 * @author jhadrava (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
	 * 
	 * @created Dec 7, 2010
	 */
	public static class ByteInputReader extends CharByteInputReader {
		private CloverBuffer byteBuffer;
		private int currentMark;
		private boolean endOfInput;
		private int maxBackMark;

		/**
		 * Sole constructor
		 * @param maxBackMark Max span for backward skip.
		 */
		public ByteInputReader(int maxBackMark) {
			super();
			byteBuffer = CloverBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
			channel = null;
			currentMark = INVALID_MARK;
			endOfInput = false;
			this.maxBackMark = maxBackMark;
		}

		@Override
		public int readByte() throws IOException, OperationNotSupportedException {
			if (!byteBuffer.hasRemaining()) { // read data from input
				if (endOfInput) {
					return END_OF_INPUT;
				}
				// byteBuffer gets ready to receive data
				int numBytesToPreserve;
				int markSpan;
				if (currentMark == INVALID_MARK) {
					markSpan = 0;
				} else {
					markSpan = byteBuffer.position() - currentMark;
				}
				numBytesToPreserve = Math.min(byteBuffer.position(), Math.max(markSpan, maxBackMark));
				if (byteBuffer.capacity() - numBytesToPreserve < MIN_BUFFER_OPERATION_SIZE) {
					byteBuffer.limit(numBytesToPreserve + MIN_BUFFER_OPERATION_SIZE);
					if (byteBuffer.capacity() - numBytesToPreserve < MIN_BUFFER_OPERATION_SIZE) {
						return BLOCKED_BY_MARK; // there's not enough space for buffer operations due to the span of the
												// mark
					}
				}
				// preserve data between mark and current position
				byteBuffer.position(byteBuffer.position() - numBytesToPreserve);
				byteBuffer.compact();
				currentMark = numBytesToPreserve - markSpan;

				byteBuffer.limit(byteBuffer.capacity()).position(numBytesToPreserve);
				int bytesConsumed = channel.read(byteBuffer.buf());
				byteBuffer.flip().position(numBytesToPreserve); // get ready to provide data
				switch (bytesConsumed) {
				case 0:
					throw new OperationNotSupportedException("Non-blocking input source not supported by the selected input reader. Choose another implementation");
				case -1: // end of input
					endOfInput = true;
					return END_OF_INPUT;
				}
			}
			return byteBuffer.get();
		}

		@Override
		public int skipBytes(int numBytes) {
			if (numBytes == 0) {
				return 0;
			}
			if (numBytes > 0) {
				int idx;
				for (idx = 0; idx < numBytes; idx++) {
					try {
						readByte();
					} catch (IOException e) {
						break;
					} catch (OperationNotSupportedException e) {
						assert false : "Unexpected execution flow";						
					}
				}
				return idx;
			}			
			int pos = byteBuffer.position();
			if (-numBytes > pos) {
				byteBuffer.position(0);
				return -pos;
			}
			byteBuffer.position(pos + numBytes);
			return numBytes;
		}
		
		@Override
		public boolean isEndOfInput() {
			return endOfInput;
		}
		
		@Override
		public void mark() throws OperationNotSupportedException {
			currentMark = byteBuffer.position();
		}

		@Override
		public void revert() throws OperationNotSupportedException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			byteBuffer.position(currentMark);
			currentMark = INVALID_MARK;
		}

		@Override
		public CloverBuffer getByteSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			int pos = byteBuffer.position();
			byteBuffer.position(currentMark);
			CloverBuffer seq = byteBuffer.slice();
			seq.limit(pos + relativeEnd - currentMark); // set the end of the sequence
			byteBuffer.position(pos); // restore original position
			currentMark = INVALID_MARK;
			return seq;
		}

		@Override
		public void setInputSource(ReadableByteChannel channel) {
			super.setInputSource(channel);
			byteBuffer.clear().flip(); // make it appear empty, so that it is clear we need to fill it
			currentMark = INVALID_MARK;
			endOfInput = false;
		}

		@Override
		protected void setMark(int mark) {
			currentMark = mark;
		}

		@Override
		protected int getMark() {
			return currentMark;
		}

		@Override
		public Charset getCharset() {
			return null;
		}

	}

	/**
	 * This input reader can be used only for data inputs which don't contain any byte-based fields
	 * 
	 * @author jhadrava (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
	 * 
	 * @created Dec 7, 2010
	 */
	public static class CharInputReader extends CharByteInputReader {
		private Charset charset;
		private ByteBuffer byteBuffer;
		private CharBuffer charBuffer;
		private CharsetDecoder decoder;
		private int currentMark;
		private boolean endOfInput;
		private int maxBackMark;

		/**
		 * Sole constructor
		 * @param charset Input charset
		 * @param maxBackMark Max span for backward skip.
		 */
		public CharInputReader(Charset charset, int maxBackMark) {
			super();
			byteBuffer = ByteBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
			charBuffer = CharBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE + MIN_BUFFER_OPERATION_SIZE);
			channel = null;
			this.charset = charset;
			if (charset == null) {
				charset = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			}
			decoder = charset.newDecoder();
			decoder.onMalformedInput(CodingErrorAction.IGNORE);
			decoder.onUnmappableCharacter(CodingErrorAction.IGNORE);
			currentMark = INVALID_MARK;
			endOfInput = false;
			this.maxBackMark = maxBackMark;
		}

		@Override
		public int readChar() throws IOException, OperationNotSupportedException {
			// simple cases first
			if (charBuffer.hasRemaining()) {
				return charBuffer.get();
			}
			if (endOfInput) {
				return END_OF_INPUT;
			}

			// need to get more data in charBuffer

			// charBuffer gets ready to receive data
			int numCharsToPreserve;
			int markSpan;
			if (currentMark == INVALID_MARK) {
				markSpan = 0;
			} else {
				markSpan = charBuffer.position() - currentMark;
			}
			numCharsToPreserve = Math.min(charBuffer.position(), Math.max(markSpan, maxBackMark));
			if (charBuffer.capacity() - numCharsToPreserve < MIN_BUFFER_OPERATION_SIZE) {
				charBuffer = ByteBufferUtils.expandCharBuffer(charBuffer, numCharsToPreserve + MIN_BUFFER_OPERATION_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
				if (charBuffer.capacity() - numCharsToPreserve < MIN_BUFFER_OPERATION_SIZE) {
					return BLOCKED_BY_MARK; // there's not enough space for buffer operations due to the span of the
										    // mark
				}
			}
			// preserve data between mark and current position
			charBuffer.position(charBuffer.position() - numCharsToPreserve);
			charBuffer.compact();
			currentMark = numCharsToPreserve - markSpan;
			do {
				charBuffer.limit(charBuffer.capacity()).position(numCharsToPreserve); // get ready to receive data
				decoder.decode(byteBuffer, charBuffer, false);
				charBuffer.flip().position(numCharsToPreserve); // get ready to provide data
				if (!charBuffer.hasRemaining()) { // need to read more data from input
					byteBuffer.compact(); // get ready to receive data
					int bytesConsumed = channel.read(byteBuffer);
					byteBuffer.flip(); // get ready to provide data
					switch (bytesConsumed) {
					case 0:
						throw new OperationNotSupportedException("Non-blocking input source not supported by the selected input reader. Choose another implementation");
					case -1: // end of input
						charBuffer.limit(charBuffer.capacity()).position(numCharsToPreserve); // get ready to receive
																								// data
						decoder.decode(byteBuffer, charBuffer, true);
						charBuffer.flip().position(numCharsToPreserve); // get ready to provide data
						if (!charBuffer.hasRemaining()) {
							charBuffer.limit(charBuffer.capacity()).position(numCharsToPreserve); // get ready to
																									// receive data
							decoder.flush(charBuffer);
							charBuffer.flip().position(numCharsToPreserve);
							if (!charBuffer.hasRemaining()) {
								endOfInput = true;
								return END_OF_INPUT;
							}
						}
						break;
					default:
						/*
						 * no need to do anything when input data are read successfully, they will be processed in the
						 * next iteration of the outermost loop
						 */
					} // attempt to read from input source
				} // attempt to decode from byte buffer
			} while (!charBuffer.hasRemaining());
			return charBuffer.get();
		}

		@Override
		public int skipChars(int numChars) {
			if (numChars == 0) {
				return 0;
			}
			if (numChars > 0) {
				int idx;
				for (idx = 0; idx < numChars; idx++) {
					try {
						readChar();
					} catch (IOException e) {
						break;
					} catch (OperationNotSupportedException e) {
						throw new JetelRuntimeException("Unexpected execution flow", e);						
					}
				}
				return idx;
			}			
			int pos = charBuffer.position();
			if (-numChars > pos) {
				charBuffer.position(0);
				return -pos;
			}
			charBuffer.position(pos + numChars);
			return numChars;
		}

		@Override
		public boolean isEndOfInput() {
			return endOfInput;
		}
		
		@Override
		public void mark() throws OperationNotSupportedException {
			currentMark = charBuffer.position();
		}

		@Override
		public void revert() throws OperationNotSupportedException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			charBuffer.position(currentMark);
			// discard mark automatically to avoid problems with instance user forgetting to discard it explicitly
			currentMark = INVALID_MARK;
		}

		@Override
		public CharSequence getCharSequence(int relativeEnd) throws OperationNotSupportedException,
				InvalidMarkException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			CharSequence seq = String.copyValueOf(charBuffer.array(), charBuffer.arrayOffset() + currentMark, charBuffer.arrayOffset() + charBuffer.position() + relativeEnd - currentMark);
//			CharSequence seq = CharBuffer.wrap(charBuffer.array(), charBuffer.arrayOffset() + currentMark, charBuffer.arrayOffset() + charBuffer.position() + relativeEnd - currentMark);
			currentMark = INVALID_MARK;
			return seq;
			
		}

		@Override
		public void setInputSource(ReadableByteChannel channel) {
			super.setInputSource(channel);
			byteBuffer.clear().flip(); // make it appear empty, so that it is clear we need to fill it
			charBuffer.clear().flip(); // make it appear empty, so that it is clear we need to fill it
			decoder.reset();
			currentMark = INVALID_MARK;
			endOfInput = false;
		}

		@Override
		protected void setMark(int mark) {
			currentMark = mark;
		}

		@Override
		protected int getMark() {
			return currentMark;
		}

		@Override
		public Charset getCharset() {
			return charset;
		}

	}

	/**
	 * This implementation of the input reader can be used for input data combining byte-based fields and char-based
	 * fields encoded in single byte charsets
	 * 
	 * @author jhadrava (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
	 * 
	 * @created Dec 7, 2010
	 */
	public static class SingleByteCharsetInputReader extends CharByteInputReader {
		private Charset charset;
		private ByteBuffer byteBuffer;
		private CharBuffer charBuffer;
		private CharsetDecoder decoder;
		private int currentMark;
		private boolean endOfInput;
		private int maxBackMark;

		/**
		 * Sole constructor
		 * @param charset Input charset
		 * @param maxBackMark Max span for backward skip.
		 */
		public SingleByteCharsetInputReader(Charset charset, int maxBackMark) {
			super();
			byteBuffer = ByteBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE + MIN_BUFFER_OPERATION_SIZE);
			charBuffer = CharBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE + MIN_BUFFER_OPERATION_SIZE);
			channel = null;
			this.charset = charset;
			if (charset == null) {
				charset = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			}
			decoder = charset.newDecoder();
			decoder.onMalformedInput(CodingErrorAction.REPORT);
			decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
			currentMark = INVALID_MARK;
			endOfInput = false;
			this.maxBackMark = maxBackMark;
		}

		/**
		 * Nomen omen
		 * @return value indicating success or the reason of failure
		 * @throws IOException
		 * @throws OperationNotSupportedException
		 */
		private int ensureBuffersNotEmpty() throws IOException, OperationNotSupportedException {
			// simple cases first
			if (charBuffer.hasRemaining()) {
				return DATA_AVAILABLE;
			}
			if (endOfInput) {
				return END_OF_INPUT;
			}

			// need to get more data in charBuffer

			int numBytesToPreserve;
			int markSpan;
			if (currentMark == INVALID_MARK) {
				markSpan = 0;
			} else {
				markSpan = charBuffer.position() - currentMark;
			}
			numBytesToPreserve = Math.min(charBuffer.position(), Math.max(markSpan, maxBackMark));
			if (charBuffer.capacity() - numBytesToPreserve < MIN_BUFFER_OPERATION_SIZE) {
				charBuffer = ByteBufferUtils.expandCharBuffer(charBuffer, numBytesToPreserve + MIN_BUFFER_OPERATION_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
				if (charBuffer.capacity() - numBytesToPreserve < MIN_BUFFER_OPERATION_SIZE) {
					return BLOCKED_BY_MARK; // there's not enough space for buffer operations due to the span of the
											// mark
				}
			}
			// preserve data between mark and current position
			charBuffer.position(charBuffer.position() - numBytesToPreserve);
			charBuffer.compact();
			byteBuffer.position(byteBuffer.position() - numBytesToPreserve);
			byteBuffer.compact();
			currentMark = numBytesToPreserve - markSpan;

			// get more data from input
			charBuffer.limit(charBuffer.capacity()).position(numBytesToPreserve); // get ready to receive data
			byteBuffer.limit(byteBuffer.capacity()).position(numBytesToPreserve); // get ready to receive data
			int bytesConsumed = channel.read(byteBuffer);
			byteBuffer.flip().position(numBytesToPreserve); // get ready to provide data
			switch (bytesConsumed) {
			case 0:
				throw new OperationNotSupportedException("Non-blocking input source not supported by the selected input reader. Choose another implementation");
			case -1: // end of input
				assert !byteBuffer.hasRemaining() : "Unexpected internal state occured during code execution";
				/*
				 * make sure there are no chars remaining inside the decoder - that would would mean our assumptions
				 * about single byte charset decoders are invalid
				 */
				decoder.decode(byteBuffer, charBuffer, true);
				decoder.flush(charBuffer);
				charBuffer.flip().position(numBytesToPreserve); // get ready to provide data
				/*
				 * it is reasonable to assume that the operations above didn't produce any characters as we are dealing
				 * with single byte charsets and the byte buffer was empty
				 */
				if (charBuffer.hasRemaining()) {
					throw new OperationNotSupportedException("Selected charset doesn't conform to limitations of this input reader. Choose another implementation");
				}
				endOfInput = true;
				return END_OF_INPUT; // no more input available
			default:
				assert byteBuffer.position() == numBytesToPreserve && byteBuffer.limit() > numBytesToPreserve : "Unexpected internal state occured during code execution";
				byteBuffer.mark();
				if (decoder.decode(byteBuffer, charBuffer, false).isError()) {
					// any errors disrupt one-to-one correspondence between byte buffer and char buffer
					throw new OperationNotSupportedException("Selected charset doesn't conform to limitations imposed by single byte charset input reader. Choose another implementation");
				}
				byteBuffer.reset();
				charBuffer.flip().position(numBytesToPreserve); // get ready to provide data
				if (byteBuffer.limit() != charBuffer.limit()) {
					throw new OperationNotSupportedException("Selected charset doesn't conform to limitations imposed by single byte charset input reader. Choose another implementation");
				}
			} // switch
			assert charBuffer.hasRemaining() : "Unexpected internal state occured during code execution";
			return DATA_AVAILABLE;
		}

		@Override
		public int readByte() throws IOException, OperationNotSupportedException {
			int retval = ensureBuffersNotEmpty();
			if (retval != DATA_AVAILABLE) {
				return retval;
			}
			charBuffer.get(); // skip character
			return byteBuffer.get();
		}

		@Override
		public int readChar() throws IOException, OperationNotSupportedException {
			int retval = ensureBuffersNotEmpty();
			if (retval != DATA_AVAILABLE) {
				return retval;
			}
			if (!byteBuffer.hasRemaining()) {
				return 0;
			}
			byteBuffer.get(); // skip byte
			return charBuffer.get();
		}

		@Override
		public int skipBytes(int numBytes) {
			assert charBuffer.position() == byteBuffer.position() : "Unexpected condition occured during code execution";
			if (numBytes == 0) {
				return 0;
			}
			if (numBytes > 0) {
				int idx;
				for (idx = 0; idx < numBytes; idx++) {
					try {
						readByte();
					} catch (IOException e) {
						break;
					} catch (OperationNotSupportedException e) {
						throw new JetelRuntimeException("Unexpected execution flow", e);						
					}
				}
				assert charBuffer.position() == byteBuffer.position() : "Unexpected condition occured during code execution";
				return idx;
			}			
			int pos = charBuffer.position();
			if (-numBytes > pos) {
				charBuffer.position(0);
				byteBuffer.position(0);
				return -pos;
			}
			charBuffer.position(pos + numBytes);
			byteBuffer.position(pos + numBytes);
			return numBytes;
		}

		@Override
		public int skipChars(int num) {
			return skipBytes(num);
		}
		
		@Override
		public boolean isEndOfInput() {
			return endOfInput;
		}
		
		@Override
		public void mark() throws OperationNotSupportedException {
			assert charBuffer.position() == byteBuffer.position() : "Unexpected internal state occured during code execution";
			currentMark = charBuffer.position();
		}

		@Override
		public void revert() throws OperationNotSupportedException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			charBuffer.position(currentMark);
			byteBuffer.position(currentMark);
			// discard mark automatically to avoid problems with instance user forgetting to discard it explicitly
			currentMark = INVALID_MARK;
		}

		@Override
		public CharSequence getCharSequence(int relativeEnd) throws OperationNotSupportedException,
				InvalidMarkException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			CharSequence seq = new String(charBuffer.array(), charBuffer.arrayOffset() + currentMark, charBuffer.arrayOffset() + charBuffer.position() + relativeEnd - currentMark);
			// discard mark automatically to avoid problems with instance user forgetting to discard it explicitly
			currentMark = INVALID_MARK;
			return seq;
		}

		@Override
		public CloverBuffer getByteSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			int pos = byteBuffer.position();
			byteBuffer.position(currentMark);
			CloverBuffer seq = CloverBuffer.wrap(byteBuffer.slice());
			seq.limit(pos + relativeEnd - currentMark); // set the end of the sequence
			byteBuffer.position(pos); // restore original position
			currentMark = INVALID_MARK;
			return seq;
		}

		@Override
		public void setInputSource(ReadableByteChannel channel) {
			super.setInputSource(channel);
			byteBuffer.clear().flip(); // make it appear empty, so that it is clear we need to fill it
			charBuffer.clear().flip(); // make it appear empty, so that it is clear we need to fill it
			decoder.reset();
			currentMark = INVALID_MARK;
			endOfInput = false;
		}

		@Override
		protected void setMark(int mark) {
			currentMark = mark;
		}

		@Override
		protected int getMark() {
			return currentMark;
		}

		@Override
		public Charset getCharset() {
			return charset;
		}

	}

	/**
	 * This implementation of the input reader should be used as last resort when none of the implementations above can
	 * be used That means it can be used for multiple byte charsets on inputs combining char-based and byte-based data
	 * fields. NOTE! Unpredictable behaviour should be expected for charsets with a CharsetDecoder maintaining internal
	 * state
	 * 
	 * @author jhadrava (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
	 * 
	 * @created Dec 7, 2010
	 */
	public static class RobustInputReader extends CharByteInputReader {
		private Charset charset;
		private CloverBuffer byteBuffer;
		private CharBuffer charBuffer;
		private CharsetDecoder decoder;
		private int currentByteMark;
		private int currentCharMark;
		private boolean endOfInput;
		private int maxBackMark;

		/**
		 * Sole constructor
		 * @param charset Input charset
		 * @param maxBackMark Max span for backward skip.
		 */
		public RobustInputReader(Charset charset, int maxBackMark) {
			super();
			channel = null;
			this.charset = charset;
			if (charset == null) {
				charset = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			}
			int maxBytesPerChar = Math.round(charset.newEncoder().maxBytesPerChar());
			byteBuffer = CloverBuffer.allocateDirect(maxBytesPerChar * (Defaults.Record.RECORD_INITIAL_SIZE + MIN_BUFFER_OPERATION_SIZE), maxBytesPerChar * Defaults.Record.RECORD_LIMIT_SIZE);
			charBuffer = CharBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE + MIN_BUFFER_OPERATION_SIZE);
			decoder = charset.newDecoder();
			decoder.onMalformedInput(CodingErrorAction.REPORT);
			decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
			currentCharMark = currentByteMark = INVALID_MARK;
			endOfInput = false;
			this.maxBackMark = maxBackMark;
		}

		@Override
		public int readChar() throws IOException, OperationNotSupportedException {
			// simple cases first
			if (charBuffer.hasRemaining()) {
				return DATA_AVAILABLE;
			}
			if (endOfInput) {
				return END_OF_INPUT;
			}

			// need to get more data in charBuffer

			int numCharsToPreserve;
			int charMarkSpan;
			if (currentCharMark == INVALID_MARK) {
				charMarkSpan = 0;
			} else {
				charMarkSpan = charBuffer.position() - currentCharMark;
				// char buffer capacity test
			}
			numCharsToPreserve = Math.min(charBuffer.position(), Math.max(charMarkSpan, maxBackMark));
			if (charBuffer.capacity() - numCharsToPreserve < MIN_BUFFER_OPERATION_SIZE) {
				charBuffer = ByteBufferUtils.expandCharBuffer(charBuffer, numCharsToPreserve + MIN_BUFFER_OPERATION_SIZE, Defaults.Record.RECORD_LIMIT_SIZE);
				if (charBuffer.capacity() - numCharsToPreserve < MIN_BUFFER_OPERATION_SIZE) {
					return BLOCKED_BY_MARK; // there's not enough space for buffer operations due to the span of the
											// mark
				}
			}
			// preserve data between mark and current position
			charBuffer.position(charBuffer.position() - numCharsToPreserve);
			charBuffer.compact();
			currentCharMark = numCharsToPreserve - charMarkSpan;

			// need to decode more data
			do {
				charBuffer.limit(numCharsToPreserve + 1).position(numCharsToPreserve); // get ready to receive one char

				if (decoder.decode(byteBuffer.buf(), charBuffer, false).isError()) {
					return DECODING_FAILED;
				}
				charBuffer.flip().position(numCharsToPreserve); // get ready to provide data
				if (!charBuffer.hasRemaining()) { // need to read and decode more data from input
					// prepare byte buffer
					int numBytesToPreserve;
					int byteMarkSpan;
					if (currentByteMark == INVALID_MARK) {
						assert currentCharMark == INVALID_MARK : "Unexpected internal state occured during code execution";
						byteMarkSpan = 0;
					} else {
						byteMarkSpan = byteBuffer.position() - currentByteMark;
					}
					numBytesToPreserve = Math.min(byteBuffer.position(), Math.max(byteMarkSpan, maxBackMark));
					// following condition is implied by char buffer capacity test above
					assert byteBuffer.capacity() - numBytesToPreserve >= MIN_BUFFER_OPERATION_SIZE : "Unexpected internal state occured during code execution";
					// preserve data between mark and current position
					byteBuffer.position(byteBuffer.position() - numBytesToPreserve);
					byteBuffer.compact();
					currentByteMark = numBytesToPreserve - byteMarkSpan;

					byteBuffer.limit(byteBuffer.capacity()).position(numBytesToPreserve); // get ready to receive data
					int bytesConsumed = channel.read(byteBuffer.buf());
					byteBuffer.flip().position(numBytesToPreserve); // get ready to provide data
					switch (bytesConsumed) {
					case 0:
						throw new OperationNotSupportedException("Non-blocking input source not supported by the selected input reader. Choose another implementation");
					case -1: // end of input
						// check that the decoder doesn't maintain internal state
						charBuffer.clear(); // get ready to receive data
						decoder.decode(byteBuffer.buf(), charBuffer, true); // decode any remaining data
						decoder.flush(charBuffer);
						charBuffer.flip();
						if (charBuffer.hasRemaining()) {
							throw new OperationNotSupportedException("Charset decoder maintaining internal state is not supported by the selected input reader");
						}
						endOfInput = true;
						return END_OF_INPUT;
					default:
						/*
						 * after input data are read successfully, there's no need to do anything. the data will be
						 * processed in the next iteration
						 */
					} // attempt to read from input source
				} // attempt to decode from byte buffer
			} while (!charBuffer.hasRemaining());
			assert charBuffer.remaining() == 1 : "Unexpected internal state occured during code execution";
			return charBuffer.get();
		}

		@Override
		public int readByte() throws IOException, OperationNotSupportedException {
			// simple cases first
			if (byteBuffer.hasRemaining()) {
				currentCharMark = INVALID_MARK; // reading bytes without decoding them invalidates any buffered chars and the char mark
				assert !charBuffer.hasRemaining() : "Unexpected internal state occured during code execution";
				return byteBuffer.get();
			}
			if (endOfInput) {
				return END_OF_INPUT;
			}

			int numBytesToPreserve;
			int byteMarkSpan;
			if (currentByteMark == INVALID_MARK) {
				assert currentCharMark == INVALID_MARK : "Unexpected internal state occured during code execution";
				byteMarkSpan = 0;
			} else {
				byteMarkSpan = byteBuffer.position() - currentByteMark;
			}
			numBytesToPreserve = Math.min(byteBuffer.position(), Math.max(byteMarkSpan, maxBackMark));
			
			if (byteBuffer.capacity() - numBytesToPreserve < MIN_BUFFER_OPERATION_SIZE) {
				byteBuffer.expand(MIN_BUFFER_OPERATION_SIZE);
				if (byteBuffer.capacity() - numBytesToPreserve < MIN_BUFFER_OPERATION_SIZE) {
					return BLOCKED_BY_MARK; // there's not enough space for buffer operations due to the span of the
											// mark
				}
			}
			// preserve data between mark and current position
			byteBuffer.limit(byteBuffer.capacity()).position(numBytesToPreserve);
			byteBuffer.compact();
			currentByteMark = numBytesToPreserve - byteMarkSpan;

			byteBuffer.flip().position(numBytesToPreserve); // get ready to receive data
			int bytesConsumed = channel.read(byteBuffer.buf());
			byteBuffer.flip().position(numBytesToPreserve); // get ready to provide data
			switch (bytesConsumed) {
			case 0:
				throw new OperationNotSupportedException("Non-blocking input source not supported by the selected input reader. Choose another implementation");
			case -1: // end of input
				// check that the decoder doesn't maintain internal state
				charBuffer.clear(); // get ready to receive data
				decoder.decode(byteBuffer.buf(), charBuffer, true); // decode any remaining data
				decoder.flush(charBuffer);
				charBuffer.flip();
				if (charBuffer.hasRemaining()) {
					throw new OperationNotSupportedException("Charset decoder maintaining internal state is not supported by the selected input reader");
				}
				endOfInput = true;
				return END_OF_INPUT;
			}
			assert byteBuffer.hasRemaining() : "Unexpected internal state occured during code execution";
			currentCharMark = INVALID_MARK; // reading bytes without decoding them invalidates any buffered chars and the char mark
			assert !charBuffer.hasRemaining() : "Unexpected internal state occured during code execution";
			return byteBuffer.get();
		}

		@Override
		public void setInputSource(ReadableByteChannel channel) {
			super.setInputSource(channel);
			byteBuffer.clear().flip(); // make it appear empty, so that it is clear we need to fill it
			charBuffer.clear().flip(); // make it appear empty, so that it is clear we need to fill it
			decoder.reset();
			currentCharMark = currentByteMark = INVALID_MARK;
			endOfInput = false;
		}

		@Override
		public int skipBytes(int numBytes) {
			//assert !charBuffer.hasRemaining() : "Unexpected internal state occured during code execution";
			if (numBytes == 0) {
				return 0;
			}
			if (numBytes > 0) {
				int idx;
				for (idx = 0; idx < numBytes; idx++) {
					try {
						readByte();
					} catch (IOException e) {
						break;
					} catch (OperationNotSupportedException e) {
						throw new JetelRuntimeException("Unexpected execution flow", e);						
					}
				}
				return idx;
			}
			int pos = byteBuffer.position();
			if (-numBytes > pos) {
				byteBuffer.position(0);
				return -pos;
			}
			byteBuffer.position(pos + numBytes);
			return numBytes;
		}

		@Override
		public int skipChars(int numChars) {
			//assert !charBuffer.hasRemaining() : "Unexpected internal state occured during code execution";
			if (numChars == 0) {
				return 0;
			}
			if (numChars > 0) {
				int idx;
				for (idx = 0; idx < numChars; idx++) {
					try {
						readChar();
					} catch (IOException e) {
						break;
					} catch (OperationNotSupportedException e) {
						throw new JetelRuntimeException("Unexpected execution flow", e);						
					}
				}
				return idx;
			}
			int pos = charBuffer.position();
			if (-numChars > pos) {
				charBuffer.position(0);
				return -pos;
			}
			charBuffer.position(pos + numChars);
			return numChars;
		}

		@Override
		public void mark() throws OperationNotSupportedException {
			currentByteMark = byteBuffer.position();
			currentCharMark = charBuffer.position();
		}

		@Override
		public boolean isEndOfInput() {
			return endOfInput;
		}
		
		@Override
		public void revert() throws OperationNotSupportedException {
			if (currentByteMark == INVALID_MARK) {
				assert currentCharMark == INVALID_MARK : "Unexpected internal state occured during code execution";
				throw new InvalidMarkException();
			}
			assert !charBuffer.hasRemaining() : "Unexpected internal state occured during code execution";
			
			byteBuffer.position(currentByteMark);
			
			endOfInput = false; // reset the flag to allow reading after END_OF_INPUT 
			decoder.reset(); // reset the decoder

			// discard mark automatically to avoid problems with instance user forgetting to discard it explicitly
			currentCharMark = currentByteMark = INVALID_MARK;
		}

		@Override
		public CharSequence getCharSequence(int relativeEnd) throws OperationNotSupportedException,
				InvalidMarkException {
			if (currentCharMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			CharSequence seq = new String(charBuffer.array(), charBuffer.arrayOffset() + currentCharMark, charBuffer.arrayOffset() + charBuffer.position() + relativeEnd - currentCharMark);
			// discard mark automatically to avoid problems with instance user forgetting to discard it explicitly
			currentCharMark = currentByteMark = INVALID_MARK;
			return seq;
		}

		@Override
		public CloverBuffer getByteSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
			if (currentByteMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			int pos = byteBuffer.position();
			byteBuffer.position(currentByteMark);
			CloverBuffer seq = byteBuffer.slice();
			seq.limit(pos + relativeEnd - currentByteMark); // set the end of the sequence
			byteBuffer.position(pos); // restore original position
			currentByteMark = INVALID_MARK;
			return seq;
		}

		@Override
		protected void setMark(int mark) {
			currentByteMark = mark;
		}

		@Override
		protected int getMark() {
			return currentByteMark;
		}

		@Override
		public Charset getCharset() {
			return charset;
		}

	}

	/**
	 * An ugly hack to provide support for the verbose error reporting in the parsers.
	 * Adds double mark capability to the underlying input reader.
	 * The additional mark (outer mark) precedes the standard mark supported natively by
	 * the underlying input reader. Typically outer mark is set at the beginning of the record
	 * while standard mark is set at the beginning of the field.
	 * Revert to the position of the outer mark is not supported, it is used only
	 * by the getOuterSequence() method.
	 *  
	 * @author jhadrava (info@cloveretl.com)
	 *         (c) Javlin, a.s. (www.cloveretl.com)
	 *
	 * @created Dec 10, 2010
	 */
	public static class DoubleMarkCharByteInputReader implements ICharByteInputReader {
		CharByteInputReader inputReader;
		int outerMark;
		int innerMark;

		/**
		 * Sole constructor
		 * @param inputReader Underlying input reader
		 */
		public DoubleMarkCharByteInputReader(CharByteInputReader inputReader) {
			this.inputReader = inputReader;
			this.outerMark = INVALID_MARK;
			this.innerMark = INVALID_MARK;
		}

		/**
		 * Sets the outer mark.
		 * @throws OperationNotSupportedException
		 */
		public void setOuterMark() throws OperationNotSupportedException {
			inputReader.mark();
			outerMark = inputReader.getMark();
		}

		/**
		 * Releases the outer mark.
		 * @throws OperationNotSupportedException
		 */
		public void releaseOuterMark() throws OperationNotSupportedException {
			outerMark = INVALID_MARK;
			inputReader.setMark(innerMark);
		}

		/**
		 * Returns a subsequence starting at the position of the outer mark. 
		 * @param relativeEnd
		 * @return
		 * @throws OperationNotSupportedException
		 * @throws InvalidMarkException
		 */
		public Object getOuterSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
			inputReader.setMark(outerMark);
			try {
				return inputReader.getByteSequence(relativeEnd);
			} catch (OperationNotSupportedException e) {
				return inputReader.getCharSequence(relativeEnd);
			}
		}

		@Override
		public int readChar() throws IOException, OperationNotSupportedException {
			inputReader.setMark(outerMark);
			int retval = inputReader.readChar();
			int markDiff = outerMark - inputReader.getMark();
			outerMark -= markDiff;
			innerMark -= markDiff;
			return retval;
		}

		@Override
		public int readByte() throws IOException, OperationNotSupportedException {
			inputReader.setMark(outerMark);
			int retval = inputReader.readByte();
			int markDiff = outerMark - inputReader.getMark();
			outerMark -= markDiff;
			innerMark -= markDiff;
			return retval;
		}

		@Override
		public void revert() throws OperationNotSupportedException, InvalidMarkException {
			inputReader.setMark(innerMark);
			inputReader.revert();
		}

		@Override
		public CharSequence getCharSequence(int relativeEnd) throws OperationNotSupportedException,
				InvalidMarkException {
			inputReader.setMark(innerMark);
			return inputReader.getCharSequence(relativeEnd);
		}

		@Override
		public CloverBuffer getByteSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
			inputReader.setMark(innerMark);
			return inputReader.getByteSequence(relativeEnd);
		}

		@Override
		public void setInputSource(ReadableByteChannel channel) {
			inputReader.setInputSource(channel);
			innerMark = outerMark = INVALID_MARK;
		}

		@Override
		public int skipBytes(int numBytes) {
			inputReader.setMark(outerMark);
			int retval = inputReader.skipBytes(numBytes);
			int markDiff = outerMark - inputReader.getMark();
			outerMark -= markDiff;
			innerMark -= markDiff;
			return retval;
		}

		@Override
		public int skipChars(int numChars) {
			inputReader.setMark(outerMark);
			int retval = inputReader.skipChars(numChars);
			int markDiff = outerMark - inputReader.getMark();
			outerMark -= markDiff;
			innerMark -= markDiff;
			return retval;
		}

		@Override
		public boolean isEndOfInput() {
			return inputReader.isEndOfInput();
		}
		
		@Override
		public void mark() throws OperationNotSupportedException {
			inputReader.mark();
			innerMark = inputReader.getMark();
			if (outerMark == INVALID_MARK) {
				outerMark = innerMark;
			}
			inputReader.setMark(outerMark);
		}

		@Override
		public int getPosition() {
			return inputReader.getPosition();
		}

		@Override
		public void setPosition(int position) throws OperationNotSupportedException, IOException {
			inputReader.setPosition(position);
		}

		@Override
		public Charset getCharset() {
			return inputReader.getCharset();
		}

	}
	
}
