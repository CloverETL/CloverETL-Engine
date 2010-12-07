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
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

import javax.naming.OperationNotSupportedException;

import org.jetel.data.Defaults;
import org.jetel.metadata.DataRecordMetadata;

/**
 * An abstract class for input readers able to provide mixed char/byte data
 * 
 * @author jhadrava (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
 * 
 * @created Dec 7, 2010
 */
public abstract class CharByteInputReader {
	/** special return value for byte/char reading, indicates that data are available */
	public static final int DATA_AVAILABLE = 0;
	/** special return value for byte/char reading, indicates end of input */
	public static final int END_OF_INPUT = -1;
	/** special return value for byte/char reading, indicates decoding error during byte->char conversion */
	public static final int DECODING_FAILED = -2;
	/** special return value for byte/char reading, indicates that input reader is blocked by large span of the mark */
	public static final int BLOCKED_BY_MARK = -3;

	/** special value indicating that mark is not set */
	public static final int INVALID_MARK = -1;

	/** minimal size of buffer operation (reading, decoding, ..) */
	protected final int MIN_BUFFER_OPERATION_SIZE = 512;
	
	protected ReadableByteChannel channel;

	/**
	 * 
	 * @return END_OF_INPUT, DECODING_FAILED, BLOCKED_BY_MARK on error, input char value otherwise
	 * @throws IOException
	 * @throws OperationNotSupportedException
	 */
	public int readChar() throws IOException, OperationNotSupportedException {
		throw new OperationNotSupportedException("Input reader doesn't support readChar operation. Choose another implementation");
	}

	/**
	 * 
	 * @return END_OF_INPUT, DECODING_FAILED, BLOCKED_BY_MARK on error, input byte value otherwise
	 * @throws IOException
	 * @throws OperationNotSupportedException
	 */
	public int readByte() throws IOException, OperationNotSupportedException {
		throw new OperationNotSupportedException("Input reader doesn't support readByte operation. Choose another implementation");
	}

	/**
	 * Marks current position in the input. Precedes reset(), getCharSequence(), and getByteSequence()
	 * 
	 * @throws OperationNotSupportedException
	 */
	public void mark() throws OperationNotSupportedException {
		throw new OperationNotSupportedException("Input reader doesn't support revertBytes() operation. Choose another implementation");
	}

	/**
	 * Reverts the input to the mark. Invalidates mark
	 * 
	 * @throws OperationNotSupportedException
	 * @throws InvalidMarkException
	 */
	public void revert() throws OperationNotSupportedException, InvalidMarkException {
		throw new OperationNotSupportedException("Input reader doesn't support revertBytes() operation. Choose another implementation");
	}

	/**
	 * Returns the chars starting at the mark and ending at the position specified by the parameter. Invalidates the
	 * mark.
	 * 
	 * @param relativeEnd
	 *            End of the sequence, relative to the current position. Should be zero or negative.
	 * @return
	 * @throws OperationNotSupportedException
	 * @throws InvalidMarkException
	 */
	public CharSequence getCharSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
		throw new OperationNotSupportedException("Input reader doesn't support revertBytes() operation. Choose another implementation");
	}

	/**
	 * Returns the bytes starting at the mark and ending at the position specified by the parameter. Invalidates the
	 * mark.
	 * 
	 * @param relativeEnd
	 *            End of the sequence, relative to the current position. Should be zero or negative
	 * @return
	 * @throws OperationNotSupportedException
	 * @throws InvalidMarkException
	 */
	public ByteBuffer getByteSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
		throw new OperationNotSupportedException("Input reader doesn't support revertBytes() operation. Choose another implementation");
	}

	/**
	 * creates a reader best suited to parse data input specified by the passed parameters
	 * 
	 * @param metadata
	 *            data description
	 * @param charset
	 *            charset of the input source
	 * @param channel
	 *            input source
	 * @return reader ready to read
	 **/
	public static CharByteInputReader create(DataRecordMetadata metadata, Charset charset) {
		CharByteInputReader reader;
		reader = new ByteInputReader(); // TODO
		return reader;
	}

	/**
	 * Sets new input source. Invalidates the mark.
	 * 
	 * @param channel
	 */
	public void setInputSource(ReadableByteChannel channel) {
		this.channel = channel;
	}

	/**
	 * This input reader can be used only for data inputs which doesn't contain any delimiters or char-based fields
	 * 
	 * @author jhadrava (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
	 * 
	 * @created Dec 7, 2010
	 */
	public static class ByteInputReader extends CharByteInputReader {
		private ByteBuffer byteBuffer;
		private int currentMark;
		private boolean endOfInput;

		public ByteInputReader() {
			byteBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
			channel = null;
			currentMark = INVALID_MARK;
			endOfInput = false;
		}

		@Override
		public int readByte() throws IOException, OperationNotSupportedException {
			if (!byteBuffer.hasRemaining()) { // read data from input
				if (endOfInput) {
					return END_OF_INPUT;
				}
				// byteBuffer gets ready to receive data
				int numBytesToPreserve;
				if (currentMark == INVALID_MARK) {
					numBytesToPreserve = 0;
				} else {
					numBytesToPreserve = byteBuffer.position() - currentMark;
					if (byteBuffer.capacity() - numBytesToPreserve < MIN_BUFFER_OPERATION_SIZE) {
						return BLOCKED_BY_MARK; // there's not enough space for buffer operations due to the span of the
												// mark
					}
					// preserve data between mark and current position
					byteBuffer.position(currentMark);
					byteBuffer.compact();
					currentMark = 0;
				}
				byteBuffer.limit(byteBuffer.capacity()).position(numBytesToPreserve);
				int bytesConsumed = channel.read(byteBuffer);
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

		public void mark() throws OperationNotSupportedException {
			currentMark = byteBuffer.position();
		}

		public void reset() throws OperationNotSupportedException {
			byteBuffer.position(currentMark);
			currentMark = INVALID_MARK;
		}

		@Override
		public ByteBuffer getByteSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			int pos = byteBuffer.position();
			byteBuffer.position(currentMark);
			ByteBuffer seq = byteBuffer.slice();
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
		}

	}

	/**
	 * This input reader can be used only for data inputs which doesn't contain any byte-based fields
	 * 
	 * @author jhadrava (info@cloveretl.com) (c) Javlin, a.s. (www.cloveretl.com)
	 * 
	 * @created Dec 7, 2010
	 */
	public static class CharInputReader extends CharByteInputReader {
		private ByteBuffer byteBuffer;
		private CharBuffer charBuffer;
		private CharsetDecoder decoder;
		private int currentMark;
		private boolean endOfInput;

		public CharInputReader(Charset charset) {
			byteBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE);
			charBuffer = CharBuffer.allocate(Defaults.Record.MAX_RECORD_SIZE + MIN_BUFFER_OPERATION_SIZE);
			channel = null;
			if (charset == null) {
				charset = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			}
			decoder = charset.newDecoder();
			decoder.onMalformedInput(CodingErrorAction.IGNORE);
			decoder.onUnmappableCharacter(CodingErrorAction.IGNORE);
			currentMark = INVALID_MARK;
			endOfInput = false;
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
			int numBytesToPreserve;
			if (currentMark == INVALID_MARK) {
				numBytesToPreserve = 0;
			} else {
				numBytesToPreserve = charBuffer.position() - currentMark;
				if (charBuffer.capacity() - numBytesToPreserve < MIN_BUFFER_OPERATION_SIZE) {
					return BLOCKED_BY_MARK; // there's not enough space for buffer operations due to the span of the
											// mark
				}
				// preserve data between mark and current position
				charBuffer.position(currentMark);
				charBuffer.compact();
				currentMark = 0;
			}
			do {
				charBuffer.limit(charBuffer.capacity()).position(numBytesToPreserve); // get ready to receive data
				decoder.decode(byteBuffer, charBuffer, false);
				charBuffer.flip().position(numBytesToPreserve); // get ready to provide data
				if (!charBuffer.hasRemaining()) { // need to read more data from input
					byteBuffer.compact(); // get ready to receive data
					int bytesConsumed = channel.read(byteBuffer);
					byteBuffer.flip(); // get ready to provide data
					switch (bytesConsumed) {
					case 0:
						throw new OperationNotSupportedException("Non-blocking input source not supported by the selected input reader. Choose another implementation");
					case -1: // end of input
						charBuffer.limit(charBuffer.capacity()).position(numBytesToPreserve); // get ready to receive
																								// data
						decoder.decode(byteBuffer, charBuffer, true);
						charBuffer.flip().position(numBytesToPreserve); // get ready to provide data
						if (!charBuffer.hasRemaining()) {
							charBuffer.limit(charBuffer.capacity()).position(numBytesToPreserve); // get ready to
																									// receive data
							decoder.flush(charBuffer);
							charBuffer.flip().position(numBytesToPreserve);
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

		public void mark() throws OperationNotSupportedException {
			currentMark = charBuffer.position();
		}

		public void reset() throws OperationNotSupportedException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			charBuffer.position(currentMark);
			// discard mark automatically to avoid problems with instance user forgetting to discard it explicitly
			currentMark = INVALID_MARK;
		}

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
		public void setInputSource(ReadableByteChannel channel) {
			super.setInputSource(channel);
			byteBuffer.clear().flip(); // make it appear empty, so that it is clear we need to fill it
			charBuffer.clear().flip(); // make it appear empty, so that it is clear we need to fill it
			decoder.reset();
			currentMark = INVALID_MARK;
			endOfInput = false;
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

		private ByteBuffer byteBuffer;
		private CharBuffer charBuffer;
		private CharsetDecoder decoder;
		private int currentMark;
		private boolean endOfInput;

		public SingleByteCharsetInputReader(Charset charset) {
			byteBuffer = ByteBuffer.allocateDirect(Defaults.Record.MAX_RECORD_SIZE + MIN_BUFFER_OPERATION_SIZE);
			charBuffer = CharBuffer.allocate(Defaults.Record.MAX_RECORD_SIZE + MIN_BUFFER_OPERATION_SIZE);
			channel = null;
			if (charset == null) {
				charset = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			}
			decoder = charset.newDecoder();
			decoder.onMalformedInput(CodingErrorAction.REPORT);
			decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
			currentMark = INVALID_MARK;
			endOfInput = false;
		}

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
			if (currentMark == INVALID_MARK) {
				numBytesToPreserve = 0;
			} else {
				assert charBuffer.position() == byteBuffer.position() : "Unexpected internal state occured during code execution";
				numBytesToPreserve = charBuffer.position() - currentMark;

				if (charBuffer.capacity() - numBytesToPreserve < MIN_BUFFER_OPERATION_SIZE) {
					return BLOCKED_BY_MARK; // there's not enough space for buffer operations due to the span of the
											// mark
				}
				// preserve data between mark and current position
				charBuffer.position(currentMark);
				charBuffer.compact();
				byteBuffer.position(currentMark);
				byteBuffer.compact();
				currentMark = 0;
			}

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

		public void mark() throws OperationNotSupportedException {
			assert charBuffer.position() == byteBuffer.position() : "Unexpected internal state occured during code execution";
			currentMark = charBuffer.position();
		}

		public void reset() throws OperationNotSupportedException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			charBuffer.position(currentMark);
			byteBuffer.position(currentMark);
			// discard mark automatically to avoid problems with instance user forgetting to discard it explicitly
			currentMark = INVALID_MARK;
		}

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
		public ByteBuffer getByteSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
			if (currentMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			int pos = byteBuffer.position();
			byteBuffer.position(currentMark);
			ByteBuffer seq = byteBuffer.slice();
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

		private ByteBuffer byteBuffer;
		private CharBuffer charBuffer;
		private CharsetDecoder decoder;
		private int currentByteMark;
		private int currentCharMark;
		private boolean endOfInput;

		public RobustInputReader(Charset charset) {
			channel = null;
			if (charset == null) {
				charset = Charset.forName(Defaults.DataParser.DEFAULT_CHARSET_DECODER);
			}
			int maxBytesPerChar = Math.round(charset.newEncoder().maxBytesPerChar());
			byteBuffer = ByteBuffer.allocateDirect(maxBytesPerChar * (Defaults.Record.MAX_RECORD_SIZE + MIN_BUFFER_OPERATION_SIZE));
			charBuffer = CharBuffer.allocate(Defaults.Record.MAX_RECORD_SIZE + MIN_BUFFER_OPERATION_SIZE);
			decoder = charset.newDecoder();
			decoder.onMalformedInput(CodingErrorAction.REPORT);
			decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
			currentCharMark = currentByteMark = INVALID_MARK;
			endOfInput = false;
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

			if (currentCharMark == INVALID_MARK) {
				numCharsToPreserve = 0;
				charBuffer.clear();
			} else {
				numCharsToPreserve = charBuffer.position() - currentCharMark;
				// char buffer capacity test
				if (charBuffer.capacity() - numCharsToPreserve < MIN_BUFFER_OPERATION_SIZE) {
					return BLOCKED_BY_MARK; // there's not enough space for buffer operations due to the span of the
											// mark
				}
				// preserve data between mark and current position
				charBuffer.position(currentCharMark);
				charBuffer.compact();
				currentCharMark = 0;
			}

			// need to decode more data
			do {
				charBuffer.limit(numCharsToPreserve + 1).position(numCharsToPreserve); // get ready to receive one char

				if (decoder.decode(byteBuffer, charBuffer, false).isError()) {
					return DECODING_FAILED;
				}
				charBuffer.flip().position(numCharsToPreserve); // get ready to provide data
				if (!charBuffer.hasRemaining()) { // need to read and decode more data from input
					// prepare byte buffer
					int numBytesToPreserve;
					if (currentByteMark == INVALID_MARK) {
						assert currentCharMark == INVALID_MARK : "Unexpected internal state occured during code execution";
						byteBuffer.clear();
						numBytesToPreserve = 0;
					} else {
						numBytesToPreserve = byteBuffer.position() - currentByteMark;
						// following condition is implied by char buffer capacity test above
						assert byteBuffer.capacity() - numBytesToPreserve >= MIN_BUFFER_OPERATION_SIZE : "Unexpected internal state occured during code execution";
						// preserve data between mark and current position
						byteBuffer.position(currentByteMark);
						byteBuffer.compact();
						currentByteMark = 0;
					}

					byteBuffer.limit(byteBuffer.capacity()).position(numBytesToPreserve); // get ready to receive data
					int bytesConsumed = channel.read(byteBuffer);
					byteBuffer.flip().position(numBytesToPreserve); // get ready to provide data
					switch (bytesConsumed) {
					case 0:
						throw new OperationNotSupportedException("Non-blocking input source not supported by the selected input reader. Choose another implementation");
					case -1: // end of input
						// check that the decoder doesn't maintain internal state
						charBuffer.clear(); // get ready to receive data
						decoder.decode(byteBuffer, charBuffer, true); // decode any remaining data
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
				return DATA_AVAILABLE;
			}
			if (endOfInput) {
				return END_OF_INPUT;
			}

			int numBytesToPreserve;
			if (currentByteMark == INVALID_MARK) {
				assert currentCharMark == INVALID_MARK : "Unexpected internal state occured during code execution";
				byteBuffer.clear();
				numBytesToPreserve = 0;
			} else {
				numBytesToPreserve = byteBuffer.position() - currentByteMark;
				// following condition is implied by char buffer capacity test above
				assert byteBuffer.capacity() - numBytesToPreserve >= MIN_BUFFER_OPERATION_SIZE : "Unexpected internal state occured during code execution";
				// preserve data between mark and current position
				byteBuffer.position(currentByteMark);
				byteBuffer.compact();
				currentByteMark = 0;
			}
			byteBuffer.flip().position(numBytesToPreserve); // get ready to receive data
			int bytesConsumed = channel.read(byteBuffer);
			byteBuffer.flip().position(numBytesToPreserve); // get ready to provide data
			switch (bytesConsumed) {
			case 0:
				throw new OperationNotSupportedException("Non-blocking input source not supported by the selected input reader. Choose another implementation");
			case -1: // end of input
				// check that the decoder doesn't maintain internal state
				charBuffer.clear(); // get ready to receive data
				decoder.decode(byteBuffer, charBuffer, true); // decode any remaining data
				decoder.flush(charBuffer);
				charBuffer.flip();
				if (charBuffer.hasRemaining()) {
					throw new OperationNotSupportedException("Charset decoder maintaining internal state is not supported by the selected input reader");
				}
				endOfInput = true;
				return END_OF_INPUT;
			}
			assert byteBuffer.hasRemaining() : "Unexpected internal state occured during code execution";
			currentCharMark = INVALID_MARK; // reading bytes without decoding them invalidates any buffered chars and
											// the char mark
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

		public void mark() throws OperationNotSupportedException {
			currentByteMark = byteBuffer.position();
			currentCharMark = charBuffer.position();
		}

		public void reset() throws OperationNotSupportedException {
			if (currentByteMark == INVALID_MARK) {
				assert currentCharMark == INVALID_MARK : "Unexpected internal state occured during code execution";
				;
				throw new InvalidMarkException();
			}
			assert !charBuffer.hasRemaining() : "Unexpected internal state occured during code execution";
			;
			byteBuffer.position(currentByteMark);
			// discard mark automatically to avoid problems with instance user forgetting to discard it explicitly
			currentCharMark = currentByteMark = INVALID_MARK;
		}

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
		public ByteBuffer getByteSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException {
			if (currentByteMark == INVALID_MARK) {
				throw new InvalidMarkException();
			}
			int pos = byteBuffer.position();
			byteBuffer.position(currentByteMark);
			ByteBuffer seq = byteBuffer.slice();
			seq.limit(pos + relativeEnd - currentByteMark); // set the end of the sequence
			byteBuffer.position(pos); // restore original position
			currentByteMark = INVALID_MARK;
			return seq;
		}

	}

}
