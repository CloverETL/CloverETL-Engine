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
import java.nio.InvalidMarkException;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;

import javax.naming.OperationNotSupportedException;

import org.jetel.util.bytes.CloverBuffer;

/**
 * Common interface for input readers providing byte input, char input or mixed char/byte input
 * @author jhadrava (info@cloveretl.com)
 *         (c) Javlin, a.s. (www.cloveretl.com)
 *
 * @created Dec 23, 2010
 */
public interface ICharByteInputReader {
	/** special return value for byte/char reading, indicates that data are available */
	public static final int DATA_AVAILABLE = Integer.MIN_VALUE + 0;
	/** special return value for byte/char reading, indicates end of input */
	public static final int END_OF_INPUT = Integer.MIN_VALUE + 1;
	/** special return value for byte/char reading, indicates decoding error during byte->char conversion */
	public static final int DECODING_FAILED = Integer.MIN_VALUE + 2;
	/** special return value for byte/char reading, indicates that input reader is blocked by large span of the mark */
	public static final int BLOCKED_BY_MARK = Integer.MIN_VALUE + 3;

	/** special value indicating that mark is not set */
	public static final int INVALID_MARK = -1;

	/**
	 * Gets one character from input
	 * @return END_OF_INPUT, DECODING_FAILED, BLOCKED_BY_MARK on error, input char value otherwise
	 * @throws IOException
	 * @throws OperationNotSupportedException
	 */
	public int readChar() throws IOException, OperationNotSupportedException;

	/**
	 * Gets one byte from input
	 * @return END_OF_INPUT, DECODING_FAILED, BLOCKED_BY_MARK on error, input byte value otherwise
	 * @throws IOException
	 * @throws OperationNotSupportedException
	 */
	public int readByte() throws IOException, OperationNotSupportedException;

	/**
	 * Skips specified number of bytes. Preserves the mark.
	 * @param num Positive value for skip, negative value for revert. zero doesn't do anything
	 * @return Number of bytes skipped
	 */
	public int skipBytes(int num);

	/**
	 * Skips specified number of characters. Preserves the mark.
	 * @param num Positive value for skip, negative value for revert. zero doesn't do anything
	 * @return Number of characters skipped
	 */
	public int skipChars(int num);

	/**
	 * Nome omen. 
	 * @return
	 */
	public boolean isEndOfInput();
	
	/**
	 * Marks current position in the input. Precedes revert(), getCharSequence(), and getByteSequence()
	 * 
	 * @throws OperationNotSupportedException
	 */
	public void mark() throws OperationNotSupportedException;

	/**
	 * Reverts the input to the mark. Invalidates mark
	 * 
	 * @throws OperationNotSupportedException
	 * @throws InvalidMarkException
	 */
	public void revert() throws OperationNotSupportedException, InvalidMarkException;

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
	public CharSequence getCharSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException;

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
	public CloverBuffer getByteSequence(int relativeEnd) throws OperationNotSupportedException, InvalidMarkException;
		

	/**
	 * Sets new input source. Invalidates the mark.
	 * 
	 * @param channel
	 */
	public void setInputSource(ReadableByteChannel channel);
	
	/**
	 * Return current position in the input source (number of bytes from the start of the source)
	 */
	public int getPosition();

	/**
	 * Set reading position in the input source. Must be called before any reading operation.
	 * @param position New position (number of bytes from the start of the source)
	 * @throws OperationNotSupportedException Thrown if method is called after input source reading has commenced
	 * @throws IOException 
	 */
	public void setPosition(int position) throws OperationNotSupportedException, IOException;
	
	/**
	 * Returns charset used for byte->char conversion
	 * @return null if char operation are not supported, charset otherwise
	 */
	public Charset getCharset();
	
}
