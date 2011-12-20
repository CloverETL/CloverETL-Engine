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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

import org.apache.commons.logging.Log;
import org.jetel.data.Defaults;

/**
 * Wrapper for WritableByteChannel, which is created above Log.
 * 
 * @author Jan Ausperger (info@cloveretl.com)
 * (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 */
public class LogOutByteChannel implements WritableByteChannel {

	private Log logger;					// final target
	private CharBuffer charBuffer;		// to where write source
	private CharsetDecoder decoder;		// converts byte to chars
	private CoderResult result;			// result for converting operation
	private String unwrittenLine;		// rest of line 

	/**
	 * Constructor.
	 * @param logger
	 * @param charsetName
	 */
	public LogOutByteChannel(Log logger, String charsetName){
		this.logger = logger;
		decoder = Charset.forName(charsetName).newDecoder();
		charBuffer = CharBuffer.allocate(Defaults.Record.RECORD_INITIAL_SIZE);
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)
	 */
	@Override
	public int write(ByteBuffer src) throws IOException {
		// src start position
		int startPosition = src.position();
		
		do {
	        // prepare clean buffer
	        charBuffer.clear();
	        
			// decode bytes to characters
			result = decoder.decode(src, charBuffer, false);
			
			// validate decoding
	        if (result.isError()) {
	            throw new IOException(result.toString() + " when converting from " + decoder.charset());
	        }
	        
	        // sets position to 0, limit to current position
	        charBuffer.flip();
	        
	        // write result to logger
	        printBuffer();
	        
		} while (src.position() != src.limit());
        
        // bytes processed
		return src.limit() - startPosition;
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#close()
	 */
	@Override
	public void close() throws IOException {
        // write unwritten line
        if (unwrittenLine != null && unwrittenLine.length() > 0) {
	        logger.info(unwrittenLine);
        }
	}

	/**
	 * Writes char buffer to logger.
	 */
	private void printBuffer() {
		// result string
		String result = charBuffer.toString();

		// find end of line
		// Windows		CRLF
		// Unix			LF
		// Macintosh	CR
		int beginIndex = 0;
        int len = result.length();
		do {
			// get begin and end position
			beginIndex = getPositionAfterCRLF(result, beginIndex);
			if (beginIndex == -1) {	// no valid character found
				return;
			}
			int endIndex = getPositionBeforeCRLF(result, beginIndex);

			// end index
			if (endIndex == -1) {
				String sRes = result.substring(beginIndex, result.length());
				unwrittenLine = unwrittenLine != null && unwrittenLine.length() > 0 ? unwrittenLine + sRes : sRes;
				return;
			} else {
				// write result to logger
				String sRes = result.substring(beginIndex, endIndex);
				if (unwrittenLine != null && unwrittenLine.length() > 0) {
					sRes = unwrittenLine + sRes;
					unwrittenLine = null;
				}
		        logger.info(sRes);
			}
			
	        // set begin index
			beginIndex = endIndex+1;
		}while (beginIndex != len);

	}
	
	/**
	 * Skip CRLF and get position.
	 * @param input
	 * @param currentPosition
	 * @return
	 */
	private int getPositionAfterCRLF(String input, int currentPosition) {
		int len = input.length();
		if (len == 0 || currentPosition == len-1) {
			return -1;
		}
		char ch = input.charAt(currentPosition);
		while (ch == Character.LETTER_NUMBER || ch == Character.LINE_SEPARATOR) {
			if (currentPosition == len-1) {
				return -1;
			}
			currentPosition++;
			ch = input.charAt(currentPosition);
		}
		return currentPosition;
	}
	
	/**
	 * Skip characters without CRLF and get position.
	 * @param input
	 * @param currentPosition
	 * @return
	 */
	private int getPositionBeforeCRLF(String input, int currentPosition) {
		int len = input.length();
		if (len == 0 || currentPosition == len-1) {
			return -1;
		}
		char ch = input.charAt(currentPosition);
		while (ch != Character.LETTER_NUMBER && ch != Character.LINE_SEPARATOR) {
			if (currentPosition == len-1) {
				return -1;
			}
			currentPosition++;
			ch = input.charAt(currentPosition);
		}
		return currentPosition;
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#isOpen()
	 */
	@Override
	public boolean isOpen() {
		return true;	// logger is always open
	}
}
