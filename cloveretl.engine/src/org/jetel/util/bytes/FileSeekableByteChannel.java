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
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;

import org.jetel.data.Defaults;
import org.jetel.data.parser.AhoCorasick;

/**
 * Simple wrapper around FileChannel, so now implements our SeekableByteChannel interface.
 * JDK1.7 already has this type of interface and so it could be removed in the future.
 * 
 * @author Martin Zatopek (info@cloveretl.com)
 *         (c) (c) Javlin, a.s. (www.javlin.eu) (www.cloveretl.com)
 *
 * @created 2.12.2009
 */
public class FileSeekableByteChannel implements SeekableByteChannel {

	private FileChannel fileChannel;
	
	public FileSeekableByteChannel(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#position()
	 */
	@Override
	public long position() throws IOException {
		return fileChannel.position();
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#position(long)
	 */
	@Override
	public SeekableByteChannel position(long newPosition) throws IOException {
		fileChannel.position(newPosition);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#size()
	 */
	@Override
	public long size() throws IOException {
		return fileChannel.size();
	}

	/* (non-Javadoc)
	 * @see org.jetel.util.bytes.SeekableByteChannel#truncate(long)
	 */
	@Override
	public SeekableByteChannel truncate(long size) throws IOException {
		fileChannel.truncate(size);
		return this;
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.ReadableByteChannel#read(java.nio.ByteBuffer)
	 */
	@Override
	public int read(ByteBuffer dst) throws IOException {
		return fileChannel.read(dst);
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#close()
	 */
	@Override
	public void close() throws IOException {
		fileChannel.close();
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.Channel#isOpen()
	 */
	@Override
	public boolean isOpen() {
		return fileChannel.isOpen();
	}

	/* (non-Javadoc)
	 * @see java.nio.channels.WritableByteChannel#write(java.nio.ByteBuffer)
	 */
	@Override
	public int write(ByteBuffer src) throws IOException {
		return fileChannel.write(src);
	}
	
	/**
	 * Find next delimiter in a seekable channel 
	 * @param channel Input channel, current position may be changed after return from the method
	 * @param recordDelimiter An array of delimiters
	 * @return position of the first character after first delimiter, returned position
     * is relative to the original position in the channel
	 * @throws IOException
	 */ 
	public static long findNextRecord(SeekableByteChannel channel, Charset channelCharset, String[] delimiters) throws IOException
	{
		boolean endOfInput = false;
		
    	long shift = 0;
		
		ByteBuffer tmpByteBuffer = ByteBuffer.allocateDirect(Defaults.Record.RECORD_INITIAL_SIZE);
		CharBuffer charBuffer = CharBuffer.allocate(1);	// small buffer, so that byte channel position can be determined for each character
		
		CharsetDecoder charsetDecoder = channelCharset.newDecoder();
		charsetDecoder.onMalformedInput(CodingErrorAction.IGNORE); 

		AhoCorasick delimiterSearcher = new AhoCorasick(delimiters);

		tmpByteBuffer.clear();
    	while (!endOfInput) { // one iteration for each byte buffer filling
        	if (shift > Defaults.Record.RECORD_LIMIT_SIZE) {
        		throw new IOException("No record delimiter was found during file partitioning.");
        	}
        	
        	if (channel.read(tmpByteBuffer) == -1) { // no more records
        		endOfInput = true;
        	}
        	
        	tmpByteBuffer.flip();

    		while (true) {	// one iteration for each character
    			charBuffer.clear();
    			charsetDecoder.decode(tmpByteBuffer, charBuffer, endOfInput);
    			charBuffer.flip();
    			if (charBuffer.remaining() == 0) { // need to read more bytes into byte buffer
    				break;
    			}

    			delimiterSearcher.update(charBuffer.get());
    			assert charBuffer.remaining() == 0 : "Buffer seems to contain more characters than expected";
    			
    			for (int i = 0; i < delimiters.length; i++) {
    				if (delimiterSearcher.isPattern(i)) {
    					return shift + tmpByteBuffer.position(); 
    				}
    			}
    		}
    		
			shift += tmpByteBuffer.position();
       		tmpByteBuffer.compact();	// preserve un-decoded bytes (possibly start of next character)
    	}    	
    	assert endOfInput : "Unexpected execution flow";
    	// we have reached end of the input. let's consider it as a special case of delimiter
    	return shift;
	}
	
}
